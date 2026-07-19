package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

type AudioSource struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type AudioEvent struct {
	Level    float32         `json:"level"`
	Active   bool            `json:"active"`
	Beat     bool            `json:"beat,omitempty"`
	Updates  []ChannelUpdate `json:"updates,omitempty"`
	SourceID string          `json:"sourceId,omitempty"`
	Error    string          `json:"error,omitempty"`
}

type AudioStatusResponse struct {
	Sources    []AudioSource `json:"sources"`
	Active     bool          `json:"active"`
	SourceID   string        `json:"sourceId,omitempty"`
	SourceName string        `json:"sourceName,omitempty"`
	SourceKind string        `json:"sourceKind,omitempty"`
	Level      float32       `json:"level"`
	LastError  string        `json:"lastError,omitempty"`
}

type AudioConfigureRequest struct {
	SourceID string `json:"sourceId"`
	Active   bool   `json:"active"`
}

type AudioManager struct {
	configureMu sync.Mutex
	mu          sync.RWMutex
	active      bool
	source      AudioSource
	level       float32
	lastError   string
	cancel      context.CancelFunc
	generation  uint64
	subscribers map[chan AudioEvent]struct{}
	processor   func(float32) ReactiveResult
	stateStore  *StateStore
}

func NewAudioManager() *AudioManager {
	return &AudioManager{subscribers: make(map[chan AudioEvent]struct{})}
}

func (a *AudioManager) setRuntime(stateStore *StateStore, processor func(float32) ReactiveResult) {
	a.mu.Lock()
	a.stateStore = stateStore
	a.processor = processor
	a.mu.Unlock()
}

func (a *AudioManager) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	status, err := a.statusWithSources()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, status)
}

func (a *AudioManager) handleConfigure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	var req AudioConfigureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !req.Active && a.stateStore != nil {
		if err := a.stateStore.UpdateAudioRuntime(req.SourceID, false); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err := a.stateStore.Save(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	if err := a.Configure(req.SourceID, req.Active); err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	if req.Active && a.stateStore != nil {
		if err := a.stateStore.UpdateAudioRuntime(req.SourceID, true); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err := a.stateStore.Save(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	status, err := a.statusWithSources()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, status)
}

func (a *AudioManager) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")

	events := a.subscribe()
	defer a.unsubscribe(events)
	initial := a.currentEvent()
	if err := writeAudioEvent(w, initial); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-events:
			if err := writeAudioEvent(w, event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeAudioEvent(w http.ResponseWriter, event AudioEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

func (a *AudioManager) statusWithSources() (AudioStatusResponse, error) {
	sources, err := listHostAudioSources()
	if err != nil {
		return AudioStatusResponse{}, fmt.Errorf("list Windows audio sources: %w", err)
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Kind == sources[j].Kind {
			return sources[i].Name < sources[j].Name
		}
		return sources[i].Kind < sources[j].Kind
	})
	a.mu.RLock()
	defer a.mu.RUnlock()
	return AudioStatusResponse{
		Sources:    sources,
		Active:     a.active,
		SourceID:   a.source.ID,
		SourceName: a.source.Name,
		SourceKind: a.source.Kind,
		Level:      a.level,
		LastError:  a.lastError,
	}, nil
}

func (a *AudioManager) Configure(sourceID string, active bool) error {
	a.configureMu.Lock()
	defer a.configureMu.Unlock()
	a.stop(false)
	if !active {
		return nil
	}

	sources, err := listHostAudioSources()
	if err != nil {
		return fmt.Errorf("list Windows audio sources: %w", err)
	}
	var selected AudioSource
	for _, source := range sources {
		if source.ID == sourceID {
			selected = source
			break
		}
	}
	if selected.ID == "" {
		return errors.New("select a valid localhost audio source")
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	a.generation++
	generation := a.generation
	a.active = true
	a.source = selected
	a.level = 0
	a.lastError = ""
	a.cancel = cancel
	a.mu.Unlock()

	ready := make(chan error, 1)
	go func() {
		err := monitorHostAudioSource(ctx, selected, func(level float32) {
			a.updateLevel(generation, level)
		}, ready)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.fail(generation, err)
		}
	}()

	select {
	case err := <-ready:
		if err != nil {
			cancel()
			a.fail(generation, err)
			return fmt.Errorf("start audio source %q: %w", selected.Name, err)
		}
		return nil
	case <-time.After(4 * time.Second):
		cancel()
		err := errors.New("audio source startup timed out")
		a.fail(generation, err)
		return err
	}
}

func (a *AudioManager) Stop() {
	a.stop(true)
}

func (a *AudioManager) stop(publish bool) {
	a.mu.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.generation++
	a.active = false
	a.source = AudioSource{}
	a.level = 0
	a.lastError = ""
	a.cancel = nil
	event := AudioEvent{Active: false}
	if publish {
		a.publishLocked(event)
	}
	a.mu.Unlock()
}

func (a *AudioManager) updateLevel(generation uint64, level float32) {
	if level < 0 {
		level = 0
	}
	if level > 1 {
		level = 1
	}
	a.mu.Lock()
	if generation != a.generation || !a.active {
		a.mu.Unlock()
		return
	}
	a.level = level
	processor := a.processor
	sourceID := a.source.ID
	a.mu.Unlock()

	result := ReactiveResult{}
	if processor != nil {
		result = processor(level)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if generation != a.generation || !a.active {
		return
	}
	a.publishLocked(AudioEvent{
		Level:    level,
		Active:   true,
		Beat:     result.Beat,
		Updates:  result.Updates,
		SourceID: sourceID,
	})
}

func (a *AudioManager) fail(generation uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if generation != a.generation {
		return
	}
	a.active = false
	a.level = 0
	a.lastError = err.Error()
	a.cancel = nil
	a.publishLocked(AudioEvent{Active: false, SourceID: a.source.ID, Error: err.Error()})
}

func (a *AudioManager) currentEvent() AudioEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return AudioEvent{Level: a.level, Active: a.active, SourceID: a.source.ID, Error: a.lastError}
}

func (a *AudioManager) subscribe() chan AudioEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	ch := make(chan AudioEvent, 4)
	a.subscribers[ch] = struct{}{}
	return ch
}

func (a *AudioManager) unsubscribe(ch chan AudioEvent) {
	a.mu.Lock()
	delete(a.subscribers, ch)
	a.mu.Unlock()
}

func (a *AudioManager) publishLocked(event AudioEvent) {
	for ch := range a.subscribers {
		select {
		case ch <- event:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- event:
			default:
			}
		}
	}
}

func (a *AudioManager) actualState() (bool, string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.active, a.source.ID
}

func superviseAudio(ctx context.Context, audioManager *AudioManager, stateStore *StateStore) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		desired := stateStore.Snapshot().Audio
		active, sourceID := audioManager.actualState()
		if desired.Active && desired.SourceID != "" && (!active || sourceID != desired.SourceID) {
			if err := audioManager.Configure(desired.SourceID, true); err != nil {
				log.Printf("restore audio source: %v", err)
			} else {
				log.Printf("restored audio source %q", desired.SourceID)
			}
		} else if !desired.Active && active {
			_ = audioManager.Configure(sourceID, false)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
