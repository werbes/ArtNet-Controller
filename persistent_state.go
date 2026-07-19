package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const persistentStateVersion = 1

type Fixture struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Start int    `json:"start"`
	Name  string `json:"name"`
}

type PersistentController struct {
	TargetIP  string `json:"targetIp"`
	Universe  uint16 `json:"universe"`
	Streaming bool   `json:"streaming"`
	FPS       int    `json:"fps"`
	DMX       []int  `json:"dmx"`
}

type PersistentAudio struct {
	SourceID    string `json:"sourceId"`
	Active      bool   `json:"active"`
	FixtureID   string `json:"fixtureId"`
	Mode        string `json:"mode"`
	Sensitivity int    `json:"sensitivity"`
	Intensity   int    `json:"intensity"`
	TempoMS     int    `json:"tempoMs"`
	Movement    string `json:"movement"`
}

type PersistentState struct {
	Version    int                  `json:"version"`
	Controller PersistentController `json:"controller"`
	Fixtures   []Fixture            `json:"fixtures"`
	Audio      PersistentAudio      `json:"audio"`
}

type AudioOptionsPatch struct {
	SourceID    *string `json:"sourceId"`
	FixtureID   *string `json:"fixtureId"`
	Mode        *string `json:"mode"`
	Sensitivity *int    `json:"sensitivity"`
	Intensity   *int    `json:"intensity"`
	TempoMS     *int    `json:"tempoMs"`
	Movement    *string `json:"movement"`
}

type RuntimeStatePatch struct {
	Fixtures *[]Fixture         `json:"fixtures"`
	Audio    *AudioOptionsPatch `json:"audio"`
}

type StateStore struct {
	mu         sync.RWMutex
	saveMu     sync.Mutex
	path       string
	state      PersistentState
	dirty      bool
	generation uint64
}

func defaultStatePath() string {
	if runtime.GOOS == "windows" {
		if programData := strings.TrimSpace(os.Getenv("ProgramData")); programData != "" {
			return filepath.Join(programData, applicationName, "state.json")
		}
	}
	configDir, err := os.UserConfigDir()
	if err != nil || configDir == "" {
		return "state.json"
	}
	return filepath.Join(configDir, applicationName, "state.json")
}

func newStateStore(path string, config appConfig) (*StateStore, error) {
	state := defaultPersistentState(config)
	store := &StateStore{path: strings.TrimSpace(path), state: state}
	if store.path == "" {
		return store, nil
	}

	data, err := os.ReadFile(store.path)
	if errors.Is(err, os.ErrNotExist) {
		store.dirty = true
		store.generation = 1
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read runtime state %s: %w", store.path, err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode runtime state %s: %w", store.path, err)
	}
	if err := normalizePersistentState(&state); err != nil {
		return nil, fmt.Errorf("validate runtime state %s: %w", store.path, err)
	}
	store.state = state
	return store, nil
}

func defaultPersistentState(config appConfig) PersistentState {
	dmx := make([]int, maxDMXChannels)
	return PersistentState{
		Version: persistentStateVersion,
		Controller: PersistentController{
			TargetIP:  strings.TrimSpace(config.target),
			Universe:  config.universe,
			Streaming: strings.TrimSpace(config.target) != "",
			FPS:       defaultFPS,
			DMX:       dmx,
		},
		Fixtures: []Fixture{},
		Audio: PersistentAudio{
			FixtureID:   "all",
			Mode:        "random",
			Sensitivity: 65,
			Intensity:   220,
			TempoMS:     240,
			Movement:    "wide",
		},
	}
}

func normalizePersistentState(state *PersistentState) error {
	if state.Version == 0 {
		state.Version = persistentStateVersion
	}
	if state.Version != persistentStateVersion {
		return fmt.Errorf("unsupported state version %d", state.Version)
	}
	state.Controller.TargetIP = strings.TrimSpace(state.Controller.TargetIP)
	if state.Controller.TargetIP != "" && net.ParseIP(state.Controller.TargetIP).To4() == nil {
		return errors.New("controller targetIp must be an IPv4 address")
	}
	if state.Controller.Universe > 32767 {
		return errors.New("controller universe must be between 0 and 32767")
	}
	if state.Controller.FPS == 0 {
		state.Controller.FPS = defaultFPS
	}
	if state.Controller.FPS < 1 || state.Controller.FPS > 44 {
		return errors.New("controller fps must be between 1 and 44")
	}
	if state.Controller.DMX == nil {
		state.Controller.DMX = make([]int, maxDMXChannels)
	}
	if len(state.Controller.DMX) != maxDMXChannels {
		return fmt.Errorf("controller dmx must contain exactly %d channels", maxDMXChannels)
	}
	for index, value := range state.Controller.DMX {
		if value < 0 || value > 255 {
			return fmt.Errorf("controller dmx channel %d must be between 0 and 255", index+1)
		}
	}
	if state.Fixtures == nil {
		state.Fixtures = []Fixture{}
	}
	if err := validateFixtures(state.Fixtures); err != nil {
		return err
	}
	return normalizePersistentAudio(&state.Audio, state.Fixtures)
}

func normalizePersistentAudio(audio *PersistentAudio, fixtures []Fixture) error {
	audio.SourceID = strings.TrimSpace(audio.SourceID)
	audio.FixtureID = strings.TrimSpace(audio.FixtureID)
	if audio.FixtureID == "" {
		audio.FixtureID = "all"
	}
	if audio.FixtureID != "all" && !containsFixture(fixtures, audio.FixtureID) {
		audio.FixtureID = "all"
	}
	audio.Mode = strings.TrimSpace(audio.Mode)
	if audio.Mode == "" {
		audio.Mode = "random"
	}
	if !oneOf(audio.Mode, "random", "color", "segments", "chase", "pulse") {
		return fmt.Errorf("unsupported audio mode %q", audio.Mode)
	}
	if audio.Sensitivity < 0 || audio.Sensitivity > 100 {
		return errors.New("audio sensitivity must be between 0 and 100")
	}
	if audio.Intensity == 0 {
		audio.Intensity = 220
	}
	if audio.Intensity < 10 || audio.Intensity > 255 {
		return errors.New("audio intensity must be between 10 and 255")
	}
	if audio.TempoMS == 0 {
		audio.TempoMS = 240
	}
	if audio.TempoMS < 100 || audio.TempoMS > 1000 {
		return errors.New("audio tempoMs must be between 100 and 1000")
	}
	audio.Movement = strings.TrimSpace(audio.Movement)
	if audio.Movement == "" {
		audio.Movement = "wide"
	}
	if !oneOf(audio.Movement, "wide", "compact", "off") {
		return fmt.Errorf("unsupported audio movement %q", audio.Movement)
	}
	if audio.Active && audio.SourceID == "" {
		audio.Active = false
	}
	return nil
}

func validateFixtures(fixtures []Fixture) error {
	seen := make(map[string]bool, len(fixtures))
	for index := range fixtures {
		fixtures[index].ID = strings.TrimSpace(fixtures[index].ID)
		fixtures[index].Type = strings.TrimSpace(fixtures[index].Type)
		fixtures[index].Name = strings.TrimSpace(fixtures[index].Name)
		fixture := fixtures[index]
		if fixture.ID == "" {
			return fmt.Errorf("fixture %d has no id", index+1)
		}
		if seen[fixture.ID] {
			return fmt.Errorf("fixture id %q is duplicated", fixture.ID)
		}
		seen[fixture.ID] = true
		channels := fixtureChannelCount(fixture.Type)
		if channels == 0 {
			return fmt.Errorf("fixture %q has unsupported type %q", fixture.ID, fixture.Type)
		}
		if fixture.Start < 1 || fixture.Start+channels-1 > maxDMXChannels {
			return fmt.Errorf("fixture %q exceeds the DMX address range", fixture.ID)
		}
	}
	return nil
}

func containsFixture(fixtures []Fixture, id string) bool {
	for _, fixture := range fixtures {
		if fixture.ID == id {
			return true
		}
	}
	return false
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func (s *StateStore) Path() string {
	return s.path
}

func (s *StateStore) Snapshot() PersistentState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return clonePersistentState(s.state)
}

func clonePersistentState(state PersistentState) PersistentState {
	state.Controller.DMX = append([]int(nil), state.Controller.DMX...)
	state.Fixtures = append([]Fixture(nil), state.Fixtures...)
	return state
}

func (s *StateStore) update(update func(*PersistentState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := clonePersistentState(s.state)
	if err := update(&next); err != nil {
		return err
	}
	if err := normalizePersistentState(&next); err != nil {
		return err
	}
	s.state = next
	s.dirty = true
	s.generation++
	return nil
}

func (s *StateStore) UpdateController(controller PersistentController) error {
	return s.update(func(state *PersistentState) error {
		state.Controller = controller
		return nil
	})
}

func (s *StateStore) UpdateAudioRuntime(sourceID string, active bool) error {
	return s.update(func(state *PersistentState) error {
		if strings.TrimSpace(sourceID) != "" {
			state.Audio.SourceID = strings.TrimSpace(sourceID)
		}
		state.Audio.Active = active
		return nil
	})
}

func (s *StateStore) ApplyPatch(patch RuntimeStatePatch) error {
	return s.update(func(state *PersistentState) error {
		if patch.Fixtures != nil {
			state.Fixtures = append([]Fixture(nil), (*patch.Fixtures)...)
		}
		if patch.Audio != nil {
			options := patch.Audio
			if options.SourceID != nil {
				state.Audio.SourceID = strings.TrimSpace(*options.SourceID)
			}
			if options.FixtureID != nil {
				state.Audio.FixtureID = strings.TrimSpace(*options.FixtureID)
			}
			if options.Mode != nil {
				state.Audio.Mode = strings.TrimSpace(*options.Mode)
			}
			if options.Sensitivity != nil {
				state.Audio.Sensitivity = *options.Sensitivity
			}
			if options.Intensity != nil {
				state.Audio.Intensity = *options.Intensity
			}
			if options.TempoMS != nil {
				state.Audio.TempoMS = *options.TempoMS
			}
			if options.Movement != nil {
				state.Audio.Movement = strings.TrimSpace(*options.Movement)
			}
		}
		return nil
	})
}

func (s *StateStore) Save() error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()

	s.mu.RLock()
	if !s.dirty || s.path == "" {
		s.mu.RUnlock()
		return nil
	}
	state := clonePersistentState(s.state)
	generation := s.generation
	s.mu.RUnlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode runtime state: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create runtime state directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create runtime state temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o640); err != nil {
		return fmt.Errorf("set runtime state permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write runtime state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("flush runtime state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close runtime state: %w", err)
	}
	if err := replaceFile(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace runtime state: %w", err)
	}
	removeTemporary = false

	s.mu.Lock()
	if s.generation == generation {
		s.dirty = false
	}
	s.mu.Unlock()
	return nil
}

func (s *StateStore) Run(ctxDone <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			if err := s.Save(); err != nil {
				log.Printf("save runtime state: %v", err)
			}
		}
	}
}

func (s *StateStore) handleRuntimeState(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.Snapshot())
	case http.MethodPost:
		var patch RuntimeStatePatch
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		if err := decoder.Decode(&patch); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := s.ApplyPatch(patch); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := s.Save(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, s.Snapshot())
	default:
		writeMethodNotAllowed(w)
	}
}
