package main

import (
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestStateStoreRoundTrip(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := newStateStore(statePath, appConfig{target: "192.168.1.10", universe: 2})
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []Fixture{{ID: "bar-1", Type: "rgb", Start: 10, Name: "Front bar"}}
	fixtureID := "bar-1"
	mode := "pulse"
	sensitivity := 0
	intensity := 180
	tempo := 400
	movement := "off"
	sourceID := "device-1"
	if err := store.ApplyPatch(RuntimeStatePatch{
		Fixtures: &fixtures,
		Audio: &AudioOptionsPatch{
			SourceID:    &sourceID,
			FixtureID:   &fixtureID,
			Mode:        &mode,
			Sensitivity: &sensitivity,
			Intensity:   &intensity,
			TempoMS:     &tempo,
			Movement:    &movement,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAudioRuntime(sourceID, true); err != nil {
		t.Fatal(err)
	}
	controller := store.Snapshot().Controller
	controller.TargetIP = "10.0.0.42"
	controller.Universe = 17
	controller.Streaming = true
	controller.FPS = 22
	controller.DMX[0] = 91
	controller.DMX[511] = 203
	if err := store.UpdateController(controller); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(); err != nil {
		t.Fatal(err)
	}

	restoredStore, err := newStateStore(statePath, appConfig{target: "127.0.0.1", universe: 99})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := restoredStore.Snapshot(), store.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored state differs\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestStateStoreRejectsInvalidFixture(t *testing.T) {
	store, err := newStateStore("", appConfig{})
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []Fixture{{ID: "bad", Type: "mh16", Start: 500, Name: "Out of range"}}
	if err := store.ApplyPatch(RuntimeStatePatch{Fixtures: &fixtures}); err == nil {
		t.Fatal("ApplyPatch accepted a fixture outside the DMX range")
	}
	if len(store.Snapshot().Fixtures) != 0 {
		t.Fatal("invalid patch changed the stored fixtures")
	}
}

func TestReactiveEngineUpdatesControllerAndPersistentDMX(t *testing.T) {
	store, err := newStateStore("", appConfig{})
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []Fixture{{ID: "rgb-1", Type: "rgb", Start: 20, Name: "RGB"}}
	fixtureID := "rgb-1"
	mode := "color"
	sensitivity := 100
	intensity := 255
	if err := store.ApplyPatch(RuntimeStatePatch{
		Fixtures: &fixtures,
		Audio: &AudioOptionsPatch{
			FixtureID:   &fixtureID,
			Mode:        &mode,
			Sensitivity: &sensitivity,
			Intensity:   &intensity,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAudioRuntime("device-1", true); err != nil {
		t.Fatal(err)
	}
	controller := NewController("", 0)
	defer controller.Close()
	controller.stateStore = store
	engine := NewReactiveEngine(controller, store)
	engine.random = rand.New(rand.NewSource(1))

	result := engine.Process(1)
	if !result.Beat {
		t.Fatal("full-scale audio did not produce a beat")
	}
	if len(result.Updates) == 0 {
		t.Fatal("reactive engine produced no channel updates")
	}
	restored := store.Snapshot().Controller.DMX
	if restored[19] == 0 && restored[20] == 0 && restored[21] == 0 {
		t.Fatal("reactive RGB values were not stored")
	}
}

func TestReactiveRandomControlsLightBarSegmentsIndependently(t *testing.T) {
	engine := &ReactiveEngine{
		random:    rand.New(rand.NewSource(1)),
		positions: make(map[string]reactivePosition),
	}
	state := PersistentState{
		Fixtures: []Fixture{
			{ID: "bar-1", Type: "fun-generation-barbara-24-24ch", Start: 1},
			{ID: "bar-2", Type: "fun-generation-barbara-24-24ch", Start: 25},
		},
		Audio: PersistentAudio{
			FixtureID: "all",
			Mode:      "random",
			Intensity: 255,
			Movement:  "off",
		},
	}
	updates := make(map[int]int)

	engine.applyScene(state, "random", 1, true, false, updates)

	for _, fixture := range state.Fixtures {
		var previous [3]int
		for segment := range 8 {
			start := fixture.Start + segment*3
			color := [3]int{updates[start], updates[start+1], updates[start+2]}
			if color == [3]int{} {
				t.Fatalf("fixture %q segment %d was not lit", fixture.ID, segment+1)
			}
			if segment > 0 && color == previous {
				t.Fatalf("fixture %q segments %d and %d received the same color %v", fixture.ID, segment, segment+1, color)
			}
			previous = color
		}
	}

	for offset := range 24 {
		if updates[1+offset] != updates[25+offset] {
			return
		}
	}
	t.Fatal("both light bars received the same random pattern")
}

func TestReactiveRandomContinuesAtConfiguredTempo(t *testing.T) {
	store, err := newStateStore("", appConfig{})
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []Fixture{
		{ID: "bar-1", Type: "fun-generation-barbara-24-24ch", Start: 1},
		{ID: "head-1", Type: "mh16", Start: 25},
	}
	fixtureID := "all"
	mode := "random"
	intensity := 255
	tempo := 100
	movement := "wide"
	if err := store.ApplyPatch(RuntimeStatePatch{
		Fixtures: &fixtures,
		Audio: &AudioOptionsPatch{
			FixtureID: &fixtureID,
			Mode:      &mode,
			Intensity: &intensity,
			TempoMS:   &tempo,
			Movement:  &movement,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateAudioRuntime("device-1", true); err != nil {
		t.Fatal(err)
	}

	controller := NewController("", 0)
	defer controller.Close()
	controller.stateStore = store
	engine := NewReactiveEngine(controller, store)
	engine.random = rand.New(rand.NewSource(2))

	engine.lastBeat = time.Now().Add(-time.Second)
	first := engine.Process(0)
	if !first.Beat || len(first.Updates) == 0 {
		t.Fatal("random mode did not produce its first timed scene")
	}
	firstDMX := store.Snapshot().Controller.DMX

	engine.lastBeat = time.Now().Add(-time.Second)
	second := engine.Process(0)
	if !second.Beat || len(second.Updates) == 0 {
		t.Fatal("random mode did not produce another timed scene")
	}
	secondDMX := store.Snapshot().Controller.DMX
	if reflect.DeepEqual(firstDMX[:24], secondDMX[:24]) {
		t.Fatal("lightbar pattern did not change between timed scenes")
	}
	if reflect.DeepEqual(firstDMX[24:40], secondDMX[24:40]) {
		t.Fatal("moving-head state did not change between timed scenes")
	}

	immediate := engine.Process(0)
	if immediate.Beat || len(immediate.Updates) != 0 {
		t.Fatal("random mode ignored the configured scene cooldown")
	}
}
