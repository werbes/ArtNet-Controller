package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestApplicationRestoresRuntimeStateAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	firstAddress := availableTCPAddress(t)
	cancel, result := startTestApplication(t, appConfig{httpAddr: firstAddress, statePath: statePath})

	postJSON(t, "http://"+firstAddress+"/api/configure", ConfigureRequest{
		TargetIP:  pointer("127.0.0.2"),
		Universe:  pointer(uint16(31)),
		Streaming: pointer(true),
		FPS:       pointer(22),
	}, nil)
	values := make([]int, maxDMXChannels)
	values[0] = 73
	values[511] = 201
	postJSON(t, "http://"+firstAddress+"/api/dmx", DMXRequest{Values: values}, nil)
	fixtures := []Fixture{{ID: "fixture-1", Type: "rgbw", Start: 20, Name: "Saved fixture"}}
	fixtureID := "fixture-1"
	mode := "pulse"
	sensitivity := 42
	intensity := 190
	tempo := 400
	movement := "compact"
	sourceID := "saved-audio-device"
	postJSON(t, "http://"+firstAddress+"/api/runtime", RuntimeStatePatch{
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
	}, nil)
	stopTestApplication(t, cancel, result)

	secondAddress := availableTCPAddress(t)
	cancel, result = startTestApplication(t, appConfig{
		httpAddr:  secondAddress,
		target:    "192.168.99.99",
		universe:  2,
		statePath: statePath,
	})
	defer stopTestApplication(t, cancel, result)

	var status StatusResponse
	getJSON(t, "http://"+secondAddress+"/api/status", &status)
	if status.TargetIP != "127.0.0.2" || status.Universe != 31 || !status.Streaming || status.FPS != 22 {
		t.Fatalf("restored controller status = %+v", status)
	}
	var runtimeState PersistentState
	getJSON(t, "http://"+secondAddress+"/api/runtime", &runtimeState)
	if len(runtimeState.Fixtures) != 1 || runtimeState.Fixtures[0] != fixtures[0] {
		t.Fatalf("restored fixtures = %+v, want %+v", runtimeState.Fixtures, fixtures)
	}
	if runtimeState.Controller.DMX[0] != 73 || runtimeState.Controller.DMX[511] != 201 {
		t.Fatalf("restored DMX endpoints = %d/%d, want 73/201", runtimeState.Controller.DMX[0], runtimeState.Controller.DMX[511])
	}
	if runtimeState.Audio.SourceID != sourceID || runtimeState.Audio.FixtureID != fixtureID || runtimeState.Audio.Mode != mode {
		t.Fatalf("restored audio state = %+v", runtimeState.Audio)
	}
}

func startTestApplication(t *testing.T, config appConfig) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runApplicationWithReady(ctx, config, ready)
	}()
	select {
	case <-ready:
		return cancel, result
	case err := <-result:
		cancel()
		t.Fatalf("application failed during startup: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("application startup timed out")
	}
	return nil, nil
}

func stopTestApplication(t *testing.T, cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("application shutdown failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("application shutdown timed out")
	}
}

func availableTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func postJSON(t *testing.T, url string, request, response any) {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpResponse, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		t.Fatalf("POST %s returned %s", url, httpResponse.Status)
	}
	if response != nil {
		if err := json.NewDecoder(httpResponse.Body).Decode(response); err != nil {
			t.Fatal(err)
		}
	}
}

func getJSON(t *testing.T, url string, response any) {
	t.Helper()
	httpResponse, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %s", url, httpResponse.Status)
	}
	if err := json.NewDecoder(httpResponse.Body).Decode(response); err != nil {
		t.Fatal(fmt.Errorf("decode %s: %w", url, err))
	}
}

func pointer[T any](value T) *T {
	return &value
}
