//go:build windows

package main

import (
	"reflect"
	"testing"
)

func TestServiceArguments(t *testing.T) {
	config := appConfig{httpAddr: "127.0.0.1:9090", target: "192.168.1.50", universe: 12, statePath: `C:\ProgramData\ArtNetController\state.json`}
	want := []string{"-http=127.0.0.1:9090", "-universe=12", "-target=192.168.1.50", `-state=C:\ProgramData\ArtNetController\state.json`}
	if got := serviceArguments(config); !reflect.DeepEqual(got, want) {
		t.Fatalf("serviceArguments() = %q, want %q", got, want)
	}
}

func TestServiceArgumentsOmitEmptyTarget(t *testing.T) {
	config := appConfig{httpAddr: ":8080", universe: 0}
	want := []string{"-http=:8080", "-universe=0"}
	if got := serviceArguments(config); !reflect.DeepEqual(got, want) {
		t.Fatalf("serviceArguments() = %q, want %q", got, want)
	}
}
