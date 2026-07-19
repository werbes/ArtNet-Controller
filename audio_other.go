//go:build !windows

package main

import (
	"context"
	"errors"
)

func listHostAudioSources() ([]AudioSource, error) {
	return nil, errors.New("localhost audio capture is only supported on Windows")
}

func monitorHostAudioSource(ctx context.Context, source AudioSource, emit func(float32), ready chan<- error) error {
	err := errors.New("localhost audio capture is only supported on Windows")
	ready <- err
	return err
}
