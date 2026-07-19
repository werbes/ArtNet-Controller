//go:build !windows

package main

import "fmt"

func isWindowsService() (bool, error) {
	return false, nil
}

func runWindowsService(appConfig) error {
	return fmt.Errorf("Windows services are only supported on Windows")
}

func manageWindowsService(string, appConfig) error {
	return fmt.Errorf("Windows services are only supported on Windows")
}
