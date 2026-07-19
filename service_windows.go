//go:build windows

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	windowsServiceName        = applicationName
	windowsServiceDisplayName = "Art-Net Web Controller"
	windowsServiceDescription = "Provides the local Art-Net lighting controller web interface and DMX transmitter."
)

type serviceHandler struct {
	config appConfig
}

type eventLogWriter struct {
	log *eventlog.Log
}

func (w eventLogWriter) Write(data []byte) (int, error) {
	message := strings.TrimSpace(string(data))
	if message == "" {
		return len(data), nil
	}
	if err := w.log.Info(1, message); err != nil {
		return 0, err
	}
	return len(data), nil
}

func isWindowsService() (bool, error) {
	return svc.IsWindowsService()
}

func runWindowsService(config appConfig) error {
	eventLogger, err := eventlog.Open(windowsServiceName)
	if err == nil {
		defer eventLogger.Close()
		originalOutput := log.Writer()
		originalFlags := log.Flags()
		log.SetOutput(eventLogWriter{log: eventLogger})
		log.SetFlags(0)
		defer func() {
			log.SetOutput(originalOutput)
			log.SetFlags(originalFlags)
		}()
	}

	log.Printf("starting %s service", windowsServiceDisplayName)
	if err := svc.Run(windowsServiceName, &serviceHandler{config: config}); err != nil {
		log.Printf("%s service dispatcher failed: %v", windowsServiceDisplayName, err)
		return err
	}
	log.Printf("%s service stopped", windowsServiceDisplayName)
	return nil
}

func (h *serviceHandler) Execute(_ []string, requests <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const accepts = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending, WaitHint: 5000}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	appErrors := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		appErrors <- runApplicationWithReady(ctx, h.config, ready)
	}()

	select {
	case <-ready:
	case err := <-appErrors:
		log.Printf("service failed during startup: %v", err)
		return true, 1
	}
	running := svc.Status{State: svc.Running, Accepts: accepts}
	changes <- running
	for {
		select {
		case err := <-appErrors:
			if err != nil {
				log.Printf("service stopped because the application failed: %v", err)
				return true, 1
			}
			return false, 0
		case request, ok := <-requests:
			if !ok {
				cancel()
				if err := <-appErrors; err != nil {
					log.Printf("application shutdown failed: %v", err)
					return true, 1
				}
				return false, 0
			}
			switch request.Cmd {
			case svc.Interrogate:
				changes <- running
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending, WaitHint: 7000}
				cancel()
				if err := <-appErrors; err != nil {
					log.Printf("application shutdown failed: %v", err)
					return true, 1
				}
				return false, 0
			default:
				log.Printf("ignoring unsupported service control command %d", request.Cmd)
			}
		}
	}
}

func manageWindowsService(action string, config appConfig) error {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "install":
		return installWindowsService(config)
	case "uninstall":
		return uninstallWindowsService()
	case "start":
		return startWindowsService()
	case "stop":
		return stopWindowsService()
	default:
		return fmt.Errorf("unknown Windows service action %q (use install, uninstall, start, or stop)", action)
	}
}

func installWindowsService(config appConfig) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find executable: %w", err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}

	manager, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("connect to Windows Service Control Manager (run as Administrator): %w", err)
	}
	defer manager.Disconnect()

	if existing, openErr := manager.OpenService(windowsServiceName); openErr == nil {
		existing.Close()
		return fmt.Errorf("service %s is already installed", windowsServiceName)
	}

	service, err := manager.CreateService(
		windowsServiceName,
		executable,
		mgr.Config{
			DisplayName:      windowsServiceDisplayName,
			Description:      windowsServiceDescription,
			StartType:        mgr.StartAutomatic,
			ErrorControl:     mgr.ErrorNormal,
			DelayedAutoStart: true,
		},
		serviceArguments(config)...,
	)
	if err != nil {
		return fmt.Errorf("install service %s: %w", windowsServiceName, err)
	}
	defer service.Close()

	if err := eventlog.InstallAsEventCreate(windowsServiceName, eventlog.Error|eventlog.Warning|eventlog.Info); err != nil {
		_ = service.Delete()
		return fmt.Errorf("install Windows Event Log source: %w", err)
	}
	return nil
}

func serviceArguments(config appConfig) []string {
	args := []string{"-http=" + config.httpAddr, "-universe=" + strconv.FormatUint(uint64(config.universe), 10)}
	if strings.TrimSpace(config.target) != "" {
		args = append(args, "-target="+config.target)
	}
	if strings.TrimSpace(config.statePath) != "" {
		args = append(args, "-state="+config.statePath)
	}
	return args
}

func uninstallWindowsService() error {
	manager, service, err := openWindowsService()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()

	status, err := service.Query()
	if err != nil {
		return fmt.Errorf("query service %s: %w", windowsServiceName, err)
	}
	if status.State != svc.Stopped {
		if err := stopOpenWindowsService(service, status); err != nil {
			return err
		}
	}
	if err := service.Delete(); err != nil {
		return fmt.Errorf("uninstall service %s: %w", windowsServiceName, err)
	}
	if err := eventlog.Remove(windowsServiceName); err != nil {
		return fmt.Errorf("remove Windows Event Log source: %w", err)
	}
	return nil
}

func startWindowsService() error {
	manager, service, err := openWindowsService()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()

	status, err := service.Query()
	if err != nil {
		return fmt.Errorf("query service %s: %w", windowsServiceName, err)
	}
	switch status.State {
	case svc.Running:
		return nil
	case svc.StopPending:
		if err := waitForServiceState(service, svc.Stopped, 15*time.Second); err != nil {
			return err
		}
	case svc.StartPending:
		return waitForServiceState(service, svc.Running, 15*time.Second)
	}
	if err := service.Start(); err != nil {
		return fmt.Errorf("start service %s: %w", windowsServiceName, err)
	}
	return waitForServiceState(service, svc.Running, 15*time.Second)
}

func stopWindowsService() error {
	manager, service, err := openWindowsService()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()

	status, err := service.Query()
	if err != nil {
		return fmt.Errorf("query service %s: %w", windowsServiceName, err)
	}
	return stopOpenWindowsService(service, status)
}

func stopOpenWindowsService(service *mgr.Service, status svc.Status) error {
	if status.State == svc.Stopped {
		return nil
	}
	if status.State != svc.StopPending {
		if _, err := service.Control(svc.Stop); err != nil {
			return fmt.Errorf("stop service %s: %w", windowsServiceName, err)
		}
	}
	return waitForServiceState(service, svc.Stopped, 15*time.Second)
}

func openWindowsService() (*mgr.Mgr, *mgr.Service, error) {
	manager, err := mgr.Connect()
	if err != nil {
		return nil, nil, fmt.Errorf("connect to Windows Service Control Manager (run as Administrator): %w", err)
	}
	service, err := manager.OpenService(windowsServiceName)
	if err != nil {
		manager.Disconnect()
		return nil, nil, fmt.Errorf("open service %s: %w", windowsServiceName, err)
	}
	return manager, service, nil
}

func waitForServiceState(service *mgr.Service, target svc.State, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastState svc.State
	for time.Now().Before(deadline) {
		status, err := service.Query()
		if err != nil {
			return fmt.Errorf("query service %s: %w", windowsServiceName, err)
		}
		lastState = status.State
		if status.State == target {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("service %s did not reach state %d within %s (last state %d)", windowsServiceName, target, timeout, lastState)
}
