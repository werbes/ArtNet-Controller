# Windows service

Build the executable, then run the service commands from an elevated PowerShell window.

```powershell
go build -o artnet-controller.exe .
.\artnet-controller.exe -service install -http :8080
.\artnet-controller.exe -service start
```

The `install` command registers `ArtNetController` as an automatic delayed-start service. The HTTP address and state-file path are persisted in the service configuration:

```powershell
.\artnet-controller.exe -service install -http :8080 -target 192.168.1.50 -universe 0
```

Runtime state is stored by default in `%ProgramData%\ArtNetController\state.json`. It includes:

- Art-Net target IP, universe, streaming status, and refresh rate
- All 512 current DMX values
- Patched light fixtures and their addresses
- Selected audio device and whether audio control is active
- Audio fixture selection, mode, sensitivity, intensity, tempo, and movement

The state is restored before output starts. Active audio capture is retried after startup if the Windows audio device is temporarily unavailable. Closing the browser does not stop audio control because reactive DMX processing runs inside the service.

Use an explicit state location when needed:

```powershell
.\artnet-controller.exe -service install -state D:\ArtNet\state.json
```

Open `http://localhost:8080` after the service starts. Runtime messages are available in **Event Viewer > Windows Logs > Application** with source `ArtNetController`.

To stop or remove the service:

```powershell
.\artnet-controller.exe -service stop
.\artnet-controller.exe -service uninstall
```

Installation, removal, starting, and stopping normally require an elevated shell. Windows services run in session 0, so Windows audio devices exposed only to an interactive desktop session may not be available to the audio-reactive API. Art-Net, DMX transmission, discovery, and the HTTP interface are not session-dependent.
