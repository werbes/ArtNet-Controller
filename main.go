package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	applicationName = "ArtNetController"
	artNetPort      = 0x1936
	protocolHi      = 0
	protocolLo      = 14
	opPoll          = 0x2000
	opPollReply     = 0x2100
	opDmx           = 0x5000
	defaultHTTP     = ":8080"
	defaultFPS      = 30
	maxDMXChannels  = 512
)

var artNetID = []byte{'A', 'r', 't', '-', 'N', 'e', 't', 0x00}

type Controller struct {
	mu          sync.RWMutex
	discoverMu  sync.Mutex
	stateStore  *StateStore
	conn        *net.UDPConn
	socketNote  string
	nodes       []ArtNetNode
	targetIP    string
	universe    uint16
	streaming   bool
	fps         int
	dmx         [maxDMXChannels]byte
	sequence    byte
	lastSent    time.Time
	lastSendErr string
}

type ArtNetNode struct {
	IP          string       `json:"ip"`
	RemoteIP    string       `json:"remoteIp"`
	ShortName   string       `json:"shortName"`
	LongName    string       `json:"longName"`
	NodeReport  string       `json:"nodeReport"`
	NumPorts    int          `json:"numPorts"`
	Style       string       `json:"style"`
	Firmware    string       `json:"firmware"`
	MAC         string       `json:"mac,omitempty"`
	BindIndex   int          `json:"bindIndex"`
	WebConfig   bool         `json:"webConfig"`
	RDMCapable  bool         `json:"rdmCapable"`
	OutputPorts []OutputPort `json:"outputPorts"`
}

type OutputPort struct {
	Index    int    `json:"index"`
	Universe uint16 `json:"universe"`
	Status   string `json:"status"`
}

type StatusResponse struct {
	TargetIP       string       `json:"targetIp"`
	SendIP         string       `json:"sendIp,omitempty"`
	Universe       uint16       `json:"universe"`
	Streaming      bool         `json:"streaming"`
	Connected      bool         `json:"connected"`
	FPS            int          `json:"fps"`
	SocketNote     string       `json:"socketNote,omitempty"`
	ConnectionNote string       `json:"connectionNote,omitempty"`
	LastSent       string       `json:"lastSent,omitempty"`
	LastSendErr    string       `json:"lastSendErr,omitempty"`
	Nodes          []ArtNetNode `json:"nodes"`
}

type DiscoverRequest struct {
	TimeoutMS int `json:"timeoutMs"`
}

type ConfigureRequest struct {
	TargetIP  *string `json:"targetIp"`
	Universe  *uint16 `json:"universe"`
	Streaming *bool   `json:"streaming"`
	FPS       *int    `json:"fps"`
}

type DMXRequest struct {
	Values  []int           `json:"values"`
	Updates []ChannelUpdate `json:"updates"`
}

type ChannelUpdate struct {
	Channel int `json:"channel"`
	Value   int `json:"value"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type appConfig struct {
	httpAddr  string
	target    string
	universe  uint16
	statePath string
}

func main() {
	httpAddr := flag.String("http", defaultHTTP, "HTTP listen address")
	target := flag.String("target", "", "optional Art-Net node IP")
	universe := flag.Uint("universe", 0, "initial Art-Net universe / Port-Address")
	statePath := flag.String("state", defaultStatePath(), "runtime state file path")
	serviceAction := flag.String("service", "", "Windows service action: install, uninstall, start, or stop")
	flag.Parse()

	config := appConfig{httpAddr: *httpAddr, target: *target, universe: uint16(*universe), statePath: *statePath}
	if strings.TrimSpace(*serviceAction) != "" {
		if err := manageWindowsService(*serviceAction, config); err != nil {
			log.Fatal(err)
		}
		log.Printf("Windows service %s completed", strings.ToLower(strings.TrimSpace(*serviceAction)))
		return
	}

	isService, err := isWindowsService()
	if err != nil {
		log.Fatalf("detect Windows service session: %v", err)
	}
	if isService {
		if err := runWindowsService(config); err != nil {
			log.Fatalf("run Windows service: %v", err)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := runApplication(ctx, config); err != nil {
		log.Fatal(err)
	}
}

func runApplication(parent context.Context, config appConfig) error {
	return runApplicationWithReady(parent, config, nil)
}

func runApplicationWithReady(parent context.Context, config appConfig, ready chan<- struct{}) error {
	stateStore, err := newStateStore(config.statePath, config)
	if err != nil {
		return err
	}
	restored := stateStore.Snapshot()
	controller := NewController(restored.Controller.TargetIP, restored.Controller.Universe)
	controller.restore(restored.Controller)
	controller.stateStore = stateStore
	audioManager := NewAudioManager()
	reactiveEngine := NewReactiveEngine(controller, stateStore)
	audioManager.setRuntime(stateStore, reactiveEngine.Process)
	if err := stateStore.Save(); err != nil {
		controller.Close()
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	transmitDone := make(chan struct{})
	stateStoreDone := make(chan struct{})
	audioSupervisorDone := make(chan struct{})
	go func() {
		defer close(transmitDone)
		controller.TransmitLoop(ctx)
	}()
	go func() {
		defer close(stateStoreDone)
		stateStore.Run(ctx.Done())
	}()
	go func() {
		defer close(audioSupervisorDone)
		superviseAudio(ctx, audioManager, stateStore)
	}()
	defer func() {
		cancel()
		<-transmitDone
		<-stateStoreDone
		<-audioSupervisorDone
		audioManager.Stop()
		controller.Close()
		if err := stateStore.Save(); err != nil {
			log.Printf("flush runtime state: %v", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.HandleFunc("/api/status", controller.handleStatus)
	mux.HandleFunc("/api/discover", controller.handleDiscover)
	mux.HandleFunc("/api/configure", controller.handleConfigure)
	mux.HandleFunc("/api/dmx", controller.handleDMX)
	mux.HandleFunc("/api/blackout", controller.handleBlackout)
	mux.HandleFunc("/api/audio", audioManager.handleStatus)
	mux.HandleFunc("/api/audio/configure", audioManager.handleConfigure)
	mux.HandleFunc("/api/audio/events", audioManager.handleEvents)
	mux.HandleFunc("/api/audio/scene", reactiveEngine.handleScene)
	mux.HandleFunc("/api/runtime", stateStore.handleRuntimeState)

	server := &http.Server{
		Addr:              config.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	listener, err := net.Listen("tcp", config.httpAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.httpAddr, err)
	}
	serverErrors := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverErrors <- err
	}()

	log.Printf("Art-Net web controller listening on %s", displayHTTPURL(config.httpAddr))
	if controller.socketNote != "" {
		log.Printf("Art-Net socket: %s", controller.socketNote)
	}
	if stateStore.Path() != "" {
		log.Printf("Runtime state: %s", stateStore.Path())
	}
	if ready != nil {
		close(ready)
	}

	select {
	case err := <-serverErrors:
		return err
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		return <-serverErrors
	}
}

func NewController(targetIP string, universe uint16) *Controller {
	conn, note := openArtNetSocket()
	return &Controller{
		conn:       conn,
		socketNote: note,
		targetIP:   strings.TrimSpace(targetIP),
		universe:   universe,
		fps:        defaultFPS,
		streaming:  targetIP != "",
	}
}

func (c *Controller) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Controller) restore(state PersistentController) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targetIP = state.TargetIP
	c.universe = state.Universe
	c.streaming = state.Streaming
	c.fps = state.FPS
	for index, value := range state.DMX {
		c.dmx[index] = byte(value)
	}
}

func (c *Controller) persistentState() PersistentController {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.persistentStateLocked()
}

func (c *Controller) persistentStateLocked() PersistentController {
	dmx := make([]int, maxDMXChannels)
	for index, value := range c.dmx {
		dmx[index] = int(value)
	}
	return PersistentController{
		TargetIP:  c.targetIP,
		Universe:  c.universe,
		Streaming: c.streaming,
		FPS:       c.fps,
		DMX:       dmx,
	}
}

func (c *Controller) persist(immediate bool) error {
	if c.stateStore == nil {
		return nil
	}
	c.mu.RLock()
	err := c.stateStore.UpdateController(c.persistentStateLocked())
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	if immediate {
		return c.stateStore.Save()
	}
	return nil
}

func openArtNetSocket() (*net.UDPConn, string) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: artNetPort})
	if err == nil {
		return conn, "bound to UDP 6454; discovery and sending are available"
	}

	fallback, fallbackErr := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if fallbackErr != nil {
		return nil, fmt.Sprintf("could not open UDP socket on 6454 (%v) or fallback socket (%v)", err, fallbackErr)
	}
	return fallback, fmt.Sprintf("UDP 6454 is unavailable (%v); sending can still work, but discovery may miss nodes", err)
}

func displayHTTPURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	if strings.Contains(addr, ":") {
		return "http://" + addr
	}
	return "http://localhost:" + addr
}

func (c *Controller) TransmitLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second / defaultFPS)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			fps := c.fps
			c.mu.RUnlock()
			if fps < 1 {
				fps = defaultFPS
			}
			ticker.Reset(time.Second / time.Duration(fps))
			c.sendCurrentFrame(false)
		}
	}
}

func (c *Controller) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, c.status())
}

func (c *Controller) handleDiscover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}

	var req DiscoverRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout < 300*time.Millisecond || timeout > 5*time.Second {
		timeout = 1600 * time.Millisecond
	}

	nodes, err := c.Discover(timeout)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	c.mu.Lock()
	c.nodes = nodes
	if c.targetIP == "" && len(nodes) > 0 {
		c.targetIP = nodes[0].IP
		if len(nodes[0].OutputPorts) > 0 {
			c.universe = nodes[0].OutputPorts[0].Universe
		}
	}
	c.mu.Unlock()
	if err := c.persist(true); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, c.status())
}

func (c *Controller) handleConfigure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}

	var req ConfigureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	c.mu.Lock()
	if req.TargetIP != nil {
		target := strings.TrimSpace(*req.TargetIP)
		if target != "" && net.ParseIP(target).To4() == nil {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("target IP must be an IPv4 address"))
			return
		}
		c.targetIP = target
	}
	if req.Universe != nil {
		if *req.Universe > 32767 {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("universe must be between 0 and 32767"))
			return
		}
		c.universe = *req.Universe
	}
	if req.Streaming != nil {
		c.streaming = *req.Streaming
	}
	if req.FPS != nil {
		if *req.FPS < 1 || *req.FPS > 44 {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("fps must be between 1 and 44"))
			return
		}
		c.fps = *req.FPS
	}
	c.mu.Unlock()
	if err := c.persist(true); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	c.sendCurrentFrame(true)
	writeJSON(w, c.status())
}

func (c *Controller) handleDMX(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}

	var req DMXRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	c.mu.Lock()
	if len(req.Values) > 0 {
		if len(req.Values) != maxDMXChannels {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("values must contain exactly 512 channels"))
			return
		}
		for i, value := range req.Values {
			if value < 0 || value > 255 {
				c.mu.Unlock()
				writeError(w, http.StatusBadRequest, fmt.Errorf("channel %d must be between 0 and 255", i+1))
				return
			}
			c.dmx[i] = byte(value)
		}
	}
	for _, update := range req.Updates {
		if update.Channel < 1 || update.Channel > maxDMXChannels {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("channel must be between 1 and 512"))
			return
		}
		if update.Value < 0 || update.Value > 255 {
			c.mu.Unlock()
			writeError(w, http.StatusBadRequest, fmt.Errorf("channel %d must be between 0 and 255", update.Channel))
			return
		}
		c.dmx[update.Channel-1] = byte(update.Value)
	}
	c.mu.Unlock()
	if err := c.persist(false); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	c.sendCurrentFrame(true)
	writeJSON(w, c.status())
}

func (c *Controller) handleBlackout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}

	c.mu.Lock()
	for i := range c.dmx {
		c.dmx[i] = 0
	}
	c.mu.Unlock()
	if err := c.persist(true); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	c.sendCurrentFrame(true)
	writeJSON(w, c.status())
}

func (c *Controller) status() StatusResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sendIP, connected := deliveryTarget(c.targetIP, c.nodes)
	connectionNote := "No Art-Net target selected"
	if c.targetIP != "" && connected {
		connectionNote = fmt.Sprintf("Art-Net reply verified; DMX is sent to %s:%d, universe %d", sendIP, artNetPort, c.universe)
	} else if c.targetIP != "" {
		connectionNote = fmt.Sprintf("No ArtPoll reply from %s; UDP delivery is unverified", c.targetIP)
	}

	lastSent := ""
	if !c.lastSent.IsZero() {
		lastSent = c.lastSent.Format(time.RFC3339)
	}
	return StatusResponse{
		TargetIP:       c.targetIP,
		SendIP:         sendIP,
		Universe:       c.universe,
		Streaming:      c.streaming,
		Connected:      connected,
		FPS:            c.fps,
		SocketNote:     c.socketNote,
		ConnectionNote: connectionNote,
		LastSent:       lastSent,
		LastSendErr:    c.lastSendErr,
		Nodes:          append([]ArtNetNode(nil), c.nodes...),
	}
}

func (c *Controller) Discover(timeout time.Duration) ([]ArtNetNode, error) {
	c.discoverMu.Lock()
	defer c.discoverMu.Unlock()

	if c.conn == nil {
		return nil, errors.New("Art-Net UDP socket is not open")
	}

	packet := buildArtPoll()
	c.mu.RLock()
	manualTarget := c.targetIP
	c.mu.RUnlock()
	for _, ip := range discoveryAddresses(manualTarget) {
		addr := &net.UDPAddr{IP: ip, Port: artNetPort}
		_, _ = c.conn.WriteToUDP(packet, addr)
	}

	deadline := time.Now().Add(timeout)
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	defer c.conn.SetReadDeadline(time.Time{})

	seen := map[string]ArtNetNode{}
	buf := make([]byte, 1024)
	for {
		n, remote, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			return nil, err
		}

		node, ok := parseArtPollReply(buf[:n], remote)
		if !ok {
			continue
		}
		key := node.IP + "-" + strconv.Itoa(node.BindIndex)
		seen[key] = node
	}

	nodes := make([]ArtNetNode, 0, len(seen))
	for _, node := range seen {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].IP == nodes[j].IP {
			return nodes[i].BindIndex < nodes[j].BindIndex
		}
		return ipLess(nodes[i].IP, nodes[j].IP)
	})

	return nodes, nil
}

func (c *Controller) sendCurrentFrame(force bool) {
	c.mu.Lock()
	if !force && !c.streaming {
		c.mu.Unlock()
		return
	}
	if c.conn == nil {
		c.lastSendErr = "Art-Net UDP socket is not open"
		c.mu.Unlock()
		return
	}
	if c.targetIP == "" {
		c.mu.Unlock()
		return
	}

	target, _ := deliveryTarget(c.targetIP, c.nodes)
	universe := c.universe
	c.sequence++
	if c.sequence == 0 {
		c.sequence = 1
	}
	sequence := c.sequence
	frame := make([]byte, maxDMXChannels)
	copy(frame, c.dmx[:])
	c.mu.Unlock()

	targetIP := net.ParseIP(target).To4()
	if targetIP == nil {
		c.mu.Lock()
		c.lastSendErr = "Art-Net target is not a valid IPv4 address"
		c.mu.Unlock()
		return
	}

	packet := buildArtDmx(universe, sequence, frame)
	_, err := c.conn.WriteToUDP(packet, &net.UDPAddr{IP: targetIP, Port: artNetPort})

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.lastSendErr = err.Error()
		return
	}
	c.lastSendErr = ""
	c.lastSent = time.Now()
}

func buildArtPoll() []byte {
	packet := make([]byte, 14)
	copy(packet[0:8], artNetID)
	binary.LittleEndian.PutUint16(packet[8:10], opPoll)
	packet[10] = protocolHi
	packet[11] = protocolLo
	packet[12] = 0x00
	packet[13] = 0x00
	return packet
}

func buildArtDmx(universe uint16, sequence byte, data []byte) []byte {
	length := len(data)
	if length < 2 {
		length = 2
	}
	if length > maxDMXChannels {
		length = maxDMXChannels
	}
	if length%2 == 1 {
		length++
	}

	packet := make([]byte, 18+length)
	copy(packet[0:8], artNetID)
	binary.LittleEndian.PutUint16(packet[8:10], opDmx)
	packet[10] = protocolHi
	packet[11] = protocolLo
	packet[12] = sequence
	packet[13] = 0
	packet[14] = byte(universe & 0xff)
	packet[15] = byte((universe >> 8) & 0x7f)
	packet[16] = byte(length >> 8)
	packet[17] = byte(length & 0xff)
	copy(packet[18:], data[:min(length, len(data))])
	return packet
}

func parseArtPollReply(packet []byte, remote *net.UDPAddr) (ArtNetNode, bool) {
	if len(packet) < 207 || !isArtNetPacket(packet) {
		return ArtNetNode{}, false
	}
	if binary.LittleEndian.Uint16(packet[8:10]) != opPollReply {
		return ArtNetNode{}, false
	}

	ip := net.IPv4(packet[10], packet[11], packet[12], packet[13]).String()
	if ip == "0.0.0.0" && remote != nil {
		ip = remote.IP.String()
	}
	netSwitch := uint16(packet[18] & 0x7f)
	subSwitch := uint16(packet[19] & 0x0f)
	numPorts := int(binary.BigEndian.Uint16(packet[172:174]))
	if numPorts > 4 {
		numPorts = 4
	}

	outputs := make([]OutputPort, 0, 4)
	for i := 0; i < 4; i++ {
		if packet[174+i]&0x80 == 0 {
			continue
		}
		universe := (netSwitch << 8) | (subSwitch << 4) | uint16(packet[190+i]&0x0f)
		outputs = append(outputs, OutputPort{
			Index:    i + 1,
			Universe: universe,
			Status:   outputStatus(packet[182+i]),
		})
	}

	mac := ""
	if len(packet) >= 207 {
		allZero := true
		for _, value := range packet[201:207] {
			if value != 0 {
				allZero = false
				break
			}
		}
		if !allZero {
			mac = fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", packet[201], packet[202], packet[203], packet[204], packet[205], packet[206])
		}
	}

	bindIndex := 0
	if len(packet) > 211 {
		bindIndex = int(packet[211])
	}
	webConfig := false
	if len(packet) > 212 {
		webConfig = packet[212]&0x01 != 0
	}

	node := ArtNetNode{
		IP:          ip,
		RemoteIP:    remoteIP(remote),
		ShortName:   fixedString(packet[26:44]),
		LongName:    fixedString(packet[44:108]),
		NodeReport:  fixedString(packet[108:172]),
		NumPorts:    numPorts,
		Style:       styleName(packet[200]),
		Firmware:    fmt.Sprintf("%d.%d", packet[16], packet[17]),
		MAC:         mac,
		BindIndex:   bindIndex,
		WebConfig:   webConfig,
		RDMCapable:  packet[23]&0x02 != 0,
		OutputPorts: outputs,
	}
	return node, true
}

func outputStatus(goodOutput byte) string {
	parts := []string{}
	if goodOutput&0x80 != 0 {
		parts = append(parts, "outputting")
	}
	if goodOutput&0x08 != 0 {
		parts = append(parts, "merging")
	}
	if goodOutput&0x04 != 0 {
		parts = append(parts, "short")
	}
	if goodOutput&0x02 != 0 {
		parts = append(parts, "LTP")
	}
	if len(parts) == 0 {
		return "idle"
	}
	return strings.Join(parts, ", ")
}

func isArtNetPacket(packet []byte) bool {
	if len(packet) < 10 {
		return false
	}
	for i, value := range artNetID {
		if packet[i] != value {
			return false
		}
	}
	return true
}

func fixedString(raw []byte) string {
	if i := strings.IndexByte(string(raw), 0); i >= 0 {
		raw = raw[:i]
	}
	return strings.TrimSpace(string(raw))
}

func remoteIP(remote *net.UDPAddr) string {
	if remote == nil {
		return ""
	}
	return remote.IP.String()
}

func styleName(style byte) string {
	switch style {
	case 0x00:
		return "Node"
	case 0x01:
		return "Controller"
	case 0x02:
		return "Media Server"
	case 0x03:
		return "Router"
	case 0x04:
		return "Backup"
	case 0x05:
		return "Config"
	case 0x06:
		return "Visualiser"
	default:
		return fmt.Sprintf("Style %d", style)
	}
}

func deliveryTarget(target string, nodes []ArtNetNode) (string, bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false
	}

	for _, node := range nodes {
		if target != node.IP && target != node.RemoteIP {
			continue
		}
		if remote := net.ParseIP(node.RemoteIP).To4(); remote != nil {
			return remote.String(), true
		}
		if advertised := net.ParseIP(node.IP).To4(); advertised != nil {
			return advertised.String(), true
		}
	}
	return target, false
}

func discoveryAddresses(manualTarget string) []net.IP {
	addresses := broadcastAddresses()
	target := net.ParseIP(strings.TrimSpace(manualTarget)).To4()
	if target == nil {
		return addresses
	}
	for _, address := range addresses {
		if address.Equal(target) {
			return addresses
		}
	}
	return append(addresses, target)
}

func broadcastAddresses() []net.IP {
	seen := map[string]bool{}
	var addresses []net.IP
	add := func(ip net.IP) {
		if ip == nil {
			return
		}
		key := ip.String()
		if seen[key] {
			return
		}
		seen[key] = true
		addresses = append(addresses, ip)
	}

	add(net.IPv4(2, 255, 255, 255))
	add(net.IPv4(10, 255, 255, 255))

	interfaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if !ok {
					continue
				}
				ip := ipNet.IP.To4()
				if ip == nil || len(ipNet.Mask) != 4 {
					continue
				}
				broadcast := net.IPv4(
					ip[0]|^ipNet.Mask[0],
					ip[1]|^ipNet.Mask[1],
					ip[2]|^ipNet.Mask[2],
					ip[3]|^ipNet.Mask[3],
				)
				add(broadcast)
			}
		}
	}

	add(net.IPv4(255, 255, 255, 255))
	return addresses
}

func ipLess(left, right string) bool {
	leftIP := net.ParseIP(left).To4()
	rightIP := net.ParseIP(right).To4()
	if leftIP == nil || rightIP == nil {
		return left < right
	}
	for i := 0; i < 4; i++ {
		if leftIP[i] == rightIP[i] {
			continue
		}
		return leftIP[i] < rightIP[i]
	}
	return false
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write json: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(indexHTML))
}

const indexHTML = `<!doctype html>
<html lang="da">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Art-Net Controller</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7f8;
      --panel: #ffffff;
      --panel-soft: #eef3f1;
      --ink: #17201d;
      --muted: #5f6b66;
      --line: #d8e0dc;
      --teal: #087f71;
      --teal-strong: #06695e;
      --amber: #b46a00;
      --red: #c73535;
      --magenta: #9c3c72;
      --blue: #3267ad;
      --shadow: 0 10px 24px rgba(15, 36, 31, 0.08);
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; background: var(--bg); color: var(--ink); font-family: "Segoe UI", system-ui, sans-serif; }
    body { overflow-x: hidden; }
    button, input, select { font: inherit; }
    button {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      min-height: 38px;
      padding: 0 12px;
      border-radius: 6px;
      cursor: pointer;
    }
    button:hover { border-color: #9db2aa; }
    button.primary { background: var(--teal); border-color: var(--teal); color: #fff; }
    button.primary:hover { background: var(--teal-strong); }
    button.warn { color: #fff; background: var(--red); border-color: var(--red); }
    button.ghost { background: transparent; }
    input, select {
      width: 100%;
      max-width: 100%;
      min-width: 0;
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      color: var(--ink);
      padding: 0 10px;
    }
    input[type="range"] { padding: 0; accent-color: var(--teal); }
    label { display: block; color: var(--muted); font-size: 12px; font-weight: 650; margin-bottom: 5px; }
    .app { min-height: 100vh; display: grid; grid-template-rows: auto 1fr; }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 18px;
      background: #fff;
      border-bottom: 1px solid var(--line);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    .brand { display: flex; align-items: baseline; gap: 10px; min-width: 0; }
    .brand h1 { margin: 0; font-size: 18px; line-height: 1.2; font-weight: 750; white-space: nowrap; }
    .brand span { color: var(--muted); font-size: 13px; white-space: nowrap; }
    .top-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 32px;
      padding: 0 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--panel-soft);
      color: var(--muted);
      font-size: 13px;
      max-width: 360px;
    }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--amber); flex: 0 0 auto; }
    .dot.live { background: var(--teal); }
    .dot.error { background: var(--red); }
    .layout {
      display: grid;
      grid-template-columns: minmax(260px, 330px) minmax(420px, 1fr) minmax(290px, 390px);
      gap: 14px;
      padding: 14px;
      align-items: start;
    }
    .panel {
      min-width: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .panel-head {
      min-width: 0;
      min-height: 48px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .panel-head h2 { margin: 0; font-size: 14px; line-height: 1.2; letter-spacing: 0; }
    .panel-body { padding: 14px; }
    .stack { display: grid; gap: 12px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .row > *, .patch-tools > *, .channel-editor > * { min-width: 0; }
    .node-list { display: grid; gap: 8px; }
    .node-button {
      text-align: left;
      display: grid;
      gap: 4px;
      min-height: 74px;
      padding: 10px;
      border-radius: 7px;
      border: 1px solid var(--line);
      background: #fff;
      cursor: default;
    }
    .node-button.active { border-color: var(--teal); box-shadow: 0 0 0 2px rgba(8, 127, 113, .12); }
    .node-main {
      min-height: 0;
      padding: 0;
      border: 0;
      background: transparent;
      text-align: left;
      display: grid;
      gap: 4px;
    }
    .node-main:hover { border-color: transparent; }
    .node-title { font-weight: 700; overflow-wrap: anywhere; }
    .node-meta { color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }
    .ports { display: flex; gap: 6px; flex-wrap: wrap; }
    .port-chip {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 3px 7px;
      min-height: 28px;
      color: var(--muted);
      background: var(--panel-soft);
      font-size: 12px;
    }
    .port-chip.active { border-color: var(--teal); color: var(--teal-strong); background: #e7f4f0; }
    .patch-tools {
      display: grid;
      grid-template-columns: minmax(150px, 1.1fr) minmax(80px, .55fr) minmax(120px, .7fr) auto;
      gap: 10px;
      align-items: end;
    }
    .fixture-list { display: grid; gap: 10px; margin-top: 14px; }
    .fixture {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      overflow: hidden;
    }
    .fixture-head {
      display: grid;
      grid-template-columns: minmax(120px, 1fr) auto auto;
      align-items: center;
      gap: 8px;
      padding: 10px;
      background: #fbfcfc;
      border-bottom: 1px solid var(--line);
    }
    .fixture-title { font-weight: 750; overflow-wrap: anywhere; }
    .fixture-address { color: var(--muted); font-size: 12px; white-space: nowrap; }
    .controls { display: grid; gap: 8px; padding: 10px; }
    .preset-control {
      display: grid;
      grid-template-columns: minmax(110px, 1fr) minmax(180px, 1.5fr);
      gap: 9px;
      align-items: center;
    }
    .segment-control { display: grid; gap: 8px; padding: 10px 0; border-top: 1px solid var(--line); }
    .segment-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; }
    .segment-title { font-size: 13px; font-weight: 700; }
    .segment-color { width: 42px; min-height: 30px; padding: 2px; cursor: pointer; }
    .segment-sliders { display: grid; gap: 6px; }
    .control {
      display: grid;
      grid-template-columns: minmax(88px, 1fr) minmax(120px, 180px) 56px;
      gap: 9px;
      align-items: center;
      min-height: 36px;
    }
    .control-name { color: var(--ink); font-size: 13px; overflow-wrap: anywhere; }
    .value-input { min-height: 32px; padding: 0 7px; text-align: right; }
    .swatch-row { display: flex; gap: 7px; flex-wrap: wrap; }
    .swatch {
      width: 28px;
      height: 28px;
      border-radius: 6px;
      border: 1px solid rgba(0, 0, 0, .18);
      cursor: pointer;
    }
    .swatch.white { background: #fff; }
    .swatch.red { background: #d83232; }
    .swatch.green { background: #198c4d; }
    .swatch.blue { background: #2769d8; }
    .swatch.amber { background: #efa51d; }
    .swatch.magenta { background: #c34c93; }
    .swatch.cyan { background: #2caeb5; }
    .quick-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
    .universe-grid {
      display: grid;
      grid-template-columns: repeat(16, minmax(0, 1fr));
      gap: 4px;
    }
    .channel-cell {
      border: 1px solid var(--line);
      border-radius: 5px;
      background: #fff;
      min-height: 34px;
      display: grid;
      place-items: center;
      font-size: 11px;
      color: var(--muted);
      cursor: pointer;
      position: relative;
      overflow: hidden;
    }
    .channel-cell::before {
      content: "";
      position: absolute;
      inset: auto 0 0 0;
      height: var(--level, 0%);
      background: rgba(8, 127, 113, .22);
    }
    .channel-cell span { position: relative; z-index: 1; }
    .channel-cell.active { border-color: var(--teal); color: var(--ink); }
    .channel-editor {
      display: grid;
      grid-template-columns: 1fr 90px;
      gap: 10px;
      align-items: end;
      margin-bottom: 12px;
    }
    .empty {
      color: var(--muted);
      background: var(--panel-soft);
      border: 1px dashed #b8c7c1;
      border-radius: 8px;
      padding: 14px;
      font-size: 13px;
    }
    .logline {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
      line-height: 1.35;
    }
    .audio-panel { grid-column: 1; }
    .panel-status { color: var(--muted); font-size: 12px; font-weight: 700; }
    .audio-meter {
      height: 14px;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 4px;
      background: #e8edeb;
    }
    .audio-meter-level {
      width: 0;
      height: 100%;
      background: linear-gradient(90deg, var(--teal) 0 62%, var(--amber) 80%, var(--red) 100%);
      transition: width 60ms linear;
    }
    .audio-control-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .audio-control-head label { margin: 0; }
    .audio-control-head output { color: var(--muted); font-size: 12px; font-variant-numeric: tabular-nums; }
    .tabs { display: flex; align-items: center; gap: 6px; }
    .tab { min-height: 32px; padding: 0 10px; }
    .tab.active { background: var(--ink); color: #fff; border-color: var(--ink); }
    @media (max-width: 1180px) {
      .layout { grid-template-columns: 320px 1fr; }
      .channel-panel { grid-column: 1 / -1; }
    }
    @media (max-width: 760px) {
      .topbar { align-items: flex-start; flex-direction: column; }
      .top-actions { justify-content: flex-start; }
      .layout { grid-template-columns: 1fr; padding: 10px; }
      .row, .patch-tools, .fixture-head, .control, .channel-editor, .preset-control { grid-template-columns: 1fr; }
      .universe-grid { grid-template-columns: repeat(8, minmax(0, 1fr)); }
      .brand { flex-wrap: wrap; }
      .brand span { white-space: normal; }
      .audio-panel { grid-column: auto; }
    }
  </style>
</head>
<body>
  <div class="app">
    <header class="topbar">
      <div class="brand">
        <h1>Art-Net Controller</h1>
        <span id="targetSummary">Ingen node valgt</span>
      </div>
      <div class="top-actions">
        <div class="status-pill"><span id="liveDot" class="dot"></span><span id="liveText">Klar</span></div>
        <button id="blackoutBtn" class="warn" type="button">Blackout</button>
        <button id="discoverBtn" class="primary" type="button">Discovery</button>
      </div>
    </header>

    <main class="layout">
      <section class="panel">
        <div class="panel-head">
          <h2>Node</h2>
          <button id="refreshStatusBtn" class="ghost" type="button">Status</button>
        </div>
        <div class="panel-body stack">
          <div>
            <label for="targetIp">Target IP</label>
            <input id="targetIp" inputmode="decimal" placeholder="2.x.x.x / 10.x.x.x / 192.168.x.x">
          </div>
          <div class="row">
            <div>
              <label for="universe">Universe</label>
              <input id="universe" type="number" min="0" max="32767" value="0">
            </div>
            <div>
              <label for="fps">Refresh</label>
              <select id="fps">
                <option value="15">15 Hz</option>
                <option value="25">25 Hz</option>
                <option value="30" selected>30 Hz</option>
                <option value="40">40 Hz</option>
                <option value="44">44 Hz</option>
              </select>
            </div>
          </div>
          <div class="row">
            <button id="streamBtn" type="button">Start output</button>
            <button id="sendOnceBtn" type="button">Send frame</button>
          </div>
          <div id="connectionNote" class="logline"></div>
          <div id="socketNote" class="logline"></div>
          <div class="node-list" id="nodeList"></div>
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h2>Fixtures</h2>
          <div class="tabs">
            <button id="savePatchBtn" class="tab active" type="button">Gem patch</button>
            <button id="clearPatchBtn" class="tab" type="button">Ryd</button>
          </div>
        </div>
        <div class="panel-body">
          <div class="patch-tools">
            <div>
              <label for="fixtureType">Type</label>
              <select id="fixtureType"></select>
            </div>
            <div>
              <label for="fixtureStart">Start</label>
              <input id="fixtureStart" type="number" min="1" max="512" value="1">
            </div>
            <div>
              <label for="fixtureName">Navn</label>
              <input id="fixtureName" placeholder="Fixture">
            </div>
            <button id="addFixtureBtn" class="primary" type="button">Tilføj</button>
          </div>
          <div class="fixture-list" id="fixtureList"></div>
        </div>
      </section>

      <section class="panel channel-panel">
        <div class="panel-head">
          <h2>Universe</h2>
          <div class="tabs">
            <button id="pagePrevBtn" class="tab" type="button">-32</button>
            <button id="pageNextBtn" class="tab" type="button">+32</button>
          </div>
        </div>
        <div class="panel-body">
          <div class="channel-editor">
            <div>
              <label id="channelLabel" for="channelRange">Kanal 1</label>
              <input id="channelRange" type="range" min="0" max="255" value="0">
            </div>
            <div>
              <label for="channelValue">Værdi</label>
              <input id="channelValue" class="value-input" type="number" min="0" max="255" value="0">
            </div>
          </div>
          <div class="quick-grid">
            <button data-level="0" type="button">0</button>
            <button data-level="127" type="button">127</button>
            <button data-level="255" type="button">255</button>
          </div>
          <div style="height: 12px"></div>
          <div id="universeGrid" class="universe-grid"></div>
        </div>
      </section>

      <section class="panel audio-panel">
        <div class="panel-head">
          <h2>Lydstyring</h2>
          <div class="tabs">
            <span id="micState" class="panel-status">Fra</span>
            <button id="audioRefreshBtn" class="tab" type="button">Opdater</button>
          </div>
        </div>
        <div class="panel-body stack">
          <div id="micMeter" class="audio-meter" role="meter" aria-label="Lydeniveau" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
            <div id="micMeterLevel" class="audio-meter-level"></div>
          </div>
          <div>
            <label for="micSource">Lydkilde på localhost</label>
            <select id="micSource"><option value="">Indlæser lydkilder</option></select>
          </div>
          <div class="row">
            <div>
              <label for="micFixture">Fixtures</label>
              <select id="micFixture"><option value="all">Alle fixtures</option></select>
            </div>
            <div>
              <label for="micMode">Mode</label>
              <select id="micMode">
                <option value="random">Fuld random</option>
                <option value="color">Farveskift</option>
                <option value="segments">Random segmenter</option>
                <option value="chase">Chase</option>
                <option value="pulse">Niveau-puls</option>
              </select>
            </div>
          </div>
          <div>
            <div class="audio-control-head">
              <label for="micSensitivity">Følsomhed</label>
              <output id="micSensitivityValue" for="micSensitivity">65%</output>
            </div>
            <input id="micSensitivity" type="range" min="0" max="100" value="65">
          </div>
          <div>
            <div class="audio-control-head">
              <label for="micIntensity">Intensitet</label>
              <output id="micIntensityValue" for="micIntensity">86%</output>
            </div>
            <input id="micIntensity" type="range" min="10" max="255" value="220">
          </div>
          <div class="row">
            <div>
              <label for="micTempo">Beat-rate</label>
              <select id="micTempo">
                <option value="140">Hurtig</option>
                <option value="240" selected>Normal</option>
                <option value="400">Rolig</option>
              </select>
            </div>
            <div>
              <label for="micMovement">Movement</label>
              <select id="micMovement">
                <option value="wide" selected>Wide</option>
                <option value="compact">Compact</option>
                <option value="off">Off</option>
              </select>
            </div>
          </div>
          <div class="row">
            <button id="micBtn" class="primary" type="button">Start lyd</button>
            <button id="micRandomBtn" type="button">Ny scene</button>
          </div>
          <div id="micStatus" class="logline">Klar</div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const barbaraShowPresets = [
      { label: "Off", value: 0 },
      { label: "Red", value: 8 },
      { label: "Yellow", value: 16 },
      { label: "Green", value: 24 },
      { label: "Cyan", value: 32 },
      { label: "Blue", value: 40 },
      { label: "Magenta", value: 48 },
      { label: "White", value: 56 },
      ...Array.from({ length: 21 }, (_, index) => ({ label: "Auto programme " + (index + 1), value: 64 + index * 8 })),
      { label: "Sound control", value: 232 }
    ];
    const barbaraSegmentChannels = Array.from({ length: 8 }, (_, index) =>
      ["Red", "Green", "Blue"].map((color) => "Segment " + (index + 1) + " / " + color)
    ).flat();
    const barbaraColorGroups = Array.from({ length: 8 }, (_, index) => ({
      name: "Segment " + (index + 1),
      offsets: { Red: index * 3, Green: index * 3 + 1, Blue: index * 3 + 2 }
    }));

    const templates = [
      {
        id: "fun-generation-barbara-24-2ch",
        name: "Fun Generation LED BARbara 24 / 2ch",
        channels: ["Show / Color", "Speed / Sound sensitivity"],
        presets: [{ offset: 0, label: "Show / Color preset", options: barbaraShowPresets }]
      },
      {
        id: "fun-generation-barbara-24-3ch",
        name: "Fun Generation LED BARbara 24 / 3ch RGB",
        channels: ["Red", "Green", "Blue"]
      },
      {
        id: "fun-generation-barbara-24-5ch",
        name: "Fun Generation LED BARbara 24 / 5ch RGB + FX",
        channels: ["Red", "Green", "Blue", "Dimmer", "Strobe"],
        colorDefaults: { 3: 255 }
      },
      {
        id: "fun-generation-barbara-24-24ch",
        name: "Fun Generation LED BARbara 24 / 24ch segments",
        channels: barbaraSegmentChannels,
        colorGroups: barbaraColorGroups
      },
      { id: "dimmer", name: "Dimmer 1ch", channels: ["Dimmer"] },
      { id: "rgb", name: "RGB 3ch", channels: ["Red", "Green", "Blue"] },
      { id: "rgbw", name: "RGBW 4ch", channels: ["Red", "Green", "Blue", "White"] },
      { id: "par7", name: "PAR 7ch", channels: ["Dimmer", "Red", "Green", "Blue", "White", "Strobe", "Macro"] },
      { id: "mh8", name: "Moving Head 8ch", channels: ["Pan", "Tilt", "Pan Fine", "Tilt Fine", "Speed", "Dimmer", "Strobe", "Color"] },
      { id: "mh16", name: "Moving Head 16ch", channels: ["Pan", "Pan Fine", "Tilt", "Tilt Fine", "Speed", "Dimmer", "Strobe", "Red", "Green", "Blue", "White", "Color", "Gobo", "Focus", "Prism", "Reset"] }
    ];

    const reactiveColors = [
      [255, 28, 18],
      [255, 116, 0],
      [245, 220, 24],
      [24, 225, 92],
      [0, 210, 255],
      [38, 92, 255],
      [196, 42, 255],
      [255, 35, 146],
      [255, 255, 255]
    ];

    const state = {
      values: new Array(512).fill(0),
      fixtures: JSON.parse(localStorage.getItem("artnet.fixtures") || "[]"),
      selectedChannel: 1,
      pageStart: 1,
      nodes: [],
      streaming: false,
      connected: false,
      targetIp: "",
      sendIp: "",
      universe: 0,
      fps: 30,
      pendingTimer: null,
      fixtureSaveTimer: null,
      audioSaveTimer: null,
      audio: {
        enabled: false,
        starting: false,
        sourceId: "",
        eventSource: null,
        levelAverage: 0.02,
        lastBeat: 0,
        lastLevelUpdate: 0,
        chaseIndex: 0,
        colorIndex: 0,
        positions: {}
      }
    };

    const $ = (id) => document.getElementById(id);

    function clamp(value, min, max) {
      const parsed = Number.parseInt(value, 10);
      if (Number.isNaN(parsed)) return min;
      return Math.max(min, Math.min(max, parsed));
    }

    async function api(path, body) {
      const options = body === undefined ? {} : {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      };
      const response = await fetch(path, options);
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.error || response.statusText);
      return data;
    }

    function setLive(text, mode) {
      $("liveText").textContent = text;
      $("liveDot").className = "dot" + (mode ? " " + mode : "");
    }

    function saveFixtures() {
      localStorage.setItem("artnet.fixtures", JSON.stringify(state.fixtures));
      window.clearTimeout(state.fixtureSaveTimer);
      state.fixtureSaveTimer = window.setTimeout(() => {
        api("/api/runtime", { fixtures: state.fixtures }).catch((error) => setLive(error.message, "error"));
      }, 100);
    }

    function saveReactiveSettings() {
      window.clearTimeout(state.audioSaveTimer);
      state.audioSaveTimer = window.setTimeout(() => {
        const sourceId = $("micSource").value || state.audio.sourceId || "";
        state.audio.sourceId = sourceId;
        api("/api/runtime", { audio: {
          sourceId,
          fixtureId: $("micFixture").value || "all",
          mode: $("micMode").value,
          sensitivity: clamp($("micSensitivity").value, 0, 100),
          intensity: clamp($("micIntensity").value, 10, 255),
          tempoMs: clamp($("micTempo").value, 100, 1000),
          movement: $("micMovement").value
        }}).catch((error) => setLive(error.message, "error"));
      }, 100);
    }

    async function loadRuntimeState() {
      const localFixtures = state.fixtures;
      try {
        let runtime = await api("/api/runtime");
        if (!(runtime.fixtures || []).length && localFixtures.length) {
          runtime = await api("/api/runtime", { fixtures: localFixtures });
        }
        state.fixtures = runtime.fixtures || [];
        localStorage.setItem("artnet.fixtures", JSON.stringify(state.fixtures));
        const controller = runtime.controller || {};
        if (Array.isArray(controller.dmx) && controller.dmx.length === 512) {
          state.values = controller.dmx.map((value) => clamp(value, 0, 255));
        }
        const audio = runtime.audio || {};
        state.audio.sourceId = audio.sourceId || "";
        state.audio.enabled = Boolean(audio.active);
        renderFixtures();
        $("micFixture").value = audio.fixtureId || "all";
        $("micMode").value = audio.mode || "random";
        $("micSensitivity").value = String(audio.sensitivity ?? 65);
        $("micIntensity").value = String(audio.intensity ?? 220);
        $("micTempo").value = String(audio.tempoMs ?? 240);
        $("micMovement").value = audio.movement || "wide";
        $("micSensitivityValue").textContent = $("micSensitivity").value + "%";
        $("micIntensityValue").textContent = Math.round(Number($("micIntensity").value) / 255 * 100) + "%";
        renderUniverseGrid();
      } catch (error) {
        setLive(error.message, "error");
      }
    }

    function setChannel(channel, value, sync = true) {
      const index = channel - 1;
      if (index < 0 || index >= 512) return;
      state.values[index] = clamp(value, 0, 255);
      if (state.selectedChannel === channel) {
        $("channelRange").value = state.values[index];
        $("channelValue").value = state.values[index];
      }
      updateUniverseCells();
      updateFixtureInputs(channel);
      updateColorInputs(channel);
      if (sync) scheduleDMX();
    }

    function scheduleDMX() {
      window.clearTimeout(state.pendingTimer);
      state.pendingTimer = window.setTimeout(sendDMX, 45);
    }

    async function sendDMX() {
      try {
        const status = await api("/api/dmx", { values: state.values });
        applyStatus(status);
        if (state.connected) setLive(state.streaming ? "Sender" : "Frame sendt", state.streaming ? "live" : "");
        else setLive("Frame sendt, men node ikke bekraeftet", "");
      } catch (error) {
        setLive(error.message, "error");
      }
    }

    async function configure(partial = {}) {
      const payload = {
        targetIp: $("targetIp").value.trim(),
        universe: clamp($("universe").value, 0, 32767),
        fps: clamp($("fps").value, 1, 44),
        ...partial
      };
      try {
        const status = await api("/api/configure", payload);
        applyStatus(status);
      } catch (error) {
        setLive(error.message, "error");
      }
    }

    function applyStatus(status) {
      state.nodes = status.nodes || [];
      state.streaming = Boolean(status.streaming);
      state.connected = Boolean(status.connected);
      state.targetIp = status.targetIp || "";
      state.sendIp = status.sendIp || state.targetIp;
      state.universe = status.universe || 0;
      state.fps = status.fps || 30;
      $("targetIp").value = state.targetIp;
      $("universe").value = state.universe;
      $("fps").value = String(state.fps);
      $("streamBtn").textContent = state.streaming ? "Stop output" : "Start output";
      $("targetSummary").textContent = state.targetIp ? state.targetIp + " / universe " + state.universe : "Ingen node valgt";
      $("connectionNote").textContent = status.connectionNote || "";
      $("socketNote").textContent = status.socketNote || "";
      if (status.lastSendErr) setLive(status.lastSendErr, "error");
      else if (state.streaming && state.connected) setLive("Sender", "live");
      else if (state.streaming) setLive("Sender, node ikke bekraeftet", "");
      else setLive(state.connected ? "Forbundet" : "Klar", state.connected ? "live" : "");
      renderNodes();
    }

    function renderNodes() {
      const list = $("nodeList");
      list.innerHTML = "";
      if (!state.nodes.length) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "Ingen noder fundet";
        list.appendChild(empty);
        return;
      }

      for (const node of state.nodes) {
        const card = document.createElement("div");
        const isTarget = node.ip === state.targetIp || node.remoteIp === state.targetIp;
        card.className = "node-button" + (isTarget ? " active" : "");
        const main = document.createElement("button");
        main.type = "button";
        main.className = "node-main";
        const title = document.createElement("div");
        title.className = "node-title";
        title.textContent = node.longName || node.shortName || node.ip;
        const meta = document.createElement("div");
        meta.className = "node-meta";
        const route = node.remoteIp && node.remoteIp !== node.ip ? " via " + node.remoteIp : "";
        meta.textContent = node.ip + route + " · " + node.style + " · " + node.numPorts + " porte";
        main.append(title, meta);
        main.addEventListener("click", () => selectNodePort(node, (node.outputPorts || [])[0]));
        const ports = document.createElement("div");
        ports.className = "ports";
        for (const port of node.outputPorts || []) {
          const chip = document.createElement("button");
          chip.type = "button";
          chip.className = "port-chip" + (isTarget && port.universe === state.universe ? " active" : "");
          chip.textContent = "Out " + port.index + " / U" + port.universe;
          chip.title = portAddressTitle(port.universe) + " / " + port.status;
          chip.addEventListener("click", () => selectNodePort(node, port));
          ports.appendChild(chip);
        }
        card.append(main, ports);
        list.appendChild(card);
      }
    }

    function selectNodePort(node, port) {
      $("targetIp").value = node.ip;
      if (port) $("universe").value = port.universe;
      configure();
    }

    function portAddressTitle(portAddress) {
      const net = (portAddress >> 8) & 0x7f;
      const subnet = (portAddress >> 4) & 0x0f;
      const universe = portAddress & 0x0f;
      return "Art-Net Net " + net + ", Sub-Net " + subnet + ", Universe " + universe;
    }

    function renderTemplateOptions() {
      const select = $("fixtureType");
      select.innerHTML = "";
      for (const template of templates) {
        const option = document.createElement("option");
        option.value = template.id;
        option.textContent = template.name;
        select.appendChild(option);
      }
    }

    function renderFixtures() {
      const list = $("fixtureList");
      list.innerHTML = "";
      refreshMicFixtureOptions();
      if (!state.fixtures.length) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "Ingen fixtures";
        list.appendChild(empty);
        return;
      }

      for (const fixture of state.fixtures) {
        const template = templates.find((item) => item.id === fixture.type) || templates[0];
        const root = document.createElement("div");
        root.className = "fixture";

        const head = document.createElement("div");
        head.className = "fixture-head";
        const title = document.createElement("div");
        title.className = "fixture-title";
        title.textContent = fixture.name || template.name;
        const address = document.createElement("div");
        address.className = "fixture-address";
        address.textContent = "DMX " + fixture.start + "-" + (fixture.start + template.channels.length - 1);
        const remove = document.createElement("button");
        remove.type = "button";
        remove.textContent = "Fjern";
        remove.addEventListener("click", () => {
          state.fixtures = state.fixtures.filter((item) => item.id !== fixture.id);
          saveFixtures();
          renderFixtures();
        });
        head.append(title, address, remove);

        const controls = document.createElement("div");
        controls.className = "controls";
        for (const preset of template.presets || []) {
          controls.appendChild(presetControl(fixture, preset));
        }
        const colorGroups = getColorGroups(template);
        if (colorGroups.length > 1) {
          controls.appendChild(colorSwatches(fixture, colorGroups, "All segments", template.colorDefaults));
          for (const group of colorGroups) controls.appendChild(segmentControl(fixture, group));
        } else {
          if (colorGroups.length) controls.appendChild(colorSwatches(fixture, colorGroups, "Fixture", template.colorDefaults));
          template.channels.forEach((name, offset) => {
            const channel = fixture.start + offset;
            if (channel < 1 || channel > 512) return;
            controls.appendChild(channelControl(channel, name));
          });
        }

        root.append(head, controls);
        list.appendChild(root);
      }
    }

    function refreshMicFixtureOptions() {
      const select = $("micFixture");
      if (!select) return;
      const selected = select.value || "all";
      select.innerHTML = "";
      const all = document.createElement("option");
      all.value = "all";
      all.textContent = "Alle fixtures";
      select.appendChild(all);
      for (const fixture of state.fixtures) {
        const option = document.createElement("option");
        option.value = fixture.id;
        option.textContent = fixture.name || "Fixture " + fixture.start;
        select.appendChild(option);
      }
      select.value = state.fixtures.some((fixture) => fixture.id === selected) ? selected : "all";
    }

    function getColorGroups(template) {
      if (template.colorGroups) return template.colorGroups;
      const offsets = {};
      ["Red", "Green", "Blue", "White"].forEach((name) => {
        const offset = template.channels.indexOf(name);
        if (offset >= 0) offsets[name] = offset;
      });
      return Object.keys(offsets).length ? [{ name: "Color", offsets }] : [];
    }

    function colorSwatches(fixture, groups, scopeLabel = "Fixture", defaults = {}) {
      const row = document.createElement("div");
      row.className = "swatch-row";
      const colors = [
        ["white", [255, 255, 255, 255]],
        ["red", [255, 0, 0, 0]],
        ["green", [0, 255, 0, 0]],
        ["blue", [0, 0, 255, 0]],
        ["amber", [255, 130, 0, 0]],
        ["magenta", [255, 0, 130, 0]],
        ["cyan", [0, 220, 255, 0]]
      ];
      for (const [name, rgba] of colors) {
        const swatch = document.createElement("button");
        swatch.type = "button";
        swatch.className = "swatch " + name;
        swatch.title = scopeLabel + ": " + name;
        swatch.setAttribute("aria-label", scopeLabel + ": " + name);
        swatch.addEventListener("click", () => {
          for (const group of groups) {
            ["Red", "Green", "Blue", "White"].forEach((channelName, i) => {
              const offset = group.offsets[channelName];
              if (offset !== undefined) setChannel(fixture.start + offset, rgba[i], false);
            });
          }
          for (const [offset, value] of Object.entries(defaults || {})) {
            setChannel(fixture.start + Number(offset), value, false);
          }
          scheduleDMX();
        });
        row.appendChild(swatch);
      }
      return row;
    }

    function presetControl(fixture, preset) {
      const row = document.createElement("div");
      row.className = "preset-control";
      const label = document.createElement("div");
      label.className = "control-name";
      label.textContent = preset.label;
      const select = document.createElement("select");
      const channel = fixture.start + preset.offset;
      select.dataset.presetChannel = String(channel);
      const custom = document.createElement("option");
      custom.value = "";
      custom.textContent = "Custom value";
      select.appendChild(custom);
      for (const optionData of preset.options) {
        const option = document.createElement("option");
        option.value = String(optionData.value);
        option.textContent = optionData.label;
        select.appendChild(option);
      }
      select.value = String(state.values[channel - 1]);
      select.addEventListener("change", () => {
        if (select.value !== "") setChannel(channel, select.value);
      });
      row.append(label, select);
      return row;
    }

    function segmentControl(fixture, group) {
      const root = document.createElement("div");
      root.className = "segment-control";
      const head = document.createElement("div");
      head.className = "segment-head";
      const title = document.createElement("div");
      title.className = "segment-title";
      title.textContent = group.name;
      const color = document.createElement("input");
      color.type = "color";
      color.className = "segment-color";
      const channels = ["Red", "Green", "Blue"].map((name) => fixture.start + group.offsets[name]);
      color.dataset.channels = channels.join(",");
      color.title = group.name + " color";
      color.setAttribute("aria-label", group.name + " color");
      color.value = channelsToHex(channels);
      color.addEventListener("input", () => {
        const rgb = hexToRGB(color.value);
        channels.forEach((channel, index) => setChannel(channel, rgb[index], false));
        scheduleDMX();
      });
      head.append(title, color);
      const sliders = document.createElement("div");
      sliders.className = "segment-sliders";
      for (const name of ["Red", "Green", "Blue"]) {
        sliders.appendChild(channelControl(fixture.start + group.offsets[name], name));
      }
      root.append(head, sliders);
      return root;
    }

    function hexToRGB(value) {
      return [1, 3, 5].map((start) => Number.parseInt(value.slice(start, start + 2), 16));
    }

    function channelsToHex(channels) {
      return "#" + channels.map((channel) => state.values[channel - 1].toString(16).padStart(2, "0")).join("");
    }

    function channelControl(channel, name) {
      const row = document.createElement("div");
      row.className = "control";
      row.dataset.channel = String(channel);
      const label = document.createElement("div");
      label.className = "control-name";
      label.textContent = channel + " · " + name;
      const range = document.createElement("input");
      range.type = "range";
      range.min = "0";
      range.max = "255";
      range.value = state.values[channel - 1];
      const value = document.createElement("input");
      value.className = "value-input";
      value.type = "number";
      value.min = "0";
      value.max = "255";
      value.value = state.values[channel - 1];
      range.addEventListener("input", () => setChannel(channel, range.value));
      value.addEventListener("input", () => setChannel(channel, value.value));
      label.addEventListener("click", () => selectChannel(channel));
      row.append(label, range, value);
      return row;
    }

    function updateFixtureInputs(channel) {
      document.querySelectorAll('.control[data-channel="' + channel + '"]').forEach((row) => {
        const value = state.values[channel - 1];
        const range = row.querySelector('input[type="range"]');
        const number = row.querySelector('input[type="number"]');
        if (range) range.value = value;
        if (number) number.value = value;
      });
      document.querySelectorAll('[data-preset-channel="' + channel + '"]').forEach((select) => {
        const exact = Array.from(select.options).some((option) => option.value === String(state.values[channel - 1]));
        select.value = exact ? String(state.values[channel - 1]) : "";
      });
    }

    function updateColorInputs(channel) {
      document.querySelectorAll(".segment-color[data-channels]").forEach((input) => {
        const channels = input.dataset.channels.split(",").map(Number);
        if (channels.includes(channel)) input.value = channelsToHex(channels);
      });
    }

    function selectedReactiveFixtures() {
      const selected = $("micFixture").value;
      return state.fixtures.filter((fixture) => selected === "all" || fixture.id === selected);
    }

    function chooseReactiveColor() {
      let next = Math.floor(Math.random() * reactiveColors.length);
      if (next === state.audio.colorIndex) next = (next + 1) % reactiveColors.length;
      state.audio.colorIndex = next;
      return reactiveColors[next];
    }

    function currentReactiveColor() {
      return reactiveColors[state.audio.colorIndex] || reactiveColors[0];
    }

    function scaledReactiveColor(color, level) {
      const intensity = clamp($("micIntensity").value, 10, 255) / 255;
      const scale = Math.max(0, Math.min(1, level)) * intensity;
      return color.map((value) => Math.round(value * scale));
    }

    function writeReactiveChannel(channel, value, changed) {
      const index = channel - 1;
      if (index < 0 || index >= 512) return;
      const next = clamp(value, 0, 255);
      if (state.values[index] === next) return;
      state.values[index] = next;
      changed.add(channel);
    }

    function writeReactiveGroup(fixture, group, color, changed) {
      ["Red", "Green", "Blue"].forEach((name, index) => {
        const offset = group.offsets[name];
        if (offset !== undefined) writeReactiveChannel(fixture.start + offset, color[index], changed);
      });
      if (group.offsets.White !== undefined) {
        writeReactiveChannel(fixture.start + group.offsets.White, Math.min(...color), changed);
      }
    }

    function writeReactiveAxis(fixture, template, coarseName, fineName, normalized, changed) {
      const coarseOffset = template.channels.indexOf(coarseName);
      if (coarseOffset < 0) return;
      const fineOffset = template.channels.indexOf(fineName);
      if (fineOffset < 0) {
        writeReactiveChannel(fixture.start + coarseOffset, Math.round(normalized * 255), changed);
        return;
      }
      const value = Math.round(normalized * 65535);
      writeReactiveChannel(fixture.start + coarseOffset, value >> 8, changed);
      writeReactiveChannel(fixture.start + fineOffset, value & 0xff, changed);
    }

    function writeReactiveMovement(fixture, template, changed) {
      const movement = $("micMovement").value;
      if (movement === "off" || template.channels.indexOf("Pan") < 0 || template.channels.indexOf("Tilt") < 0) return;

      const range = movement === "compact"
        ? { panMin: 0.34, panMax: 0.66, tiltMin: 0.32, tiltMax: 0.64 }
        : { panMin: 0.1, panMax: 0.9, tiltMin: 0.16, tiltMax: 0.82 };
      const key = fixture.id || fixture.type + "-" + fixture.start;
      const previous = state.audio.positions[key] || { pan: 0.5, tilt: 0.5 };
      let next = previous;
      for (let attempt = 0; attempt < 6; attempt++) {
        const candidate = {
          pan: range.panMin + Math.random() * (range.panMax - range.panMin),
          tilt: range.tiltMin + Math.random() * (range.tiltMax - range.tiltMin)
        };
        next = candidate;
        if (Math.abs(candidate.pan - previous.pan) + Math.abs(candidate.tilt - previous.tilt) >= 0.2) break;
      }
      state.audio.positions[key] = next;
      writeReactiveAxis(fixture, template, "Pan", "Pan Fine", next.pan, changed);
      writeReactiveAxis(fixture, template, "Tilt", "Tilt Fine", next.tilt, changed);

      const speedOffset = template.channels.indexOf("Speed");
      if (speedOffset >= 0) {
        const cooldown = clamp($("micTempo").value, 100, 1000);
        writeReactiveChannel(fixture.start + speedOffset, clamp(Math.round(cooldown * 0.45), 48, 180), changed);
      }
    }

    function applyReactiveFixture(fixture, mode, color, level, changed, moveHead) {
      const template = templates.find((item) => item.id === fixture.type) || templates[0];
      const intensity = Math.round(clamp($("micIntensity").value, 10, 255) * Math.max(0, Math.min(1, level)));
      if (moveHead) writeReactiveMovement(fixture, template, changed);

      if (template.id === "fun-generation-barbara-24-2ch") {
        const colorPresets = [8, 16, 24, 32, 40, 48, 56];
        if (mode === "random") chooseReactiveColor();
        writeReactiveChannel(fixture.start, colorPresets[state.audio.colorIndex % colorPresets.length], changed);
        writeReactiveChannel(fixture.start + 1, intensity, changed);
        return;
      }

      const groups = getColorGroups(template);
      if (mode === "random") {
        groups.forEach((group) => {
          writeReactiveGroup(fixture, group, scaledReactiveColor(chooseReactiveColor(), level), changed);
        });
      } else if (groups.length > 1 && mode === "chase") {
        const active = state.audio.chaseIndex % groups.length;
        groups.forEach((group, index) => {
          writeReactiveGroup(fixture, group, index === active ? color : [0, 0, 0], changed);
        });
      } else if (groups.length > 1 && mode === "segments") {
        groups.forEach((group, index) => {
          const groupColor = index === 0 || Math.random() > 0.24
            ? scaledReactiveColor(chooseReactiveColor(), level)
            : [0, 0, 0];
          writeReactiveGroup(fixture, group, groupColor, changed);
        });
      } else {
        groups.forEach((group) => writeReactiveGroup(fixture, group, color, changed));
      }

      const colorOffset = template.channels.indexOf("Color");
      if (!groups.length && colorOffset >= 0) {
        const colorWheelValues = [8, 24, 40, 56, 72, 88, 104, 120, 136];
        if (mode === "random") chooseReactiveColor();
        writeReactiveChannel(fixture.start + colorOffset, colorWheelValues[state.audio.colorIndex % colorWheelValues.length], changed);
      }

      const dimmerOffset = template.channels.indexOf("Dimmer");
      if (dimmerOffset >= 0) writeReactiveChannel(fixture.start + dimmerOffset, intensity, changed);
      const strobeOffset = template.channels.indexOf("Strobe");
      if (strobeOffset >= 0) writeReactiveChannel(fixture.start + strobeOffset, 0, changed);
    }

    function commitReactiveChanges(changed) {
      if (!changed.size) return;
      updateUniverseCells();
      changed.forEach((channel) => {
        updateFixtureInputs(channel);
        updateColorInputs(channel);
      });
      scheduleDMX();
    }

    function applyReactiveScene(requestedMode, level = 1, chooseColor = true, moveHeads = chooseColor) {
      const fixtures = selectedReactiveFixtures();
      if (!fixtures.length) {
        $("micStatus").textContent = "Ingen fixture valgt";
        return false;
      }

      const baseColor = chooseColor && requestedMode !== "random" ? chooseReactiveColor() : currentReactiveColor();
      const color = scaledReactiveColor(baseColor, level);
      const changed = new Set();
      fixtures.forEach((fixture) => applyReactiveFixture(fixture, requestedMode, color, level, changed, moveHeads));
      if (requestedMode === "chase") state.audio.chaseIndex++;
      commitReactiveChanges(changed);
      return true;
    }

    function updateMicMeter(level) {
      const percent = Math.round(Math.max(0, Math.min(1, level)) * 100);
      $("micMeterLevel").style.width = percent + "%";
      $("micMeter").setAttribute("aria-valuenow", String(percent));
      return percent;
    }

    function populateHostAudioSources(status) {
      const select = $("micSource");
      const saved = localStorage.getItem("artnet.audioSource") || "";
      const selected = status.sourceId || state.audio.sourceId || select.value || saved;
      select.innerHTML = "";
      for (const source of status.sources || []) {
        const option = document.createElement("option");
        option.value = source.id;
        option.textContent = (source.kind === "loopback" ? "Output · " : "Input · ") + source.name;
        select.appendChild(option);
      }
      if (Array.from(select.options).some((option) => option.value === selected)) select.value = selected;
      if (!select.options.length) {
        const option = document.createElement("option");
        option.value = "";
        option.textContent = "Ingen lydkilder fundet";
        select.appendChild(option);
      }
    }

    async function loadHostAudioSources() {
      try {
        const status = await api("/api/audio");
        populateHostAudioSources(status);
        if (status.active) {
          state.audio.enabled = true;
          state.audio.sourceId = status.sourceId;
          $("micSource").value = status.sourceId;
          $("micBtn").textContent = "Stop lyd";
          $("micState").textContent = "Live";
          $("micStatus").textContent = status.sourceName || "Lyd aktiv";
          connectHostAudioEvents();
        } else if (state.audio.enabled) {
          $("micBtn").textContent = "Stop lyd";
          $("micState").textContent = "Venter";
          $("micStatus").textContent = status.lastError || "Gendanner lydkilde";
          connectHostAudioEvents();
        } else if (status.lastError) {
          $("micStatus").textContent = status.lastError;
        }
      } catch (error) {
        $("micStatus").textContent = error.message;
      }
    }

    function connectHostAudioEvents() {
      if (state.audio.eventSource) state.audio.eventSource.close();
      const stream = new EventSource("/api/audio/events");
      state.audio.eventSource = stream;
      stream.onmessage = (message) => {
        const event = JSON.parse(message.data);
        if (event.error) {
          $("micState").textContent = "Venter";
          $("micStatus").textContent = event.error;
          updateMicMeter(0);
          return;
        }
        if (event.active) processHostAudioLevel(Number(event.level) || 0, event.updates || [], Boolean(event.beat));
      };
      stream.onerror = () => {
        if (state.audio.enabled) $("micStatus").textContent = "Lydstream genopretter";
      };
    }

    function applyServerUpdates(updates) {
      for (const update of updates || []) {
        const channel = clamp(update.channel, 1, 512);
        state.values[channel - 1] = clamp(update.value, 0, 255);
      }
      updateUniverseCells();
      for (const update of updates || []) {
        updateFixtureInputs(update.channel);
        updateColorInputs(update.channel);
      }
    }

    function processHostAudioLevel(rawLevel, updates = [], beat = false) {
      if (!state.audio.enabled) return;
      const level = Math.max(0, Math.min(1, rawLevel));
      const meterLevel = Math.min(1, Math.sqrt(level));
      const percent = updateMicMeter(meterLevel);
      applyServerUpdates(updates);
      $("micState").textContent = beat ? "Beat" : "Live";
      $("micStatus").textContent = (beat ? "Beat · " : "Niveau · ") + percent + "%";
    }

    async function startLocalAudio() {
      if (state.audio.enabled || state.audio.starting) return;
      if (!selectedReactiveFixtures().length) {
        $("micStatus").textContent = "Tilføj et fixture først";
        return;
      }
      const sourceId = $("micSource").value;
      if (!sourceId) {
        $("micStatus").textContent = "Vælg en lydkilde";
        return;
      }

      state.audio.starting = true;
      $("micBtn").disabled = true;
      $("micBtn").textContent = "Forbinder";
      try {
        const status = await api("/api/audio/configure", { sourceId, active: true });
        state.audio.enabled = Boolean(status.active);
        state.audio.sourceId = sourceId;
        state.audio.levelAverage = 0.02;
        state.audio.lastBeat = 0;
        state.audio.lastLevelUpdate = 0;
        localStorage.setItem("artnet.audioSource", sourceId);
        populateHostAudioSources(status);
        connectHostAudioEvents();
        $("micState").textContent = "Live";
        $("micStatus").textContent = status.sourceName || "Lyd aktiv";
      } catch (error) {
        stopLocalAudio(error.message, false);
      } finally {
        state.audio.starting = false;
        $("micBtn").disabled = false;
        $("micBtn").textContent = state.audio.enabled ? "Stop lyd" : "Start lyd";
      }
    }

    function stopLocalAudio(statusText = "Stoppet", notifyServer = true) {
      const audio = state.audio;
      audio.enabled = false;
      audio.starting = false;
      if (audio.eventSource) audio.eventSource.close();
      audio.eventSource = null;
      if (notifyServer) api("/api/audio/configure", { sourceId: $("micSource").value || state.audio.sourceId, active: false }).catch(() => {});
      $("micBtn").textContent = "Start lyd";
      $("micState").textContent = "Fra";
      $("micStatus").textContent = statusText;
      updateMicMeter(0);
    }

    function toggleLocalAudio() {
      if (state.audio.enabled) stopLocalAudio();
      else startLocalAudio();
    }

    async function triggerReactiveScene() {
      try {
        const result = await api("/api/audio/scene", {});
        applyServerUpdates(result.updates || []);
        $("micStatus").textContent = "Ny scene";
      } catch (error) {
        $("micStatus").textContent = error.message;
      }
    }

    function addFixture() {
      const template = templates.find((item) => item.id === $("fixtureType").value) || templates[0];
      const start = clamp($("fixtureStart").value, 1, 512);
      const end = start + template.channels.length - 1;
      if (end > 512) {
        setLive("Fixture går forbi kanal 512", "error");
        return;
      }
      const name = $("fixtureName").value.trim() || template.name;
      const id = crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + "-" + Math.random().toString(16).slice(2);
      state.fixtures.push({ id, type: template.id, start, name });
      $("fixtureStart").value = String(Math.min(512, end + 1));
      $("fixtureName").value = "";
      saveFixtures();
      renderFixtures();
    }

    function renderUniverseGrid() {
      const grid = $("universeGrid");
      grid.innerHTML = "";
      for (let channel = state.pageStart; channel < state.pageStart + 32 && channel <= 512; channel++) {
        const cell = document.createElement("button");
        cell.type = "button";
        cell.className = "channel-cell";
        cell.dataset.channel = String(channel);
        cell.addEventListener("click", () => selectChannel(channel));
        const text = document.createElement("span");
        text.textContent = channel;
        cell.appendChild(text);
        grid.appendChild(cell);
      }
      updateUniverseCells();
    }

    function updateUniverseCells() {
      document.querySelectorAll(".channel-cell").forEach((cell) => {
        const channel = Number.parseInt(cell.dataset.channel, 10);
        const value = state.values[channel - 1] || 0;
        cell.style.setProperty("--level", (value / 255 * 100).toFixed(1) + "%");
        cell.classList.toggle("active", channel === state.selectedChannel);
        const span = cell.querySelector("span");
        if (span) span.textContent = channel + "\n" + value;
      });
    }

    function selectChannel(channel) {
      state.selectedChannel = clamp(channel, 1, 512);
      if (state.selectedChannel < state.pageStart || state.selectedChannel >= state.pageStart + 32) {
        state.pageStart = Math.floor((state.selectedChannel - 1) / 32) * 32 + 1;
        renderUniverseGrid();
      }
      $("channelLabel").textContent = "Kanal " + state.selectedChannel;
      const value = state.values[state.selectedChannel - 1];
      $("channelRange").value = value;
      $("channelValue").value = value;
      updateUniverseCells();
    }

    function nextPage(delta) {
      state.pageStart = clamp(state.pageStart + delta, 1, 481);
      state.pageStart = Math.floor((state.pageStart - 1) / 32) * 32 + 1;
      renderUniverseGrid();
    }

    async function discover() {
      setLive("Discovery kører", "");
      try {
        const status = await api("/api/discover", { timeoutMs: 1800 });
        applyStatus(status);
        setLive((status.nodes || []).length + " noder fundet", status.streaming ? "live" : "");
      } catch (error) {
        setLive(error.message, "error");
      }
    }

    async function loadStatus() {
      try {
        const status = await api("/api/status");
        applyStatus(status);
      } catch (error) {
        setLive(error.message, "error");
      }
    }

    function bindEvents() {
      $("discoverBtn").addEventListener("click", discover);
      $("refreshStatusBtn").addEventListener("click", loadStatus);
      $("addFixtureBtn").addEventListener("click", addFixture);
      $("savePatchBtn").addEventListener("click", () => {
        saveFixtures();
        setLive("Patch gemt", state.streaming ? "live" : "");
      });
      $("clearPatchBtn").addEventListener("click", () => {
        stopLocalAudio("Patch ryddet");
        state.fixtures = [];
        saveFixtures();
        renderFixtures();
      });
      $("streamBtn").addEventListener("click", () => configure({ streaming: !state.streaming }));
      $("sendOnceBtn").addEventListener("click", sendDMX);
      $("blackoutBtn").addEventListener("click", async () => {
        stopLocalAudio("Blackout");
        state.values.fill(0);
        renderFixtures();
        updateUniverseCells();
        try {
          const status = await api("/api/blackout", {});
          applyStatus(status);
          setLive("Blackout sendt", state.streaming ? "live" : "");
        } catch (error) {
          setLive(error.message, "error");
        }
      });
      ["targetIp", "universe", "fps"].forEach((id) => $(id).addEventListener("change", () => configure()));
      $("channelRange").addEventListener("input", () => setChannel(state.selectedChannel, $("channelRange").value));
      $("channelValue").addEventListener("input", () => setChannel(state.selectedChannel, $("channelValue").value));
      document.querySelectorAll("[data-level]").forEach((button) => {
        button.addEventListener("click", () => setChannel(state.selectedChannel, button.dataset.level));
      });
      $("pagePrevBtn").addEventListener("click", () => nextPage(-32));
      $("pageNextBtn").addEventListener("click", () => nextPage(32));
      $("micBtn").addEventListener("click", toggleLocalAudio);
      $("micRandomBtn").addEventListener("click", triggerReactiveScene);
      $("audioRefreshBtn").addEventListener("click", loadHostAudioSources);
      $("micSource").addEventListener("change", () => {
        localStorage.setItem("artnet.audioSource", $("micSource").value);
        state.audio.sourceId = $("micSource").value;
        saveReactiveSettings();
        if (state.audio.enabled) {
          stopLocalAudio("Skifter lydkilde", false);
          startLocalAudio();
        }
      });
      $("micSensitivity").addEventListener("input", () => {
        $("micSensitivityValue").textContent = $("micSensitivity").value + "%";
        saveReactiveSettings();
      });
      $("micIntensity").addEventListener("input", () => {
        $("micIntensityValue").textContent = Math.round(Number($("micIntensity").value) / 255 * 100) + "%";
        saveReactiveSettings();
      });
      ["micFixture", "micMode", "micTempo", "micMovement"].forEach((id) => $(id).addEventListener("change", saveReactiveSettings));
      window.addEventListener("beforeunload", () => {
        if (state.audio.eventSource) state.audio.eventSource.close();
      });
    }

    async function bootstrap() {
      renderTemplateOptions();
      renderFixtures();
      renderUniverseGrid();
      bindEvents();
      await loadRuntimeState();
      await loadStatus();
      await loadHostAudioSources();
    }

    bootstrap();
  </script>
</body>
</html>`
