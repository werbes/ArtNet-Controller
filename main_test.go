package main

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunApplicationStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runApplicationWithReady(ctx, appConfig{httpAddr: "127.0.0.1:0"}, ready)
	}()

	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("application did not become ready")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runApplicationWithReady() = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("application did not stop after cancellation")
	}
}

func TestBuildArtPoll(t *testing.T) {
	packet := buildArtPoll()
	if len(packet) != 14 {
		t.Fatalf("length = %d, want 14", len(packet))
	}
	if !isArtNetPacket(packet) {
		t.Fatal("packet does not have Art-Net header")
	}
	if got := binary.LittleEndian.Uint16(packet[8:10]); got != opPoll {
		t.Fatalf("opcode = %#x, want %#x", got, opPoll)
	}
	if packet[10] != protocolHi || packet[11] != protocolLo {
		t.Fatalf("protocol = %d.%d, want %d.%d", packet[10], packet[11], protocolHi, protocolLo)
	}
}

func TestBuildArtDmx(t *testing.T) {
	data := make([]byte, maxDMXChannels)
	data[0] = 17
	data[511] = 233

	packet := buildArtDmx(0x1234, 7, data)
	if len(packet) != 18+maxDMXChannels {
		t.Fatalf("length = %d, want %d", len(packet), 18+maxDMXChannels)
	}
	if !isArtNetPacket(packet) {
		t.Fatal("packet does not have Art-Net header")
	}
	if got := binary.LittleEndian.Uint16(packet[8:10]); got != opDmx {
		t.Fatalf("opcode = %#x, want %#x", got, opDmx)
	}
	if packet[12] != 7 {
		t.Fatalf("sequence = %d, want 7", packet[12])
	}
	if packet[14] != 0x34 || packet[15] != 0x12 {
		t.Fatalf("universe bytes = %#x %#x, want 0x34 0x12", packet[14], packet[15])
	}
	if packet[16] != 0x02 || packet[17] != 0x00 {
		t.Fatalf("length bytes = %#x %#x, want 0x02 0x00", packet[16], packet[17])
	}
	if packet[18] != 17 || packet[len(packet)-1] != 233 {
		t.Fatal("DMX payload was not copied")
	}
}

func TestParseArtPollReply(t *testing.T) {
	packet := make([]byte, 239)
	copy(packet[0:8], artNetID)
	binary.LittleEndian.PutUint16(packet[8:10], opPollReply)
	copy(packet[10:14], []byte{10, 0, 0, 42})
	packet[16] = 1
	packet[17] = 4
	packet[18] = 0
	packet[19] = 0
	copyFixed(packet[26:44], "Short")
	copyFixed(packet[44:108], "Long Node")
	copyFixed(packet[108:172], "#0001 [0001] OK")
	binary.BigEndian.PutUint16(packet[172:174], 2)
	packet[174] = 0x80
	packet[175] = 0x80
	packet[190] = 0
	packet[191] = 1
	packet[200] = 0x00
	copy(packet[201:207], []byte{1, 2, 3, 4, 5, 6})
	packet[211] = 1
	packet[212] = 1

	node, ok := parseArtPollReply(packet, &net.UDPAddr{IP: net.IPv4(10, 0, 0, 99), Port: artNetPort})
	if !ok {
		t.Fatal("parseArtPollReply returned false")
	}
	if node.IP != "10.0.0.42" {
		t.Fatalf("IP = %q, want 10.0.0.42", node.IP)
	}
	if node.ShortName != "Short" || node.LongName != "Long Node" {
		t.Fatalf("names = %q / %q", node.ShortName, node.LongName)
	}
	if len(node.OutputPorts) != 2 {
		t.Fatalf("outputs = %d, want 2", len(node.OutputPorts))
	}
	if node.OutputPorts[0].Universe != 0 || node.OutputPorts[1].Universe != 1 {
		t.Fatalf("universes = %+v, want 0 and 1", node.OutputPorts)
	}
	if node.MAC != "01:02:03:04:05:06" {
		t.Fatalf("MAC = %q", node.MAC)
	}
	if !node.WebConfig {
		t.Fatal("WebConfig = false, want true")
	}
}

func TestDiscoveryAddressesIncludeManualTarget(t *testing.T) {
	target := net.IPv4(192, 168, 40, 23)
	addresses := discoveryAddresses(target.String())
	for _, address := range addresses {
		if address.Equal(target) {
			return
		}
	}
	t.Fatalf("discovery addresses do not include manual target %s", target)
}

func TestDeliveryTargetPrefersReplySource(t *testing.T) {
	nodes := []ArtNetNode{{
		IP:       "2.0.0.15",
		RemoteIP: "192.168.40.23",
	}}

	target, connected := deliveryTarget("2.0.0.15", nodes)
	if !connected {
		t.Fatal("delivery target was not verified")
	}
	if target != "192.168.40.23" {
		t.Fatalf("delivery target = %q, want reply source 192.168.40.23", target)
	}
}

func TestSendCurrentFrameOverUDP(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: artNetPort})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	controller := &Controller{conn: sender, targetIP: "127.0.0.2", universe: 7}
	controller.dmx[0] = 191
	controller.sendCurrentFrame(true)

	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 1024)
	n, _, err := receiver.ReadFromUDP(packet)
	if err != nil {
		t.Fatal(err)
	}
	packet = packet[:n]
	if got := binary.LittleEndian.Uint16(packet[8:10]); got != opDmx {
		t.Fatalf("opcode = %#x, want %#x", got, opDmx)
	}
	if packet[14] != 7 || packet[15] != 0 {
		t.Fatalf("universe bytes = %#x %#x, want 0x07 0x00", packet[14], packet[15])
	}
	if packet[18] != 191 {
		t.Fatalf("channel 1 = %d, want 191", packet[18])
	}
	if status := controller.status(); status.LastSendErr != "" || status.LastSent == "" {
		t.Fatalf("send status = %+v, want successful send", status)
	}
}

func TestStatusReportsUnverifiedTarget(t *testing.T) {
	controller := &Controller{targetIP: "10.0.0.42", universe: 3, fps: defaultFPS}
	status := controller.status()
	if status.Connected {
		t.Fatal("Connected = true without an ArtPoll reply")
	}
	if status.SendIP != "10.0.0.42" {
		t.Fatalf("SendIP = %q, want configured target", status.SendIP)
	}
	if !strings.Contains(status.ConnectionNote, "unverified") {
		t.Fatalf("ConnectionNote = %q, want an unverified warning", status.ConnectionNote)
	}
}

func TestIndexIncludesBARbara24ProfilesAndPortSelection(t *testing.T) {
	profiles := []string{
		"fun-generation-barbara-24-2ch",
		"fun-generation-barbara-24-3ch",
		"fun-generation-barbara-24-5ch",
		"fun-generation-barbara-24-24ch",
	}
	for _, profile := range profiles {
		if !strings.Contains(indexHTML, profile) {
			t.Errorf("indexHTML does not include fixture profile %q", profile)
		}
	}
	profileNames := []string{
		"Fun Generation LED BARbara 24 / 2ch",
		"Fun Generation LED BARbara 24 / 3ch RGB",
		"Fun Generation LED BARbara 24 / 5ch RGB + FX",
		"Fun Generation LED BARbara 24 / 24ch segments",
	}
	for _, name := range profileNames {
		if !strings.Contains(indexHTML, name) {
			t.Errorf("indexHTML does not include fixture profile name %q", name)
		}
	}
	for _, feature := range []string{
		"barbaraShowPresets",
		"barbaraColorGroups",
		"selectNodePort",
		"portAddressTitle",
		"connectionNote",
		"startLocalAudio",
		"processHostAudioLevel",
		"applyReactiveFixture",
		"writeReactiveMovement",
		"writeReactiveAxis",
		"micSensitivity",
		"micIntensity",
		"micMovement",
		"micSource",
		"/api/runtime",
		"saveReactiveSettings",
		"applyServerUpdates",
	} {
		if !strings.Contains(indexHTML, feature) {
			t.Errorf("indexHTML does not include %q", feature)
		}
	}
	if strings.Contains(indexHTML, "getUserMedia") {
		t.Error("indexHTML still requests browser microphone capture")
	}
	if strings.Contains(indexHTML, "navigator.sendBeacon") {
		t.Error("indexHTML still stops persisted audio when the browser closes")
	}
}

func TestServeIndexDisablesCaching(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	serveIndex(response, request)
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
}

func copyFixed(dst []byte, value string) {
	copy(dst, []byte(value))
}
