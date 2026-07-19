package main

import (
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

type reactiveTemplate struct {
	channels    []string
	colorGroups []reactiveColorGroup
}

type reactiveColorGroup struct {
	red   int
	green int
	blue  int
	white int
}

type reactivePosition struct {
	pan  float64
	tilt float64
}

type ReactiveResult struct {
	Beat    bool            `json:"beat"`
	Updates []ChannelUpdate `json:"updates,omitempty"`
}

type ReactiveEngine struct {
	mu              sync.Mutex
	controller      *Controller
	stateStore      *StateStore
	random          *rand.Rand
	levelAverage    float64
	lastBeat        time.Time
	lastLevelUpdate time.Time
	chaseIndex      int
	colorIndex      int
	positions       map[string]reactivePosition
}

var reactiveColors = [][3]int{
	{255, 28, 18},
	{255, 116, 0},
	{245, 220, 24},
	{24, 225, 92},
	{0, 210, 255},
	{38, 92, 255},
	{196, 42, 255},
	{255, 35, 146},
	{255, 255, 255},
}

func NewReactiveEngine(controller *Controller, stateStore *StateStore) *ReactiveEngine {
	return &ReactiveEngine{
		controller:   controller,
		stateStore:   stateStore,
		random:       rand.New(rand.NewSource(time.Now().UnixNano())),
		levelAverage: 0.02,
		positions:    make(map[string]reactivePosition),
	}
}

func fixtureTemplate(fixtureType string) (reactiveTemplate, bool) {
	var channels []string
	switch fixtureType {
	case "fun-generation-barbara-24-2ch":
		channels = []string{"Show / Color", "Speed / Sound sensitivity"}
	case "fun-generation-barbara-24-3ch", "rgb":
		channels = []string{"Red", "Green", "Blue"}
	case "fun-generation-barbara-24-5ch":
		channels = []string{"Red", "Green", "Blue", "Dimmer", "Strobe"}
	case "fun-generation-barbara-24-24ch":
		channels = make([]string, 0, 24)
		for range 8 {
			channels = append(channels, "Red", "Green", "Blue")
		}
	case "dimmer":
		channels = []string{"Dimmer"}
	case "rgbw":
		channels = []string{"Red", "Green", "Blue", "White"}
	case "par7":
		channels = []string{"Dimmer", "Red", "Green", "Blue", "White", "Strobe", "Macro"}
	case "mh8":
		channels = []string{"Pan", "Tilt", "Pan Fine", "Tilt Fine", "Speed", "Dimmer", "Strobe", "Color"}
	case "mh16":
		channels = []string{"Pan", "Pan Fine", "Tilt", "Tilt Fine", "Speed", "Dimmer", "Strobe", "Red", "Green", "Blue", "White", "Color", "Gobo", "Focus", "Prism", "Reset"}
	default:
		return reactiveTemplate{}, false
	}

	template := reactiveTemplate{channels: channels}
	if fixtureType == "fun-generation-barbara-24-24ch" {
		for index := range 8 {
			offset := index * 3
			template.colorGroups = append(template.colorGroups, reactiveColorGroup{red: offset, green: offset + 1, blue: offset + 2, white: -1})
		}
		return template, true
	}
	group := reactiveColorGroup{
		red:   channelOffset(channels, "Red"),
		green: channelOffset(channels, "Green"),
		blue:  channelOffset(channels, "Blue"),
		white: channelOffset(channels, "White"),
	}
	if group.red >= 0 || group.green >= 0 || group.blue >= 0 || group.white >= 0 {
		template.colorGroups = []reactiveColorGroup{group}
	}
	return template, true
}

func fixtureChannelCount(fixtureType string) int {
	template, ok := fixtureTemplate(fixtureType)
	if !ok {
		return 0
	}
	return len(template.channels)
}

func channelOffset(channels []string, name string) int {
	for index, channel := range channels {
		if channel == name {
			return index
		}
	}
	return -1
}

func (e *ReactiveEngine) Process(rawLevel float32) ReactiveResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.stateStore.Snapshot()
	if !state.Audio.Active {
		return ReactiveResult{}
	}
	level := math.Max(0, math.Min(1, float64(rawLevel)))
	meterLevel := math.Min(1, math.Sqrt(level))
	now := time.Now()
	e.levelAverage = e.levelAverage*0.92 + level*0.08
	sensitivity := float64(state.Audio.Sensitivity) / 100
	thresholdFactor := 1.72 - sensitivity*0.52
	minimumLevel := 0.006 + (1-sensitivity)*0.07
	cooldown := time.Duration(state.Audio.TempoMS) * time.Millisecond
	cooldownElapsed := now.Sub(e.lastBeat) >= cooldown
	peakDetected := level > minimumLevel && level > e.levelAverage*thresholdFactor
	beat := cooldownElapsed && (state.Audio.Mode == "random" || peakDetected)
	updates := make(map[int]int)

	if beat {
		e.lastBeat = now
		if state.Audio.Mode == "pulse" {
			e.chooseColor()
			e.applyScene(state, "pulse", math.Max(0.2, math.Min(1, meterLevel*1.7)), false, true, updates)
		} else {
			e.applyScene(state, state.Audio.Mode, 1, true, true, updates)
		}
	}
	if state.Audio.Mode == "pulse" && now.Sub(e.lastLevelUpdate) >= 75*time.Millisecond {
		e.lastLevelUpdate = now
		pulseLevel := math.Max(0.04, math.Min(1, meterLevel*1.7))
		e.applyScene(state, "pulse", pulseLevel, false, false, updates)
	}
	return ReactiveResult{Beat: beat, Updates: e.controller.applyDMXUpdates(updates)}
}

func (e *ReactiveEngine) Trigger() ReactiveResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.stateStore.Snapshot()
	updates := make(map[int]int)
	mode := state.Audio.Mode
	if mode == "pulse" {
		e.chooseColor()
	}
	e.applyScene(state, mode, 1, mode != "pulse", true, updates)
	return ReactiveResult{Updates: e.controller.applyDMXUpdates(updates)}
}

func (e *ReactiveEngine) handleScene(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, e.Trigger())
}

func (e *ReactiveEngine) applyScene(state PersistentState, requestedMode string, level float64, chooseColor, moveHeads bool, updates map[int]int) {
	fixtures := selectedFixtures(state.Fixtures, state.Audio.FixtureID)
	if len(fixtures) == 0 {
		return
	}
	baseColor := reactiveColors[e.colorIndex%len(reactiveColors)]
	if chooseColor && requestedMode != "random" {
		baseColor = e.chooseColor()
	}
	color := scaledColor(baseColor, level, state.Audio.Intensity)
	for _, fixture := range fixtures {
		e.applyFixture(fixture, state.Audio, requestedMode, color, level, moveHeads, updates)
	}
	if requestedMode == "chase" {
		e.chaseIndex++
	}
}

func selectedFixtures(fixtures []Fixture, selectedID string) []Fixture {
	if selectedID == "" || selectedID == "all" {
		return fixtures
	}
	for _, fixture := range fixtures {
		if fixture.ID == selectedID {
			return []Fixture{fixture}
		}
	}
	return nil
}

func (e *ReactiveEngine) chooseColor() [3]int {
	next := e.random.Intn(len(reactiveColors))
	if next == e.colorIndex {
		next = (next + 1) % len(reactiveColors)
	}
	e.colorIndex = next
	return reactiveColors[next]
}

func scaledColor(color [3]int, level float64, intensity int) [3]int {
	scale := math.Max(0, math.Min(1, level)) * float64(intensity) / 255
	return [3]int{
		int(math.Round(float64(color[0]) * scale)),
		int(math.Round(float64(color[1]) * scale)),
		int(math.Round(float64(color[2]) * scale)),
	}
}

func (e *ReactiveEngine) applyFixture(fixture Fixture, options PersistentAudio, mode string, color [3]int, level float64, moveHead bool, updates map[int]int) {
	template, ok := fixtureTemplate(fixture.Type)
	if !ok {
		return
	}
	intensity := int(math.Round(float64(options.Intensity) * math.Max(0, math.Min(1, level))))
	if moveHead {
		e.applyMovement(fixture, template, options, updates)
	}
	if fixture.Type == "fun-generation-barbara-24-2ch" {
		presets := []int{8, 16, 24, 32, 40, 48, 56}
		if mode == "random" {
			e.chooseColor()
		}
		setReactiveChannel(updates, fixture.Start, presets[e.colorIndex%len(presets)])
		setReactiveChannel(updates, fixture.Start+1, intensity)
		return
	}

	groups := template.colorGroups
	if mode == "random" {
		for _, group := range groups {
			groupColor := scaledColor(e.chooseColor(), level, options.Intensity)
			writeReactiveGroup(updates, fixture, group, groupColor)
		}
	} else if len(groups) > 1 && mode == "chase" {
		active := e.chaseIndex % len(groups)
		for index, group := range groups {
			groupColor := color
			if index != active {
				groupColor = [3]int{}
			}
			writeReactiveGroup(updates, fixture, group, groupColor)
		}
	} else if len(groups) > 1 && mode == "segments" {
		for index, group := range groups {
			groupColor := [3]int{}
			if index == 0 || e.random.Float64() > 0.24 {
				groupColor = scaledColor(e.chooseColor(), level, options.Intensity)
			}
			writeReactiveGroup(updates, fixture, group, groupColor)
		}
	} else {
		for _, group := range groups {
			writeReactiveGroup(updates, fixture, group, color)
		}
	}

	colorOffset := channelOffset(template.channels, "Color")
	if len(groups) == 0 && colorOffset >= 0 {
		wheel := []int{8, 24, 40, 56, 72, 88, 104, 120, 136}
		if mode == "random" {
			e.chooseColor()
		}
		setReactiveChannel(updates, fixture.Start+colorOffset, wheel[e.colorIndex%len(wheel)])
	}
	if dimmerOffset := channelOffset(template.channels, "Dimmer"); dimmerOffset >= 0 {
		setReactiveChannel(updates, fixture.Start+dimmerOffset, intensity)
	}
	if strobeOffset := channelOffset(template.channels, "Strobe"); strobeOffset >= 0 {
		setReactiveChannel(updates, fixture.Start+strobeOffset, 0)
	}
}

func writeReactiveGroup(updates map[int]int, fixture Fixture, group reactiveColorGroup, color [3]int) {
	if group.red >= 0 {
		setReactiveChannel(updates, fixture.Start+group.red, color[0])
	}
	if group.green >= 0 {
		setReactiveChannel(updates, fixture.Start+group.green, color[1])
	}
	if group.blue >= 0 {
		setReactiveChannel(updates, fixture.Start+group.blue, color[2])
	}
	if group.white >= 0 {
		setReactiveChannel(updates, fixture.Start+group.white, min(color[0], min(color[1], color[2])))
	}
}

func (e *ReactiveEngine) applyMovement(fixture Fixture, template reactiveTemplate, options PersistentAudio, updates map[int]int) {
	panOffset := channelOffset(template.channels, "Pan")
	tiltOffset := channelOffset(template.channels, "Tilt")
	if options.Movement == "off" || panOffset < 0 || tiltOffset < 0 {
		return
	}
	panMin, panMax, tiltMin, tiltMax := 0.1, 0.9, 0.16, 0.82
	if options.Movement == "compact" {
		panMin, panMax, tiltMin, tiltMax = 0.34, 0.66, 0.32, 0.64
	}
	key := fixture.ID
	previous, ok := e.positions[key]
	if !ok {
		previous = reactivePosition{pan: 0.5, tilt: 0.5}
	}
	next := previous
	for range 6 {
		candidate := reactivePosition{
			pan:  panMin + e.random.Float64()*(panMax-panMin),
			tilt: tiltMin + e.random.Float64()*(tiltMax-tiltMin),
		}
		next = candidate
		if math.Abs(candidate.pan-previous.pan)+math.Abs(candidate.tilt-previous.tilt) >= 0.2 {
			break
		}
	}
	e.positions[key] = next
	writeReactiveAxis(updates, fixture.Start, template.channels, "Pan", "Pan Fine", next.pan)
	writeReactiveAxis(updates, fixture.Start, template.channels, "Tilt", "Tilt Fine", next.tilt)
	if speedOffset := channelOffset(template.channels, "Speed"); speedOffset >= 0 {
		setReactiveChannel(updates, fixture.Start+speedOffset, clampInt(int(math.Round(float64(options.TempoMS)*0.45)), 48, 180))
	}
}

func writeReactiveAxis(updates map[int]int, start int, channels []string, coarseName, fineName string, normalized float64) {
	coarseOffset := channelOffset(channels, coarseName)
	if coarseOffset < 0 {
		return
	}
	fineOffset := channelOffset(channels, fineName)
	if fineOffset < 0 {
		setReactiveChannel(updates, start+coarseOffset, int(math.Round(normalized*255)))
		return
	}
	value := int(math.Round(normalized * 65535))
	setReactiveChannel(updates, start+coarseOffset, value>>8)
	setReactiveChannel(updates, start+fineOffset, value&0xff)
}

func setReactiveChannel(updates map[int]int, channel, value int) {
	if channel < 1 || channel > maxDMXChannels {
		return
	}
	updates[channel] = clampInt(value, 0, 255)
}

func clampInt(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func (c *Controller) applyDMXUpdates(values map[int]int) []ChannelUpdate {
	if len(values) == 0 {
		return nil
	}
	channels := make([]int, 0, len(values))
	for channel := range values {
		channels = append(channels, channel)
	}
	sort.Ints(channels)
	updates := make([]ChannelUpdate, 0, len(channels))
	c.mu.Lock()
	for _, channel := range channels {
		value := clampInt(values[channel], 0, 255)
		if c.dmx[channel-1] == byte(value) {
			continue
		}
		c.dmx[channel-1] = byte(value)
		updates = append(updates, ChannelUpdate{Channel: channel, Value: value})
	}
	c.mu.Unlock()
	if len(updates) == 0 {
		return nil
	}
	if err := c.persist(false); err != nil {
		log.Printf("persist reactive DMX state: %v", err)
	}
	c.sendCurrentFrame(true)
	return updates
}
