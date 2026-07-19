//go:build windows

package main

import (
	"encoding/binary"
	"math"
	"testing"
	"unsafe"

	"github.com/moutend/go-wca/pkg/wca"
)

func TestPeakFromPCM16AudioBuffer(t *testing.T) {
	format := &wca.WAVEFORMATEX{
		WFormatTag:     wca.WAVE_FORMAT_PCM,
		NChannels:      1,
		NBlockAlign:    2,
		WBitsPerSample: 16,
	}
	buffer := make([]byte, 6)
	binary.LittleEndian.PutUint16(buffer[0:], 0xc000)
	binary.LittleEndian.PutUint16(buffer[2:], 0x2000)
	binary.LittleEndian.PutUint16(buffer[4:], 0x1000)

	peak, err := peakFromAudioBuffer(&buffer[0], 3, 0, format)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(peak)-0.5) > 0.0001 {
		t.Fatalf("peak = %v, want 0.5", peak)
	}
}

func TestPeakFromFloatAudioBuffer(t *testing.T) {
	format := &wca.WAVEFORMATEX{
		WFormatTag:     0x0003,
		NChannels:      2,
		NBlockAlign:    8,
		WBitsPerSample: 32,
	}
	values := []float32{0.1, -0.25, 0.8, -0.4}
	buffer := make([]byte, len(values)*4)
	for index, value := range values {
		binary.LittleEndian.PutUint32(buffer[index*4:], math.Float32bits(value))
	}

	peak, err := peakFromAudioBuffer(&buffer[0], 2, 0, format)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(peak)-0.8) > 0.0001 {
		t.Fatalf("peak = %v, want 0.8", peak)
	}
}

func TestExtensibleFloatWaveFormat(t *testing.T) {
	buffer := make([]byte, 40)
	binary.LittleEndian.PutUint16(buffer[0:], 0xfffe)
	binary.LittleEndian.PutUint16(buffer[2:], 2)
	binary.LittleEndian.PutUint16(buffer[12:], 8)
	binary.LittleEndian.PutUint16(buffer[14:], 32)
	binary.LittleEndian.PutUint16(buffer[16:], 22)
	binary.LittleEndian.PutUint16(buffer[18:], 32)
	binary.LittleEndian.PutUint32(buffer[24:], 0x0003)
	format := (*wca.WAVEFORMATEX)(unsafe.Pointer(&buffer[0]))

	description, err := sampleFormatForWave(format)
	if err != nil {
		t.Fatal(err)
	}
	if !description.isFloat || description.bytesPerValue != 4 {
		t.Fatalf("format = %+v, want 32-bit float", description)
	}
}

func TestSilentAudioBufferHasZeroPeak(t *testing.T) {
	format := &wca.WAVEFORMATEX{
		WFormatTag:     wca.WAVE_FORMAT_PCM,
		NChannels:      1,
		NBlockAlign:    2,
		WBitsPerSample: 16,
	}
	peak, err := peakFromAudioBuffer(nil, 128, wca.AUDCLNT_BUFFERFLAGS_SILENT, format)
	if err != nil {
		t.Fatal(err)
	}
	if peak != 0 {
		t.Fatalf("peak = %v, want 0", peak)
	}
}
