//go:build windows

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"time"
	"unsafe"

	"github.com/go-ole/go-ole"
	"github.com/moutend/go-wca/pkg/wca"
)

func listHostAudioSources() ([]AudioSource, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := ole.CoInitializeEx(0, ole.COINIT_APARTMENTTHREADED); err != nil {
		return nil, err
	}
	defer ole.CoUninitialize()

	var enumerator *wca.IMMDeviceEnumerator
	if err := wca.CoCreateInstance(wca.CLSID_MMDeviceEnumerator, 0, wca.CLSCTX_ALL, wca.IID_IMMDeviceEnumerator, &enumerator); err != nil {
		return nil, err
	}
	defer enumerator.Release()

	var sources []AudioSource
	inputs, err := enumerateAudioFlow(enumerator, wca.ECapture, "input")
	if err != nil {
		return nil, err
	}
	sources = append(sources, inputs...)
	outputs, err := enumerateAudioFlow(enumerator, wca.ERender, "loopback")
	if err != nil {
		return nil, err
	}
	sources = append(sources, outputs...)
	return sources, nil
}

func enumerateAudioFlow(enumerator *wca.IMMDeviceEnumerator, flow uint32, kind string) ([]AudioSource, error) {
	var collection *wca.IMMDeviceCollection
	if err := enumerator.EnumAudioEndpoints(flow, wca.DEVICE_STATE_ACTIVE, &collection); err != nil {
		return nil, err
	}
	defer collection.Release()
	var count uint32
	if err := collection.GetCount(&count); err != nil {
		return nil, err
	}
	sources := make([]AudioSource, 0, count)
	for index := uint32(0); index < count; index++ {
		var device *wca.IMMDevice
		if err := collection.Item(index, &device); err != nil {
			return nil, err
		}
		id, name, err := audioDeviceIdentity(device)
		device.Release()
		if err != nil {
			return nil, err
		}
		sources = append(sources, AudioSource{ID: id, Name: name, Kind: kind})
	}
	return sources, nil
}

func audioDeviceIdentity(device *wca.IMMDevice) (string, string, error) {
	var id string
	if err := device.GetId(&id); err != nil {
		return "", "", err
	}
	var store *wca.IPropertyStore
	if err := device.OpenPropertyStore(wca.STGM_READ, &store); err != nil {
		return "", "", err
	}
	defer store.Release()
	var value wca.PROPVARIANT
	if err := store.GetValue(&wca.PKEY_Device_FriendlyName, &value); err != nil {
		return "", "", err
	}
	name := value.String()
	if name == "" {
		name = id
	}
	return id, name, nil
}

func monitorHostAudioSource(ctx context.Context, source AudioSource, emit func(float32), ready chan<- error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	readySent := false
	signalReady := func(err error) {
		if readySent {
			return
		}
		ready <- err
		readySent = true
	}

	if err := ole.CoInitializeEx(0, ole.COINIT_APARTMENTTHREADED); err != nil {
		signalReady(err)
		return err
	}
	defer ole.CoUninitialize()
	var enumerator *wca.IMMDeviceEnumerator
	if err := wca.CoCreateInstance(wca.CLSID_MMDeviceEnumerator, 0, wca.CLSCTX_ALL, wca.IID_IMMDeviceEnumerator, &enumerator); err != nil {
		signalReady(err)
		return err
	}
	defer enumerator.Release()

	device, err := findAudioDevice(enumerator, source)
	if err != nil {
		signalReady(err)
		return err
	}
	defer device.Release()

	var audioClient *wca.IAudioClient
	if err := device.Activate(wca.IID_IAudioClient, wca.CLSCTX_ALL, nil, &audioClient); err != nil {
		signalReady(err)
		return err
	}
	defer audioClient.Release()

	var format *wca.WAVEFORMATEX
	if err := audioClient.GetMixFormat(&format); err != nil {
		signalReady(err)
		return err
	}
	if format == nil {
		err := fmt.Errorf("Windows returned no audio format for %q", source.Name)
		signalReady(err)
		return err
	}
	defer ole.CoTaskMemFree(uintptr(unsafe.Pointer(format)))
	if _, err := sampleFormatForWave(format); err != nil {
		signalReady(err)
		return err
	}

	streamFlags := uint32(0)
	if source.Kind == "loopback" {
		streamFlags = wca.AUDCLNT_STREAMFLAGS_LOOPBACK
	}
	const bufferDuration = wca.REFERENCE_TIME(1_000_000) // 100 ms in 100 ns units.
	if err := audioClient.Initialize(wca.AUDCLNT_SHAREMODE_SHARED, streamFlags, bufferDuration, 0, format, nil); err != nil {
		signalReady(err)
		return err
	}

	var captureClient *wca.IAudioCaptureClient
	if err := audioClient.GetService(wca.IID_IAudioCaptureClient, &captureClient); err != nil {
		signalReady(err)
		return err
	}
	defer captureClient.Release()
	if err := audioClient.Start(); err != nil {
		signalReady(err)
		return err
	}
	defer audioClient.Stop()

	signalReady(nil)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			peak, err := readAudioPeak(captureClient, format)
			if err != nil {
				return err
			}
			emit(peak)
		}
	}
}

type waveSampleFormat struct {
	isFloat       bool
	bytesPerValue int
}

func sampleFormatForWave(format *wca.WAVEFORMATEX) (waveSampleFormat, error) {
	if format.NChannels == 0 || format.NBlockAlign == 0 {
		return waveSampleFormat{}, errorsForWaveFormat(format, "invalid channel or block alignment")
	}
	bytesPerValue := int(format.NBlockAlign) / int(format.NChannels)
	if bytesPerValue == 0 || bytesPerValue*int(format.NChannels) != int(format.NBlockAlign) {
		return waveSampleFormat{}, errorsForWaveFormat(format, "invalid sample width")
	}

	const (
		waveFormatPCM        = 0x0001
		waveFormatIEEEFloat  = 0x0003
		waveFormatExtensible = 0xfffe
	)
	tag := format.WFormatTag
	if tag == waveFormatExtensible {
		if format.CbSize < 22 {
			return waveSampleFormat{}, errorsForWaveFormat(format, "invalid extensible format")
		}
		// WAVEFORMATEXTENSIBLE stores the sub-format GUID 24 bytes from the
		// beginning of its packed WAVEFORMATEX header. Data1 is the base tag.
		tag = *(*uint16)(unsafe.Pointer(uintptr(unsafe.Pointer(format)) + 24))
	}

	switch tag {
	case waveFormatIEEEFloat:
		if bytesPerValue != 4 && bytesPerValue != 8 {
			return waveSampleFormat{}, errorsForWaveFormat(format, "unsupported floating-point width")
		}
		return waveSampleFormat{isFloat: true, bytesPerValue: bytesPerValue}, nil
	case waveFormatPCM:
		if bytesPerValue < 1 || bytesPerValue > 4 {
			return waveSampleFormat{}, errorsForWaveFormat(format, "unsupported PCM width")
		}
		return waveSampleFormat{bytesPerValue: bytesPerValue}, nil
	default:
		return waveSampleFormat{}, errorsForWaveFormat(format, fmt.Sprintf("unsupported format tag 0x%04x", tag))
	}
}

func errorsForWaveFormat(format *wca.WAVEFORMATEX, reason string) error {
	return fmt.Errorf("%s (tag 0x%04x, %d-bit, %d channels)", reason, format.WFormatTag, format.WBitsPerSample, format.NChannels)
}

func readAudioPeak(capture *wca.IAudioCaptureClient, format *wca.WAVEFORMATEX) (float32, error) {
	var peak float32
	for {
		var packetFrames uint32
		if err := capture.GetNextPacketSize(&packetFrames); err != nil {
			return 0, err
		}
		if packetFrames == 0 {
			return peak, nil
		}

		var data *byte
		var frames uint32
		var flags uint32
		if err := capture.GetBuffer(&data, &frames, &flags, nil, nil); err != nil {
			return 0, err
		}
		packetPeak, peakErr := peakFromAudioBuffer(data, frames, flags, format)
		releaseErr := capture.ReleaseBuffer(frames)
		if peakErr != nil {
			return 0, peakErr
		}
		if releaseErr != nil {
			return 0, releaseErr
		}
		if packetPeak > peak {
			peak = packetPeak
		}
	}
}

func peakFromAudioBuffer(data *byte, frames uint32, flags uint32, format *wca.WAVEFORMATEX) (float32, error) {
	if flags&wca.AUDCLNT_BUFFERFLAGS_SILENT != 0 || data == nil || frames == 0 {
		return 0, nil
	}
	sampleFormat, err := sampleFormatForWave(format)
	if err != nil {
		return 0, err
	}
	byteCount64 := uint64(frames) * uint64(format.NBlockAlign)
	if byteCount64 > uint64(^uint(0)>>1) {
		return 0, errorsForWaveFormat(format, "audio packet is too large")
	}
	buffer := unsafe.Slice(data, int(byteCount64))
	var peak float64
	for offset := 0; offset+sampleFormat.bytesPerValue <= len(buffer); offset += sampleFormat.bytesPerValue {
		value := decodeAudioSample(buffer[offset:offset+sampleFormat.bytesPerValue], sampleFormat)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		value = math.Abs(value)
		if value > peak {
			peak = value
		}
	}
	if peak > 1 {
		peak = 1
	}
	return float32(peak), nil
}

func decodeAudioSample(sample []byte, format waveSampleFormat) float64 {
	if format.isFloat {
		if format.bytesPerValue == 4 {
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(sample)))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(sample))
	}

	switch format.bytesPerValue {
	case 1:
		return float64(int(sample[0])-128) / 128
	case 2:
		return float64(int16(binary.LittleEndian.Uint16(sample))) / 32768
	case 3:
		value := int32(sample[0]) | int32(sample[1])<<8 | int32(sample[2])<<16
		if value&0x00800000 != 0 {
			value |= ^int32(0x00ffffff)
		}
		return float64(value) / 8388608
	case 4:
		return float64(int32(binary.LittleEndian.Uint32(sample))) / 2147483648
	default:
		return 0
	}
}

func findAudioDevice(enumerator *wca.IMMDeviceEnumerator, source AudioSource) (*wca.IMMDevice, error) {
	flow := uint32(wca.ECapture)
	if source.Kind == "loopback" {
		flow = wca.ERender
	}
	var collection *wca.IMMDeviceCollection
	if err := enumerator.EnumAudioEndpoints(flow, wca.DEVICE_STATE_ACTIVE, &collection); err != nil {
		return nil, err
	}
	defer collection.Release()
	var count uint32
	if err := collection.GetCount(&count); err != nil {
		return nil, err
	}
	for index := uint32(0); index < count; index++ {
		var device *wca.IMMDevice
		if err := collection.Item(index, &device); err != nil {
			return nil, err
		}
		var id string
		if err := device.GetId(&id); err != nil {
			device.Release()
			return nil, err
		}
		if id == source.ID {
			return device, nil
		}
		device.Release()
	}
	return nil, fmt.Errorf("Windows audio source %q is no longer available", source.Name)
}
