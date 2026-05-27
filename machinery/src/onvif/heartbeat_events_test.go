package onvif

import (
	"encoding/json"
	"testing"

	"github.com/kerberos-io/onvif/event/stream"
	"github.com/stretchr/testify/assert"
)

// resetCache isolates each test from the package-global singleton:
// clear on entry and on cleanup, so order does not matter.
func resetCache(t *testing.T) {
	t.Helper()
	SharedEventCache().Reset()
	t.Cleanup(func() { SharedEventCache().Reset() })
}

func TestAssembleHeartbeatEvents_CachePopulated_UsesCache(t *testing.T) {
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"InputToken": "DI1"},
	})

	got := AssembleHeartbeatEvents(nil)

	var events []ONVIFEvents
	if assert.NoError(t, json.Unmarshal(got, &events)) {
		if assert.Len(t, events, 1) {
			assert.Equal(t, "DI1-input", events[0].Key)
			assert.Equal(t, "true", events[0].Value)
		}
	}
}

func TestAssembleHeartbeatEvents_CacheEmptyDeviceNil_ReturnsEmptyArray(t *testing.T) {
	resetCache(t)

	got := AssembleHeartbeatEvents(nil)
	assert.Equal(t, "[]", string(got))
}

func TestAssembleHeartbeatEvents_CachePopulated_IgnoresDevice(t *testing.T) {
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"InputToken": "DI1"},
	})
	assert.NotPanics(t, func() { AssembleHeartbeatEvents(nil) })
}
