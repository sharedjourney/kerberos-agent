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

func TestAssembleHeartbeatEvents_CacheEmptyNoFallback_ReturnsEmptyArray(t *testing.T) {
	resetCache(t)

	got := AssembleHeartbeatEvents(nil)
	assert.Equal(t, "[]", string(got))
}

// When the live cache has not yet observed any tokens (e.g. a motion-
// only camera that never emits digital I/O), the heartbeat falls back
// to the token list gathered out-of-band by the metadata poller. This
// list must be passed in — AssembleHeartbeatEvents must NOT do its own
// blocking device enumeration on the send path.
func TestAssembleHeartbeatEvents_CacheEmpty_UsesFallback(t *testing.T) {
	resetCache(t)
	fallback := []ONVIFEvents{
		{Key: "DI1", Type: "input", Value: "false"},
		{Key: "DO1", Type: "output", Value: "false"},
	}

	got := AssembleHeartbeatEvents(fallback)

	var events []ONVIFEvents
	if assert.NoError(t, json.Unmarshal(got, &events)) {
		assert.Equal(t, fallback, events)
	}
}

// Live cache state always wins over the fallback token list.
func TestAssembleHeartbeatEvents_LiveCachePreferredOverFallback(t *testing.T) {
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"InputToken": "DI1"},
	})
	fallback := []ONVIFEvents{{Key: "STALE", Type: "input", Value: "false"}}

	got := AssembleHeartbeatEvents(fallback)

	var events []ONVIFEvents
	if assert.NoError(t, json.Unmarshal(got, &events)) {
		if assert.Len(t, events, 1) {
			assert.Equal(t, "DI1-input", events[0].Key, "live cache must win over fallback")
		}
	}
}

// --- MergeCacheTokensForHTTP ----------------------------------------
//
// The HTTP I/O endpoints in routers/http/methods.go used to surface
// live state from the heartbeat's pull-point map merged with raw
// device-API tokens. The agent and the credential-supplied camera are
// usually the same camera (single-camera-per-agent deployment), so
// preserving the live-state path is what callers depend on.

func TestMergeCacheTokensForHTTP_EmptyCacheNoTokens(t *testing.T) {
	resetCache(t)
	assert.Empty(t, MergeCacheTokensForHTTP("input", nil))
}

func TestMergeCacheTokensForHTTP_EmptyCacheTokensOnly(t *testing.T) {
	resetCache(t)
	got := MergeCacheTokensForHTTP("input", []string{"DI1", "DI2"})
	assert.Equal(t, []ONVIFEvents{
		{Key: "DI1", Type: "input"},
		{Key: "DI2", Type: "input"},
	}, got)
}

func TestMergeCacheTokensForHTTP_CachedEntriesSurfaceLiveValue(t *testing.T) {
	// The whole point of the merge: cached entries carry Value/
	// Timestamp from the event stream, and the HTTP endpoint must
	// surface that — not just the bare token list.
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"InputToken": "DI1"},
	})
	got := MergeCacheTokensForHTTP("input", nil)
	if assert.Len(t, got, 1) {
		assert.Equal(t, "DI1-input", got[0].Key)
		assert.Equal(t, "true", got[0].Value)
	}
}

func TestMergeCacheTokensForHTTP_OtherTypeFilteredOut(t *testing.T) {
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalOutput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"OutputToken": "DO1"},
	})
	got := MergeCacheTokensForHTTP("input", []string{"DI1"})
	assert.Equal(t, []ONVIFEvents{{Key: "DI1", Type: "input"}}, got, "outputs in cache must not leak into the inputs response")
}

func TestMergeCacheTokensForHTTP_CachedAndDeviceTokensCoexist(t *testing.T) {
	// Cache keys are suffixed ("<token>-input"); device tokens are
	// bare. The comparison is by literal Key, matching the
	// pre-refactor master behaviour where both entries can appear
	// for the same physical input. The 1:1 case (agent and
	// credential-supplied camera same) gets live state via the
	// cached entry; the bare token entry is harmless context for
	// the UI's token list.
	resetCache(t)
	SharedEventCache().Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		Operation: stream.PropertyInitialized,
		State:     stream.StateActive,
		Source:    map[string]string{"InputToken": "DI1"},
	})
	got := MergeCacheTokensForHTTP("input", []string{"DI1", "DI2"})
	// DI1-input from cache (active), DI1 + DI2 from device tokens.
	if assert.Len(t, got, 3) {
		assert.Equal(t, "DI1-input", got[0].Key)
		assert.Equal(t, "true", got[0].Value)
		assert.Equal(t, "DI1", got[1].Key)
		assert.Empty(t, got[1].Value)
		assert.Equal(t, "DI2", got[2].Key)
	}
}

func TestMergeCacheTokensForHTTP_TokenAlreadyAsBareKeySuppressesDup(t *testing.T) {
	// If a token already appears in the cache with its bare key
	// (unlikely in practice, but defensible), the device-API merge
	// should not re-add it.
	resetCache(t)
	SharedEventCache().Reset()
	// Inject directly so we get a bare-key cached entry.
	c := SharedEventCache()
	c.items["DI1"] = ONVIFEvents{Key: "DI1", Type: "input"}

	got := MergeCacheTokensForHTTP("input", []string{"DI1"})
	assert.Equal(t, []ONVIFEvents{{Key: "DI1", Type: "input"}}, got)
}
