package onvif

import (
	"testing"

	"github.com/kerberos-io/agent/machinery/src/models"
	"github.com/stretchr/testify/assert"
)

func TestMetadataCache_Default_WhenNothingStored(t *testing.T) {
	md := NewMetadataCache().Snapshot()
	assert.Equal(t, "false", md.Enabled)
	assert.Equal(t, "false", md.Zoom)
	assert.Equal(t, "false", md.PanTilt)
	assert.Equal(t, "false", md.Presets)
	assert.Equal(t, "[]", string(md.PresetsList), "presets list must be a valid empty JSON array, never nil")
}

// A nil cache (e.g. read before construction) must return usable
// defaults rather than panic.
func TestMetadataCache_NilSnapshot_ReturnsDefault(t *testing.T) {
	var c *MetadataCache
	md := c.Snapshot()
	assert.Equal(t, "false", md.Enabled)
	assert.Equal(t, "[]", string(md.PresetsList))
}

// Store/Reset must share Snapshot's nil-safe contract so all three
// methods are symmetric on a nil receiver.
func TestMetadataCache_NilStoreAndReset_DoNotPanic(t *testing.T) {
	var c *MetadataCache
	assert.NotPanics(t, func() {
		c.Store(DeviceMetadata{Enabled: "true"})
		c.Reset()
	})
}

func TestMetadataCache_StoreSnapshot_RoundTrips(t *testing.T) {
	c := NewMetadataCache()
	stored := DeviceMetadata{
		Enabled:     "true",
		Zoom:        "true",
		PanTilt:     "false",
		Presets:     "true",
		PresetsList: []byte(`[{"Token":"1"}]`),
		IOFallback:  []ONVIFEvents{{Key: "DI1", Type: "input"}},
	}
	c.Store(stored)

	got := c.Snapshot()
	assert.Equal(t, "true", got.Enabled)
	assert.Equal(t, "true", got.Zoom)
	assert.Equal(t, "false", got.PanTilt)
	assert.Equal(t, "true", got.Presets)
	assert.Equal(t, `[{"Token":"1"}]`, string(got.PresetsList))
	assert.Equal(t, []ONVIFEvents{{Key: "DI1", Type: "input"}}, got.IOFallback)
}

// Snapshot must hand back defensive copies of its reference-typed
// fields: a caller mutating a snapshot must not corrupt the cached value
// (and must not race the poller that owns the backing arrays).
func TestMetadataCache_Snapshot_DefensiveCopy(t *testing.T) {
	c := NewMetadataCache()
	c.Store(DeviceMetadata{
		Enabled:     "true",
		PresetsList: []byte(`[{"Token":"1"}]`),
		IOFallback:  []ONVIFEvents{{Key: "DI1", Type: "input"}},
	})

	snap := c.Snapshot()
	snap.PresetsList[0] = 'X'
	snap.IOFallback[0].Key = "MUTATED"

	fresh := c.Snapshot()
	assert.Equal(t, `[{"Token":"1"}]`, string(fresh.PresetsList), "mutating a snapshot must not corrupt the cached PresetsList")
	assert.Equal(t, "DI1", fresh.IOFallback[0].Key, "mutating a snapshot must not corrupt the cached IOFallback")
}

// GatherHeartbeatMetadata is the one place that does blocking ONVIF
// I/O. With no XAddr configured it must short-circuit to defaults and
// never touch the network.
func TestGatherHeartbeatMetadata_NoXAddr_ReturnsDefault(t *testing.T) {
	camera := &models.IPCamera{ONVIFXAddr: ""}
	md := GatherHeartbeatMetadata(camera)
	assert.Equal(t, "false", md.Enabled)
	assert.Equal(t, "[]", string(md.PresetsList))
}

// GatherHeartbeatMetadata must tolerate a nil camera (defensive: the
// poller builds the struct, but the contract is "never panics").
func TestGatherHeartbeatMetadata_NilCamera_ReturnsDefault(t *testing.T) {
	md := GatherHeartbeatMetadata(nil)
	assert.Equal(t, "false", md.Enabled)
	assert.Equal(t, "[]", string(md.PresetsList))
}

// fix #2: every ONVIF call must be bounded so a wedged camera delays a
// single heartbeat by a known ceiling instead of stalling indefinitely.
func TestOnvifHTTPClient_HasTimeout(t *testing.T) {
	c := onvifHTTPClient()
	assert.NotNil(t, c)
	assert.Greater(t, int64(c.Timeout), int64(0), "ONVIF http client must have a non-zero timeout")
}
