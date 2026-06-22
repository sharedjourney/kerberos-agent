package onvif

import (
	"testing"

	"github.com/kerberos-io/agent/machinery/src/models"
	"github.com/stretchr/testify/assert"
)

// resetMetadata isolates each test from the package-global singleton.
func resetMetadata(t *testing.T) {
	t.Helper()
	SharedMetadataCache().Reset()
	t.Cleanup(func() { SharedMetadataCache().Reset() })
}

func TestMetadataCache_Default_WhenNothingStored(t *testing.T) {
	resetMetadata(t)

	md := SharedMetadataCache().Snapshot()
	assert.Equal(t, "false", md.Enabled)
	assert.Equal(t, "false", md.Zoom)
	assert.Equal(t, "false", md.PanTilt)
	assert.Equal(t, "false", md.Presets)
	assert.Equal(t, "[]", string(md.PresetsList), "presets list must be a valid empty JSON array, never nil")
}

func TestMetadataCache_StoreSnapshot_RoundTrips(t *testing.T) {
	resetMetadata(t)

	stored := DeviceMetadata{
		Enabled:     "true",
		Zoom:        "true",
		PanTilt:     "false",
		Presets:     "true",
		PresetsList: []byte(`[{"Token":"1"}]`),
		IOFallback:  []ONVIFEvents{{Key: "DI1", Type: "input"}},
	}
	SharedMetadataCache().Store(stored)

	got := SharedMetadataCache().Snapshot()
	assert.Equal(t, "true", got.Enabled)
	assert.Equal(t, "true", got.Zoom)
	assert.Equal(t, "false", got.PanTilt)
	assert.Equal(t, "true", got.Presets)
	assert.Equal(t, `[{"Token":"1"}]`, string(got.PresetsList))
	assert.Equal(t, []ONVIFEvents{{Key: "DI1", Type: "input"}}, got.IOFallback)
}

// The read path the heartbeat depends on must be pure in-memory: a
// Snapshot must never trigger a device round-trip. We can't assert "no
// network" directly, but a Snapshot before any gather has stored data
// must return immediately with defaults rather than block.
func TestMetadataCache_SnapshotIsNonBlockingDefault(t *testing.T) {
	resetMetadata(t)
	md := SharedMetadataCache().Snapshot()
	assert.Equal(t, "false", md.Enabled)
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

// fix #2: every ONVIF call must be bounded so a wedged camera delays a
// single heartbeat by a known ceiling instead of stalling indefinitely.
func TestOnvifHTTPClient_HasTimeout(t *testing.T) {
	c := onvifHTTPClient()
	assert.NotNil(t, c)
	assert.Greater(t, int64(c.Timeout), int64(0), "ONVIF http client must have a non-zero timeout")
}
