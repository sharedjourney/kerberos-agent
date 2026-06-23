package cloud

import (
	"testing"
	"time"

	"github.com/kerberos-io/agent/machinery/src/models"
	"github.com/kerberos-io/agent/machinery/src/onvif"
	"github.com/stretchr/testify/assert"
)

// pollONVIFMetadata is the orchestration that keeps blocking ONVIF I/O
// off the heartbeat send path: it primes the shared metadata cache once
// immediately, then ticks until stopped. This locks the prime-once +
// clean-stop contract (the heartbeat-stall fix) without needing a real
// camera — an empty ONVIFXAddr makes GatherHeartbeatMetadata return
// defaults synchronously and touch no network.
func TestPollONVIFMetadata_PrimesOnceAndStops(t *testing.T) {
	cfg := &models.Configuration{
		Config: models.Config{
			Capture: models.Capture{
				IPCamera: models.IPCamera{ONVIFXAddr: ""}, // no camera -> defaults, no I/O
			},
		},
	}

	cache := onvif.NewMetadataCache()
	// Seed a non-default value so we can prove the poller actually wrote
	// the cache (priming), rather than the cache merely starting at
	// defaults.
	cache.Store(onvif.DeviceMetadata{Enabled: "true"})

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		pollONVIFMetadata(cfg, stop, cache)
		close(done)
	}()

	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollONVIFMetadata must return promptly after stop is closed")
	}

	// The immediate prime ran: the removed camera reset the cache back to
	// defaults.
	assert.Equal(t, "false", cache.Snapshot().Enabled, "poller must prime the cache to defaults when no ONVIF camera is configured")
}
