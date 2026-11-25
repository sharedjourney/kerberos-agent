package cloud

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// newTwoChannelONVIFDevice fakes a two-sensor camera whose GetPresets names
// its preset after the profile token it was asked for.
func newTwoChannelONVIFDevice(t *testing.T) string {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := string(body)
		var resp string
		switch {
		case strings.Contains(req, "GetCapabilities"):
			resp = `<GetCapabilitiesResponse><Capabilities>` +
				`<Media><XAddr>` + srv.URL + `/onvif/media</XAddr></Media>` +
				`<PTZ><XAddr>` + srv.URL + `/onvif/ptz</XAddr></PTZ>` +
				`</Capabilities></GetCapabilitiesResponse>`
		case strings.Contains(req, "GetProfiles"):
			resp = `<GetProfilesResponse>` +
				`<Profiles token="profile-ch1"><VideoSourceConfiguration><SourceToken>source-1</SourceToken></VideoSourceConfiguration></Profiles>` +
				`<Profiles token="profile-ch2"><VideoSourceConfiguration><SourceToken>source-2</SourceToken></VideoSourceConfiguration></Profiles>` +
				`</GetProfilesResponse>`
		case strings.Contains(req, "GetConfigurations"):
			resp = `<GetConfigurationsResponse></GetConfigurationsResponse>`
		case strings.Contains(req, "GetPresets"):
			token := "profile-ch1"
			if strings.Contains(req, "profile-ch2") {
				token = "profile-ch2"
			}
			resp = `<GetPresetsResponse><Preset token="p1"><Name>preset-of-` + token + `</Name></Preset></GetPresetsResponse>`
		}
		_, _ = w.Write([]byte(`<Envelope><Body>` + resp + `</Body></Envelope>`))
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestPollONVIFMetadata_UsesConfiguredChannel(t *testing.T) {
	cfg := &models.Configuration{
		Config: models.Config{
			Capture: models.Capture{
				IPCamera: models.IPCamera{ONVIFXAddr: newTwoChannelONVIFDevice(t), ONVIFChannel: 2},
			},
		},
	}
	cache := onvif.NewMetadataCache()
	stop := make(chan struct{})
	close(stop) // the prime runs before stop is checked

	pollONVIFMetadata(cfg, stop, cache)

	assert.Contains(t, string(cache.Snapshot().PresetsList), "preset-of-profile-ch2",
		"the poller must pass the camera's ONVIF channel through to the metadata gather")
}
