package onvif

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/kerberos-io/agent/machinery/src/log"
	"github.com/kerberos-io/agent/machinery/src/models"
)

// onvifCallTimeout bounds every ONVIF round-trip. The SDK defaults to a
// timeout-less http.Client, so a camera that accepts the TCP connection
// but never answers (common when it is busy servicing the event-stream
// long-poll) would otherwise block the caller indefinitely. The
// heartbeat used to make these calls inline on its send path, which is
// how a slow camera stalled heartbeats while RTSP stayed healthy.
const onvifCallTimeout = 5 * time.Second

// onvifHTTPClient returns the bounded client handed to NewDevice so all
// ONVIF SOAP calls share one ceiling.
func onvifHTTPClient() *http.Client {
	return &http.Client{Timeout: onvifCallTimeout}
}

// DeviceMetadata is the slow-changing ONVIF capability snapshot the
// heartbeat reports: PTZ/preset capabilities plus a digital-I/O token
// fallback. Values are pre-rendered to the heartbeat wire format
// ("true"/"false", PresetsList as JSON) so the send path does zero work
// beyond reading them.
type DeviceMetadata struct {
	Enabled     string
	Zoom        string
	PanTilt     string
	Presets     string
	PresetsList []byte
	// IOFallback is the digital input/output token list enumerated via
	// the device API, used by AssembleHeartbeatEvents only when the
	// live event cache has not yet observed any tokens.
	IOFallback []ONVIFEvents
}

// DefaultMetadata is what the heartbeat reports before the poller has
// completed its first gather, or when ONVIF is unavailable. PresetsList
// is a valid empty JSON array so it can be embedded verbatim.
func DefaultMetadata() DeviceMetadata {
	return DeviceMetadata{
		Enabled:     "false",
		Zoom:        "false",
		PanTilt:     "false",
		Presets:     "false",
		PresetsList: []byte("[]"),
	}
}

// MetadataCache holds the most recent DeviceMetadata. A background
// poller writes it on its own cadence; the heartbeat loop reads it
// without ever touching the camera, so a slow ONVIF call can no longer
// delay a heartbeat.
//
// Singleton today (one configured camera per agent), matching
// EventCache.
type MetadataCache struct {
	mu   sync.RWMutex
	data DeviceMetadata
}

var sharedMetadataCache = NewMetadataCache()

func SharedMetadataCache() *MetadataCache { return sharedMetadataCache }

func NewMetadataCache() *MetadataCache {
	return &MetadataCache{data: DefaultMetadata()}
}

func (c *MetadataCache) Store(md DeviceMetadata) {
	if md.PresetsList == nil {
		md.PresetsList = []byte("[]")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = md
}

func (c *MetadataCache) Snapshot() DeviceMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data
}

// Reset returns the cache to defaults (used on shutdown and in tests).
func (c *MetadataCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = DefaultMetadata()
}

// GatherHeartbeatMetadata performs the blocking ONVIF I/O — connect,
// PTZ/preset capabilities, and digital-I/O token enumeration — and
// returns a ready-to-report snapshot. This is the ONLY function that
// talks to the camera for heartbeat metadata; it runs in the poller
// goroutine, never on the heartbeat send path. Always returns a usable
// value (defaults on any failure), never panics.
func GatherHeartbeatMetadata(camera *models.IPCamera) DeviceMetadata {
	md := DefaultMetadata()
	if camera == nil || camera.ONVIFXAddr == "" {
		return md
	}

	device, _, err := ConnectToOnvifDevice(camera)
	if err != nil {
		log.Log.Error("onvif.GatherHeartbeatMetadata(): connect: " + err.Error())
		return md
	}
	md.Enabled = "true"

	configurations, err := GetPTZConfigurationsFromDevice(device)
	if err == nil {
		_, canZoom, canPanTilt := GetPTZFunctionsFromDevice(configurations)
		if canZoom {
			md.Zoom = "true"
		}
		if canPanTilt {
			md.PanTilt = "true"
		}
		presets, err := GetPresetsFromDevice(device)
		if err == nil && len(presets) > 0 {
			md.Presets = "true"
			if b, err := json.Marshal(presets); err == nil {
				md.PresetsList = b
			} else {
				log.Log.Error("onvif.GatherHeartbeatMetadata(): marshal presets: " + err.Error())
			}
		} else if err != nil {
			log.Log.Debug("onvif.GatherHeartbeatMetadata(): get presets: " + err.Error())
		}
	} else {
		log.Log.Debug("onvif.GatherHeartbeatMetadata(): get PTZ configurations: " + err.Error())
	}

	md.IOFallback = enumerateIOTokens(device)
	return md
}
