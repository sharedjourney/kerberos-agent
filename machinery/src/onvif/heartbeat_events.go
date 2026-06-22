package onvif

import (
	"encoding/json"
	"time"

	onvifsdk "github.com/kerberos-io/onvif"

	"github.com/kerberos-io/agent/machinery/src/log"
)

// AssembleHeartbeatEvents builds the onvif_events_list JSON. Prefers
// the live event-stream cache; when it has not yet observed any tokens
// it uses the supplied fallback (the digital-I/O token list gathered
// out-of-band by the metadata poller) so the heartbeat still surfaces
// something the UI can list. This path is intentionally non-blocking:
// it never talks to the camera, so it cannot delay a heartbeat. Returns
// "[]" rather than nil when nothing is available.
func AssembleHeartbeatEvents(fallback []ONVIFEvents) []byte {
	events := SharedEventCache().Snapshot()
	if len(events) == 0 {
		events = fallback
	}
	if len(events) == 0 {
		return []byte("[]")
	}
	out, err := json.Marshal(events)
	if err != nil {
		log.Log.Error("onvif.AssembleHeartbeatEvents(): marshal: " + err.Error())
		return []byte("[]")
	}
	return out
}

// MergeCacheTokensForHTTP returns the HTTP I/O endpoint response shape
// for the given type ("input" or "output"). Cached entries (with live
// Value/Timestamp) come first; raw device-API tokens are appended for
// any token not already present by Key.
//
// Comparison is by literal Key — cache uses "<token>-<type>", device
// API uses the bare token — preserving master's behaviour where both
// can appear for the same physical I/O. In the 1:1 agent-camera
// deployment model the cached entry carries the live state callers
// actually care about; the bare-token entry is harmless context.
func MergeCacheTokensForHTTP(eventType string, deviceTokens []string) []ONVIFEvents {
	var out []ONVIFEvents
	for _, e := range SharedEventCache().Snapshot() {
		if e.Type == eventType {
			out = append(out, e)
		}
	}
	for _, tok := range deviceTokens {
		seen := false
		for _, e := range out {
			if e.Key == tok {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, ONVIFEvents{Key: tok, Type: eventType})
		}
	}
	return out
}

// enumerateIOTokens lists tokens via the device API as a fallback for
// the cache. Value is "false" because the device API does not expose
// state.
func enumerateIOTokens(device *onvifsdk.Device) []ONVIFEvents {
	if device == nil {
		return nil
	}
	var events []ONVIFEvents
	now := time.Now().Unix()

	if outputs, err := GetRelayOutputs(device); err == nil {
		for _, output := range outputs.RelayOutputs {
			events = append(events, ONVIFEvents{
				Key:       string(output.Token),
				Value:     "false",
				Type:      "output",
				Timestamp: now,
			})
		}
	} else {
		log.Log.Debug("onvif.enumerateIOTokens(): GetRelayOutputs: " + err.Error())
	}

	if inputs, err := GetDigitalInputs(device); err == nil {
		for _, input := range inputs.DigitalInputs {
			events = append(events, ONVIFEvents{
				Key:       string(input.Token),
				Value:     "false",
				Type:      "input",
				Timestamp: now,
			})
		}
	} else {
		log.Log.Debug("onvif.enumerateIOTokens(): GetDigitalInputs: " + err.Error())
	}
	return events
}
