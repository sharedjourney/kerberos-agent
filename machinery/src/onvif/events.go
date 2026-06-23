package onvif

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/kerberos-io/agent/machinery/src/log"
	"github.com/kerberos-io/agent/machinery/src/models"
	"github.com/kerberos-io/onvif/event/stream"
)

// The library handles in-stream reconnect; these guards cover the
// initial-connect path the library cannot see.
const (
	initialBackoff = time.Second
	maxBackoff     = 5 * time.Minute
)

// HandleONVIFEventStream feeds digital-input/output events into the
// shared EventCache (so the heartbeat doesn't need its own
// subscription) and routes motion events into communication.HandleMotion.
//
// The ONVIFMotion flag gates only motion dispatch — the cache is
// populated whenever ONVIFXAddr is set. On transient construction
// failures (boot, network blip, credential reload) retries with
// exponential backoff. Exits when ctx is cancelled.
func HandleONVIFEventStream(ctx context.Context, configuration *models.Configuration, communication *models.Communication) {
	log.Log.Debug("onvif.HandleONVIFEventStream(): started")
	defer SharedEventCache().Reset() // belt-and-braces for clean-shutdown lifecycle
	defer log.Log.Debug("onvif.HandleONVIFEventStream(): finished")

	if configuration.Config.Capture.IPCamera.ONVIFXAddr == "" {
		return
	}

	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		ran, recoverable := runStreamOnce(ctx, configuration, communication)
		if !recoverable {
			return
		}
		// Reset backoff after a healthy run so a flaky reconnect
		// after hours of uptime does not inherit the max delay from
		// a long-past failure.
		if ran {
			backoff = initialBackoff
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runStreamOnce returns (ran, retry). ran=true if at least one event
// or error was observed before exit. retry=true on transient failure
// (including channel-close while ctx is still alive — that means the
// stream library wedged and we should reconnect); false on clean
// ctx-driven shutdown.
func runStreamOnce(ctx context.Context, configuration *models.Configuration, communication *models.Communication) (ran, retry bool) {
	camera := configuration.Config.Capture.IPCamera

	// Drop tokens from any previous run/camera before connecting so
	// the heartbeat cannot snapshot stale state during the connect
	// window.
	SharedEventCache().Reset()

	device, _, err := ConnectToOnvifDevice(&camera)
	if err != nil {
		log.Log.Error("onvif.HandleONVIFEventStream(): connect: " + err.Error())
		return false, true
	}

	deviceID := resolveDeviceID(configuration.Name, camera.ONVIFXAddr)
	s, err := stream.NewStream(ctx, device, stream.Options{DeviceID: deviceID})
	if err != nil {
		log.Log.Error("onvif.HandleONVIFEventStream(): open stream: " + err.Error())
		return false, true
	}
	defer func() {
		if err := s.Close(); err != nil {
			log.Log.Debug("onvif.HandleONVIFEventStream(): close: " + err.Error())
		}
	}()

	log.Log.Info("onvif.HandleONVIFEventStream(): consuming events for " + deviceID)

	var recovering bool
	for {
		select {
		case <-ctx.Done():
			return ran, false
		case ev, ok := <-s.Events():
			if !ok {
				// Library closed the channel while ctx is still
				// alive: the stream wedged. Retry.
				return ran, ctx.Err() == nil
			}
			ran = true
			handleStreamEvent(ctx, ev, configuration, communication, deviceID, &recovering)
		case e, ok := <-s.Errors():
			if !ok {
				return ran, ctx.Err() == nil
			}
			ran = true
			recovering = true
			logStreamError(e)
		}
	}
}

// handleStreamEvent applies a single event to the shared cache and
// dispatches to the motion channel. Split out from the select body so
// the cache-wiring path is unit-testable. recovering flips back to
// false on the first event after an error streak so on-call sees the
// clear-of-condition.
func handleStreamEvent(
	ctx context.Context,
	ev stream.Event,
	configuration *models.Configuration,
	communication *models.Communication,
	deviceID string,
	recovering *bool,
) {
	if *recovering {
		log.Log.Info("onvif.HandleONVIFEventStream(): event stream recovered for " + deviceID)
		*recovering = false
	}
	SharedEventCache().Apply(ev)
	dispatchEvent(ctx, ev, configuration, communication)
}

// dispatchEvent routes motion-active events to HandleMotion. The
// ctx pre-check + the ctx arm in the select narrow the shutdown-race
// window where HandleMotion is closed shortly after ctx is cancelled:
// without the ctx arm, a select on a closed channel picks the
// send-and-panic case before default.
func dispatchEvent(ctx context.Context, ev stream.Event, configuration *models.Configuration, communication *models.Communication) {
	if !isONVIFMotionEnabled(configuration.Config.Capture.ONVIFMotion) {
		return
	}
	topic := sanitiseTopic(ev.Topic)
	if ev.Kind != stream.KindMotion {
		log.Log.Debug("onvif.dispatchEvent(): non-motion event " + ev.Kind.String() + " topic=" + topic)
		return
	}
	if ev.State != stream.StateActive {
		return
	}
	if !isTransition(ev.Operation) {
		log.Log.Debug("onvif.dispatchEvent(): " + ev.Operation.String() + " is not a transition, not a trigger: topic=" + topic)
		return
	}
	if configuration.Config.Capture.Recording == "false" {
		return
	}
	if ctx.Err() != nil {
		return
	}

	dataToPass := models.MotionDataPartial{
		Timestamp:       time.Now().Unix(),
		NumberOfChanges: 0, // ONVIF does not quantify motion area.
	}
	defer func() {
		if r := recover(); r != nil {
			// HandleMotion was closed by the shutdown path mid-dispatch.
			log.Log.Debug("onvif.dispatchEvent(): HandleMotion closed during shutdown, dropping ONVIF motion event")
		}
	}()
	select {
	case <-ctx.Done():
	case communication.HandleMotion <- dataToPass:
		// Logged on the send, not before it: this line records that a
		// recording started, so a dropped event must not leave one.
		log.Log.Debug("onvif.dispatchEvent(): recording trigger " + ev.Kind.String() + " topic=" + topic)
	default:
		log.Log.Debug("onvif.dispatchEvent(): HandleMotion full, dropping ONVIF motion event")
	}
}

// isTransition reports whether an operation represents a state change.
// A camera replays every property's current state as Initialized on
// each new subscription and announces removals as Deleted; neither is
// motion starting. Absent (Unknown) counts — PropertyOperation is
// optional per WS-Notification and many non-property events omit it.
func isTransition(op stream.PropertyOperation) bool {
	return op == stream.PropertyChanged || op == stream.PropertyUnknown
}

// maxLoggedTopic bounds a topic in the log; the wire imposes no limit,
// and the reject path logs every event received.
const maxLoggedTopic = 256

// sanitiseTopic makes a camera-controlled topic safe to concatenate
// into a log line. logrus's coloured text formatter writes the message
// unquoted, so a raw newline would let a camera forge entries in the
// log being used to diagnose it.
func sanitiseTopic(topic string) string {
	if len(topic) > maxLoggedTopic {
		topic = topic[:maxLoggedTopic] + "…(truncated)"
	}
	quoted := strconv.Quote(topic)
	return quoted[1 : len(quoted)-1]
}

// logStreamError log levels: recreate is ERROR (camera likely
// offline); pull/renew are Debug since the library recovers.
func logStreamError(e error) {
	var recreate stream.ErrRecreateFailed
	var pull stream.ErrPullFailed
	var renew stream.ErrRenewFailed
	switch {
	case errors.As(e, &recreate):
		log.Log.Error("onvif.HandleONVIFEventStream(): subscription recreate failed (camera may be offline): " + recreate.Err.Error())
	case errors.As(e, &renew):
		log.Log.Debug("onvif.HandleONVIFEventStream(): renew failed (will recover via pull/recreate): " + renew.Err.Error())
	case errors.As(e, &pull):
		log.Log.Debug("onvif.HandleONVIFEventStream(): pull failed (will retry): " + pull.Err.Error())
	default:
		log.Log.Info("onvif.HandleONVIFEventStream(): stream error: " + e.Error())
	}
}

func isONVIFMotionEnabled(v string) bool {
	return strings.EqualFold(strings.TrimSpace(v), "true")
}

// resolveDeviceID: name → xaddr → "unknown", so log lines always
// have something to grep.
func resolveDeviceID(configName, xaddr string) string {
	if n := strings.TrimSpace(configName); n != "" {
		return n
	}
	if x := strings.TrimSpace(xaddr); x != "" {
		return x
	}
	return "unknown"
}

// sleepCtx returns false if ctx was cancelled, true if d elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
