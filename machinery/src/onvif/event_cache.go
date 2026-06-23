package onvif

import (
	"sort"
	"sync"
	"time"

	"github.com/kerberos-io/onvif/event/stream"
)

// ONVIFEvents is the wire shape returned in heartbeat onvif_events_list
// and the HTTP I/O endpoints. Key encodes "<token>-input" or
// "<token>-output"; Value is "true"/"false"; Timestamp is unix seconds
// of the last Changed observation (0 for first-sight / Initialized).
type ONVIFEvents struct {
	Key       string
	Type      string
	Value     string
	Timestamp int64
}

// EventCache holds the most recent digital input/output state per
// token. The heartbeat snapshots this instead of opening a parallel
// pull-point subscription, so the agent holds a single ONVIF
// subscription per camera.
//
// One instance per agent is created at bootstrap and carried on
// models.Communication (see EventCacheFor); the event-stream goroutine
// writes it, the heartbeat and HTTP I/O endpoints read it. Per-camera
// caching would key the items by DeviceID.
type EventCache struct {
	mu    sync.RWMutex
	items map[string]ONVIFEvents
	// nowFn lets tests pin the clock to exercise the 15s polling-
	// active heuristic at its boundary.
	nowFn func() int64
}

func NewEventCache() *EventCache {
	return &EventCache{
		items: make(map[string]ONVIFEvents),
		nowFn: func() int64 { return time.Now().Unix() },
	}
}

// Apply folds a stream.Event into the cache.
//
//   - Initialized: seed/refresh Value without stamping Timestamp, so
//     first-sight inactive inputs are not later flipped "active" by
//     the 15s polling heuristic in Snapshot.
//   - Changed first-sight: same — seed without timestamp. The
//     heuristic should only kick in on the second observation.
//   - Changed: update Value and stamp Timestamp.
//   - Deleted: remove the entry.
//   - Unknown (missing PropertyOperation): ignored. Treating absent
//     as Changed would let malformed notifications poison the
//     heuristic.
func (c *EventCache) Apply(ev stream.Event) {
	if c == nil {
		return
	}
	suffix := cacheKindSuffix(ev.Kind)
	if suffix == "" {
		return
	}
	token := sourceToken(ev.Source)
	if token == "" {
		return
	}
	key := token + "-" + suffix

	c.mu.Lock()
	defer c.mu.Unlock()

	switch ev.Operation {
	case stream.PropertyDeleted:
		delete(c.items, key)
	case stream.PropertyInitialized:
		existing, ok := c.items[key]
		if !ok {
			c.items[key] = ONVIFEvents{Key: key, Type: suffix, Value: stateToLegacyValue(ev.State)}
			return
		}
		existing.Value = stateToLegacyValue(ev.State)
		c.items[key] = existing
	case stream.PropertyChanged:
		existing, ok := c.items[key]
		if !ok {
			c.items[key] = ONVIFEvents{Key: key, Type: suffix, Value: stateToLegacyValue(ev.State)}
			return
		}
		existing.Value = stateToLegacyValue(ev.State)
		existing.Timestamp = c.nowFn()
		c.items[key] = existing
	}
}

// Snapshot returns a stable, ordered copy of the cache.
//
// The 15s window flips a recent "false" to "true" because some
// Hikvision firmwares emit a steady stream of "false" events while the
// input is actually high; absence of recent events is the real signal
// for inactive.
func (c *EventCache) Snapshot() []ONVIFEvents {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.items) == 0 {
		return nil
	}
	out := make([]ONVIFEvents, 0, len(c.items))
	now := c.nowFn()
	for _, v := range c.items {
		if now-v.Timestamp < 15 && v.Value == "false" {
			v.Value = "true"
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Reset is called by the event-stream goroutine on (re)connect so the
// heartbeat cannot publish tokens cached from a previous run or a
// different camera.
func (c *EventCache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]ONVIFEvents)
}

func cacheKindSuffix(k stream.Kind) string {
	switch k {
	case stream.KindDigitalInput:
		return "input"
	case stream.KindDigitalOutput:
		return "output"
	default:
		return ""
	}
}

func stateToLegacyValue(s stream.State) string {
	if s == stream.StateActive {
		return "true"
	}
	return "false"
}

// sourceToken picks the token-bearing value from an ONVIF Source map.
// Cameras label the field differently (InputToken, OutputToken,
// RelayToken, Source); the known-name list is the priority order. The
// sorted-key fallback exists so behaviour does not depend on Go's
// randomised map iteration.
func sourceToken(src map[string]string) string {
	if len(src) == 0 {
		return ""
	}
	for _, name := range []string{"InputToken", "OutputToken", "RelayToken", "Source"} {
		if v, ok := src[name]; ok && v != "" {
			return v
		}
	}
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if v := src[k]; v != "" {
			return v
		}
	}
	return ""
}
