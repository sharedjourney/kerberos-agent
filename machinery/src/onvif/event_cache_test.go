package onvif

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kerberos-io/onvif/event/stream"
	"github.com/stretchr/testify/assert"
)

// newFixedCache returns a cache whose clock is pinned to the supplied
// pointer. Tests can mutate *now to advance time deterministically and
// exercise the 15s heuristic at its boundary.
func newFixedCache(now *int64) *EventCache {
	c := NewEventCache()
	c.nowFn = func() int64 { return atomic.LoadInt64(now) }
	return c
}

// --- Apply: operation semantics --------------------------------------

func TestEventCache_InitializedFirstSight_SeedsWithoutTimestamp(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{
		Kind:      stream.KindDigitalInput,
		State:     stream.StateInactive,
		Operation: stream.PropertyInitialized,
		Source:    map[string]string{"InputToken": "DI1"},
	})
	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "DI1-input", snap[0].Key)
		assert.Equal(t, "input", snap[0].Type)
		assert.Equal(t, "false", snap[0].Value)
		assert.Equal(t, int64(0), snap[0].Timestamp, "Initialized first-sight must not stamp a timestamp")
	}
}

func TestEventCache_InitializedRepeated_DoesNotStampTimestamp(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})
	atomic.StoreInt64(&now, 2000)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})

	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "true", snap[0].Value)
		assert.Equal(t, int64(0), snap[0].Timestamp, "Initialized must never stamp Timestamp, even when refreshing Value")
	}
}

func TestEventCache_ChangedFirstSight_SeedsWithoutTimestamp(t *testing.T) {
	// Locks the legacy semantics: a Changed event without a prior
	// Initialized still does NOT stamp a timestamp. The 15s polling
	// heuristic only kicks in once a second observation arrives.
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})
	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "false", snap[0].Value)
		assert.Equal(t, int64(0), snap[0].Timestamp)
	}
}

func TestEventCache_ChangedStampsTimestamp(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})
	atomic.StoreInt64(&now, 1234)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})

	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "true", snap[0].Value)
		assert.Equal(t, int64(1234), snap[0].Timestamp)
	}
}

func TestEventCache_DeletedRemovesKey(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyDeleted, Source: map[string]string{"InputToken": "DI1"}})
	assert.Empty(t, c.Snapshot())
}

func TestEventCache_UnknownOperationIgnored(t *testing.T) {
	// A malformed notification missing PropertyOperation must not
	// touch the cache. Treating Unknown as Changed would let it
	// poison the timestamp and the 15s heuristic.
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})
	assert.Empty(t, c.Snapshot())
}

// --- Apply: kind / source guards -------------------------------------

func TestEventCache_RelayMappedToOutputSuffix(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalOutput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"OutputToken": "DO1"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalOutput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"OutputToken": "DO1"}})
	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "DO1-output", snap[0].Key)
		assert.Equal(t, "output", snap[0].Type)
	}
}

func TestEventCache_SameTokenInputAndOutput_BothKept(t *testing.T) {
	// Some cameras reuse the same token string for an input and an
	// output. The suffix in the key keeps them distinct.
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateActive, Source: map[string]string{"InputToken": "1"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalOutput, Operation: stream.PropertyInitialized, State: stream.StateInactive, Source: map[string]string{"OutputToken": "1"}})
	snap := c.Snapshot()
	if assert.Len(t, snap, 2) {
		assert.Equal(t, "1-input", snap[0].Key)
		assert.Equal(t, "1-output", snap[1].Key)
	}
}

func TestEventCache_UnsupportedKindIgnored(t *testing.T) {
	c := NewEventCache()
	c.Apply(stream.Event{Kind: stream.KindMotion, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"VideoSource": "cam1"}})
	assert.Empty(t, c.Snapshot())
}

func TestEventCache_MissingSourceTokenIgnored(t *testing.T) {
	c := NewEventCache()
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateActive})
	assert.Empty(t, c.Snapshot())
}

// --- Snapshot: 15s polling-active heuristic --------------------------

func TestEventCache_RecentFalseTreatedAsActive(t *testing.T) {
	// Hikvision-style polling: after the first-sight seed, repeated
	// "false" events stamp a recent timestamp; the 15s heuristic then
	// flips the reported value back to "true".
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})
	atomic.StoreInt64(&now, 1010)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})

	atomic.StoreInt64(&now, 1014) // 4s after Changed — well inside 15s
	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "true", snap[0].Value)
	}
}

func TestEventCache_FifteenSecondBoundary(t *testing.T) {
	// At exactly +15s the "<15" comparison flips the value back to
	// its literal "false". Locks the boundary so a future refactor
	// cannot silently move it.
	now := int64(1000)
	c := newFixedCache(&now)
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateInactive, Source: map[string]string{"InputToken": "DI1"}})

	atomic.StoreInt64(&now, 1014)
	assert.Equal(t, "true", c.Snapshot()[0].Value, "now-ts=14 still active")

	atomic.StoreInt64(&now, 1015)
	assert.Equal(t, "false", c.Snapshot()[0].Value, "now-ts=15 reverts to literal value")
}

func TestEventCache_StaleFalseStaysFalse(t *testing.T) {
	now := int64(1000)
	c := newFixedCache(&now)
	c.items["DI1-input"] = ONVIFEvents{Key: "DI1-input", Type: "input", Value: "false", Timestamp: 100}
	snap := c.Snapshot()
	if assert.Len(t, snap, 1) {
		assert.Equal(t, "false", snap[0].Value)
	}
}

// --- Snapshot: ordering and Reset ------------------------------------

func TestEventCache_SnapshotSortedByKey(t *testing.T) {
	c := NewEventCache()
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"InputToken": "DI2"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})
	c.Apply(stream.Event{Kind: stream.KindDigitalOutput, Operation: stream.PropertyChanged, State: stream.StateActive, Source: map[string]string{"OutputToken": "DO1"}})
	snap := c.Snapshot()
	if assert.Len(t, snap, 3) {
		assert.Equal(t, "DI1-input", snap[0].Key)
		assert.Equal(t, "DI2-input", snap[1].Key)
		assert.Equal(t, "DO1-output", snap[2].Key)
	}
}

func TestEventCache_ResetClears(t *testing.T) {
	c := NewEventCache()
	c.Apply(stream.Event{Kind: stream.KindDigitalInput, Operation: stream.PropertyInitialized, State: stream.StateActive, Source: map[string]string{"InputToken": "DI1"}})
	assert.Len(t, c.Snapshot(), 1)
	c.Reset()
	assert.Empty(t, c.Snapshot())
}

// --- Concurrency -----------------------------------------------------

func TestEventCache_ConcurrentApplyAndSnapshot(t *testing.T) {
	// Run -race to make this meaningful. Concurrent Apply / Snapshot /
	// Reset must not produce data races.
	c := NewEventCache()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.Apply(stream.Event{
					Kind:      stream.KindDigitalInput,
					Operation: stream.PropertyChanged,
					State:     stream.StateActive,
					Source:    map[string]string{"InputToken": string(rune('A' + id))},
				})
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.Snapshot()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.Reset()
		}
	}()

	// Let the goroutines hammer the cache briefly. Test passes if -race
	// does not flag anything; we are not asserting state here.
	for i := 0; i < 1000; i++ {
		_ = c.Snapshot()
	}
	close(stop)
	wg.Wait()
}

// --- Helpers ---------------------------------------------------------

func TestSourceToken_PrefersKnownNames(t *testing.T) {
	tests := []struct {
		name string
		src  map[string]string
		want string
	}{
		{"input_token", map[string]string{"InputToken": "DI1"}, "DI1"},
		{"output_token", map[string]string{"OutputToken": "DO1"}, "DO1"},
		{"relay_token", map[string]string{"RelayToken": "R1"}, "R1"},
		{"unknown_key_falls_back_to_value", map[string]string{"WeirdKey": "X9"}, "X9"},
		{"empty_value_skipped", map[string]string{"InputToken": "", "OtherKey": "Y"}, "Y"},
		{"empty_source", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sourceToken(tc.src))
		})
	}
}

func TestStateToLegacyValue(t *testing.T) {
	assert.Equal(t, "true", stateToLegacyValue(stream.StateActive))
	assert.Equal(t, "false", stateToLegacyValue(stream.StateInactive))
	assert.Equal(t, "false", stateToLegacyValue(stream.StateUnknown))
}
