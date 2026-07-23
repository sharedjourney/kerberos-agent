package onvif

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamHTTPTimeoutOutlastsPull is the regression guard for the
// timeout collision that shipped: the event-stream HTTP client and the
// PullMessages long-poll were both 5s. http.Client.Timeout covers the
// whole exchange and starts before the camera has parsed the request,
// so it always expired first and every quiet pull failed. The stream
// stayed alive only because the subscription was recreated after three
// failures, which made the camera replay its entire property state on
// an ~18s cadence.
//
// Keep the strict inequality. Equal values are the exact bug.
func TestStreamHTTPTimeoutOutlastsPull(t *testing.T) {
	assert.Greater(t, onvifStreamHTTPTimeout, onvifStreamPullTimeout,
		"the HTTP ceiling must outlast the long-poll or every quiet pull aborts")
}

// TestStreamHTTPClientIsSeparateFromCallClient — the 5s bound exists to
// stop a wedged camera stalling the heartbeat's inline ONVIF calls. The
// event stream deliberately blocks, so it must not inherit that bound.
func TestStreamHTTPClientIsSeparateFromCallClient(t *testing.T) {
	call := onvifHTTPClient()
	streamClient := onvifStreamHTTPClient()

	require.NotNil(t, call)
	require.NotNil(t, streamClient)
	assert.Equal(t, onvifCallTimeout, call.Timeout,
		"metadata/heartbeat calls keep the short bound")
	assert.Equal(t, onvifStreamHTTPTimeout, streamClient.Timeout,
		"the event stream gets the long bound")
	assert.Greater(t, streamClient.Timeout, call.Timeout,
		"a long-poll cannot share the short-call ceiling")
}

// TestStreamOptionsCarryPullTimeout — the long-poll duration has to
// reach the stream package; leaving it zero silently restores its 5s
// default and reintroduces the collision.
func TestStreamOptionsCarryPullTimeout(t *testing.T) {
	opts := streamOptions("cam-1")

	assert.Equal(t, "cam-1", opts.DeviceID)
	assert.Equal(t, onvifStreamPullTimeout, opts.PullTimeout,
		"PullTimeout must be set explicitly, not left to the package default")
	assert.Empty(t, opts.RawTopicFilter,
		"server-side topic filtering is fragile across vendors and must stay off for AXIS")
}
