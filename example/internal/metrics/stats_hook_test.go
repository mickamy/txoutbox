package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
)

func TestStatsHookTracksCounters(t *testing.T) {
	hook := NewStatsHook(fmt.Sprintf("test_%d", time.Now().UnixNano()))
	env := txoutbox.Envelope{ID: 1}

	hook.OnClaim(context.Background(), 3, 2)
	hook.OnSendSuccess(context.Background(), env)
	hook.OnSendFailure(context.Background(), env, fmt.Errorf("boom"))
	hook.OnRetry(context.Background(), env, 2, time.Second)
	hook.OnFail(context.Background(), env, 3, fmt.Errorf("fail"))
	hook.OnStoreError(context.Background(), "send", env.ID, fmt.Errorf("db down"))
	hook.OnCycle(context.Background(), time.Millisecond)

	snap := hook.snapshot()
	if snap["requested"] != 3 {
		t.Fatalf("requested = %d, want 3", snap["requested"])
	}
	if snap["claimed"] != 2 {
		t.Fatalf("claimed = %d, want 2", snap["claimed"])
	}
	if snap["send_success"] != 1 || snap["send_failure"] != 1 {
		t.Fatalf("send counters = %+v", snap)
	}
	if snap["retries"] != 1 || snap["failures"] != 1 {
		t.Fatalf("retry/fail counters = %+v", snap)
	}
	if snap["store_errors"] != 1 {
		t.Fatalf("store_errors = %d, want 1", snap["store_errors"])
	}
	if snap["cycles"] != 1 {
		t.Fatalf("cycles = %d, want 1", snap["cycles"])
	}
	if snap["cycle_latency_ns"] <= 0 {
		t.Fatalf("cycle_latency_ns = %d, want > 0", snap["cycle_latency_ns"])
	}
}
