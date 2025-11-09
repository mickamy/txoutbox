package metrics

import (
	"context"
	"expvar"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mickamy/txoutbox"
)

// StatsHook publishes basic txoutbox counters via expvar.
type StatsHook struct {
	requested      atomic.Int64
	claimed        atomic.Int64
	sendSuccess    atomic.Int64
	sendFailure    atomic.Int64
	retries        atomic.Int64
	failures       atomic.Int64
	storeErrors    atomic.Int64
	cycles         atomic.Int64
	cycleLatencyNs atomic.Int64
}

// NewStatsHook registers an expvar entry named "<prefix>_stats".
func NewStatsHook(prefix string) *StatsHook {
	if prefix == "" {
		prefix = "txoutbox"
	}
	h := &StatsHook{}
	expvar.Publish(fmt.Sprintf("%s_stats", prefix), expvar.Func(func() any {
		return h.snapshot()
	}))
	return h
}

// OnClaim tracks how many rows we attempted to claim and how many we actually received.
func (h *StatsHook) OnClaim(_ context.Context, batchSize int, claimed int) {
	h.requested.Add(int64(batchSize))
	h.claimed.Add(int64(claimed))
}

// OnSendSuccess increments successful deliveries.
func (h *StatsHook) OnSendSuccess(_ context.Context, _ txoutbox.Envelope) {
	h.sendSuccess.Add(1)
}

// OnSendFailure increments failed deliveries before retry/fail handling.
func (h *StatsHook) OnSendFailure(_ context.Context, _ txoutbox.Envelope, _ error) {
	h.sendFailure.Add(1)
}

// OnRetry increments retry counters.
func (h *StatsHook) OnRetry(_ context.Context, _ txoutbox.Envelope, _ int, _ time.Duration) {
	h.retries.Add(1)
}

// OnFail increments permanent failures.
func (h *StatsHook) OnFail(_ context.Context, _ txoutbox.Envelope, _ int, _ error) {
	h.failures.Add(1)
}

// OnStoreError increments store error counter.
func (h *StatsHook) OnStoreError(_ context.Context, _ string, _ int64, _ error) {
	h.storeErrors.Add(1)
}

// OnCycle records cycle durations and counts.
func (h *StatsHook) OnCycle(_ context.Context, d time.Duration) {
	h.cycles.Add(1)
	h.cycleLatencyNs.Add(d.Nanoseconds())
}

func (h *StatsHook) snapshot() map[string]int64 {
	return map[string]int64{
		"requested":        h.requested.Load(),
		"claimed":          h.claimed.Load(),
		"send_success":     h.sendSuccess.Load(),
		"send_failure":     h.sendFailure.Load(),
		"retries":          h.retries.Load(),
		"failures":         h.failures.Load(),
		"store_errors":     h.storeErrors.Load(),
		"cycles":           h.cycles.Load(),
		"cycle_latency_ns": h.cycleLatencyNs.Load(),
	}
}
