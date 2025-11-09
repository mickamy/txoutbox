package txoutbox

import (
	"context"
	"time"
)

// Sender dispatches an outbox message to the actual transport.
type Sender interface {
	Send(ctx context.Context, msg Envelope) error
}

// Logger captures Relay logs; implementors can wrap slog/zap/etc.
type Logger interface {
	Info(ctx context.Context, format string, v ...any)
	Warn(ctx context.Context, format string, v ...any)
	Error(ctx context.Context, format string, v ...any)
}

// Hooks lets callers observe relay activity for metrics/tracing/logs.
type Hooks interface {
	// OnClaim fires after each Claim with the requested batch vs actual rows.
	OnClaim(ctx context.Context, batchSize int, claimed int)
	// OnSendSuccess fires for every Envelope delivered successfully.
	OnSendSuccess(ctx context.Context, env Envelope)
	// OnSendFailure fires when Sender returns an error before retry/fail handling.
	OnSendFailure(ctx context.Context, env Envelope, err error)
	// OnRetry fires when a message is rescheduled for another attempt.
	OnRetry(ctx context.Context, env Envelope, nextAttempt int, delay time.Duration)
	// OnFail fires when a message is permanently failed.
	OnFail(ctx context.Context, env Envelope, attempts int, err error)
	// OnStoreError fires when a Store call returns an error.
	OnStoreError(ctx context.Context, op string, id int64, err error)
	// OnCycle fires once per processOnce iteration with the elapsed duration.
	OnCycle(ctx context.Context, duration time.Duration)
}

// Backoff returns the wait duration before the given attempt.
type Backoff func(attempt int) time.Duration

// Exponential creates a capped exponential backoff function.
func Exponential(base time.Duration, factor float64, max time.Duration) Backoff {
	return func(attempt int) time.Duration {
		if attempt <= 0 {
			return base
		}
		d := float64(base)
		for i := 1; i < attempt; i++ {
			d *= factor
			if time.Duration(d) >= max {
				return max
			}
		}
		delay := time.Duration(d)
		if delay > max {
			return max
		}
		if delay < base {
			return base
		}
		return delay
	}
}

// Options configure Relay behaviour and tuning knobs for workers.
type Options struct {
	// BatchSize controls how many records the relay claims per iteration.
	BatchSize int
	// LeaseTTL defines how long a claimed message stays owned before expiring.
	LeaseTTL time.Duration
	// MaxAttempts is the number of total send tries before marking as failed.
	MaxAttempts int
	// PollInterval is the sleep duration between claim cycles when no work exists.
	PollInterval time.Duration
	// Backoff computes the retry delay based on attempt count.
	Backoff Backoff
	// Logger emits structured logs for relay activity.
	Logger Logger
	// Hooks let callers plug metrics/tracing/etc. into relay events.
	Hooks Hooks
	// WorkerID identifies this relay instance in the database.
	WorkerID string
	// Now supplies the current time; override for tests or custom time sources.
	Now func() time.Time
}

func (o *Options) setDefaults() {
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 500 * time.Millisecond
	}
	if o.Backoff == nil {
		o.Backoff = Exponential(500*time.Millisecond, 2.0, 30*time.Second)
	}
	if o.Logger == nil {
		o.Logger = noopLogger{}
	}
	if o.Hooks == nil {
		o.Hooks = noopHooks{}
	}
	if o.WorkerID == "" {
		o.WorkerID = randomWorkerID()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Relay coordinates pulling messages from the store and sending them via a Sender.
type Relay struct {
	// store handles DB interactions for claiming and updating messages.
	store Store
	// sender knows how to deliver a message to the external transport.
	sender Sender
	// opts hold tuning parameters for the worker.
	opts Options
}

// NewRelay wires a Store and Sender with the provided options.
func NewRelay(store Store, sender Sender, opts Options) *Relay {
	opts.setDefaults()
	return &Relay{
		store:  store,
		sender: sender,
		opts:   opts,
	}
}

// Run processes messages until the context is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.opts.PollInterval)
	defer ticker.Stop()

	for {
		if err := r.processOnce(ctx); err != nil {
			r.opts.Logger.Error(ctx, "relay error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// processOnce claims at most BatchSize messages and attempts delivery.
func (r *Relay) processOnce(ctx context.Context) error {
	start := time.Now()
	envelopes, err := r.store.Claim(ctx, r.opts.WorkerID, r.opts.BatchSize, r.opts.LeaseTTL)
	if err != nil {
		return err
	}
	r.opts.Hooks.OnClaim(ctx, r.opts.BatchSize, len(envelopes))
	if len(envelopes) == 0 {
		r.opts.Hooks.OnCycle(ctx, time.Since(start))
		return nil
	}

	now := r.opts.Now().UTC()
	for _, env := range envelopes {
		if err := r.sender.Send(ctx, env); err != nil {
			r.opts.Hooks.OnSendFailure(ctx, env, err)
			r.handleFailure(ctx, env, err)
			continue
		}
		if err := r.store.Send(ctx, env.ID, now); err != nil {
			r.opts.Logger.Error(ctx, "mark sent failed id=%d: %v", env.ID, err)
			r.opts.Hooks.OnStoreError(ctx, "send", env.ID, err)
			continue
		}
		r.opts.Hooks.OnSendSuccess(ctx, env)
	}
	r.opts.Hooks.OnCycle(ctx, time.Since(start))
	return nil
}

// handleFailure decides whether to retry or fail a message permanently.
func (r *Relay) handleFailure(ctx context.Context, env Envelope, sendErr error) {
	attempt := env.RetryCount + 1
	if attempt >= r.opts.MaxAttempts {
		if err := r.store.Fail(ctx, env.ID, attempt); err != nil {
			r.opts.Logger.Error(ctx, "mark failed id=%d: %v (original err: %v)", env.ID, err, sendErr)
			r.opts.Hooks.OnStoreError(ctx, "fail", env.ID, err)
		} else {
			r.opts.Logger.Warn(ctx, "message %d failed permanently after %d attempts: %v", env.ID, attempt, sendErr)
			r.opts.Hooks.OnFail(ctx, env, attempt, sendErr)
		}
		return
	}
	delay := r.opts.Backoff(attempt)
	nextRetry := r.opts.Now().UTC().Add(delay)
	if err := r.store.Retry(ctx, env.ID, attempt, nextRetry); err != nil {
		r.opts.Logger.Error(ctx, "mark retry failed id=%d: %v (original err: %v)", env.ID, err, sendErr)
		r.opts.Hooks.OnStoreError(ctx, "retry", env.ID, err)
		return
	}
	r.opts.Hooks.OnRetry(ctx, env, attempt, delay)
	r.opts.Logger.Warn(ctx, "message %d scheduled for retry #%d in %s: %v", env.ID, attempt, delay, sendErr)
}

// noopLogger discards all relay logs.
type noopLogger struct{}

func (noopLogger) Info(context.Context, string, ...any)  {}
func (noopLogger) Warn(context.Context, string, ...any)  {}
func (noopLogger) Error(context.Context, string, ...any) {}

// noopHooks discards all Relay hook invocations.
type noopHooks struct{}

func (noopHooks) OnClaim(context.Context, int, int)                     {}
func (noopHooks) OnSendSuccess(context.Context, Envelope)               {}
func (noopHooks) OnSendFailure(context.Context, Envelope, error)        {}
func (noopHooks) OnRetry(context.Context, Envelope, int, time.Duration) {}
func (noopHooks) OnFail(context.Context, Envelope, int, error)          {}
func (noopHooks) OnStoreError(context.Context, string, int64, error)    {}
func (noopHooks) OnCycle(context.Context, time.Duration)                {}
