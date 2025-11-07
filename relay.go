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
	envelopes, err := r.store.Claim(ctx, r.opts.WorkerID, r.opts.BatchSize, r.opts.LeaseTTL)
	if err != nil {
		return err
	}
	if len(envelopes) == 0 {
		return nil
	}

	now := r.opts.Now().UTC()
	for _, env := range envelopes {
		if err := r.sender.Send(ctx, env); err != nil {
			r.handleFailure(ctx, env, err)
			continue
		}
		if err := r.store.Send(ctx, env.ID, now); err != nil {
			r.opts.Logger.Error(ctx, "mark sent failed id=%d: %v", env.ID, err)
		}
	}
	return nil
}

// handleFailure decides whether to retry or fail a message permanently.
func (r *Relay) handleFailure(ctx context.Context, env Envelope, sendErr error) {
	attempt := env.RetryCount + 1
	if attempt >= r.opts.MaxAttempts {
		if err := r.store.Fail(ctx, env.ID, attempt); err != nil {
			r.opts.Logger.Error(ctx, "mark failed id=%d: %v (original err: %v)", env.ID, err, sendErr)
		} else {
			r.opts.Logger.Warn(ctx, "message %d failed permanently after %d attempts: %v", env.ID, attempt, sendErr)
		}
		return
	}
	delay := r.opts.Backoff(attempt)
	nextRetry := r.opts.Now().UTC().Add(delay)
	if err := r.store.Retry(ctx, env.ID, attempt, nextRetry); err != nil {
		r.opts.Logger.Error(ctx, "mark retry failed id=%d: %v (original err: %v)", env.ID, err, sendErr)
		return
	}
	r.opts.Logger.Warn(ctx, "message %d scheduled for retry #%d in %s: %v", env.ID, attempt, delay, sendErr)
}

// noopLogger discards all relay logs.
type noopLogger struct{}

// Info implements Logger.
func (noopLogger) Info(context.Context, string, ...any) {}

// Warn implements Logger.
func (noopLogger) Warn(context.Context, string, ...any) {}

// Error implements Logger.
func (noopLogger) Error(context.Context, string, ...any) {}
