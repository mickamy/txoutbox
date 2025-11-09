package txoutbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
)

func TestRelayProcessOnceSuccess(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 1, Topic: "topic"}})
	sender := &fakeSender{}
	fixed := time.Unix(1700000000, 0)
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		Now: func() time.Time {
			return fixed
		},
		PollInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, store.sendCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	if len(sender.calls) != 1 {
		t.Fatalf("sender calls = %d, want 1", len(sender.calls))
	}
	if len(store.sendCalls) != 1 {
		t.Fatalf("store.Send calls = %d, want 1", len(store.sendCalls))
	}
	if got := store.sendCalls[0].sendAt; !got.Equal(fixed.UTC()) {
		t.Fatalf("sendAt = %v, want %v", got, fixed.UTC())
	}
}

func TestRelayRetriesOnFailure(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 10, Topic: "topic", RetryCount: 1}})
	sender := &fakeSender{err: errors.New("boom")}
	fixed := time.Unix(1700000000, 0)
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		MaxAttempts: 5,
		Backoff: func(attempt int) time.Duration {
			if attempt != 2 {
				t.Fatalf("Backoff attempt = %d, want 2", attempt)
			}
			return time.Second
		},
		Now:          func() time.Time { return fixed },
		PollInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, store.retryCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	if len(store.retryCalls) != 1 {
		t.Fatalf("retry calls = %d, want 1", len(store.retryCalls))
	}
	call := store.retryCalls[0]
	if call.retryCount != 2 {
		t.Fatalf("retryCount = %d, want 2", call.retryCount)
	}
	expectedNext := fixed.UTC().Add(time.Second)
	if !call.nextRetry.Equal(expectedNext) {
		t.Fatalf("nextRetry = %v, want %v", call.nextRetry, expectedNext)
	}
}

func TestRelayFailsAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 3, Topic: "topic", RetryCount: 1}})
	sender := &fakeSender{err: errors.New("boom")}
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		MaxAttempts:  2,
		PollInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, store.failCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	if len(store.failCalls) != 1 {
		t.Fatalf("fail calls = %d, want 1", len(store.failCalls))
	}
	if store.failCalls[0].retryCount != 2 {
		t.Fatalf("fail retryCount = %d, want 2", store.failCalls[0].retryCount)
	}
	if len(store.retryCalls) != 0 {
		t.Fatalf("retry calls = %d, want 0", len(store.retryCalls))
	}
}

func TestRelayEmitsHooksOnSuccess(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 11, Topic: "topic"}})
	sender := &fakeSender{}
	hooks := &hookSpy{}
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		BatchSize:    2,
		PollInterval: 5 * time.Millisecond,
		Hooks:        hooks,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, store.sendCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.claims) == 0 {
		t.Fatal("expected at least one claim metric")
	}
	first := hooks.claims[0]
	if first.batch != 2 || first.claimed != 1 {
		t.Fatalf("claim metric = %+v, want batch=2 claimed=1", first)
	}
	if hooks.sendSuccess != 1 {
		t.Fatalf("sendSuccess = %d, want 1", hooks.sendSuccess)
	}
	if hooks.sendFailure != 0 {
		t.Fatalf("sendFailure = %d, want 0", hooks.sendFailure)
	}
	if hooks.retries != 0 || hooks.fails != 0 || len(hooks.storeErrors) != 0 {
		t.Fatalf("unexpected hook counts: %+v", hooks)
	}
	if hooks.cycles == 0 {
		t.Fatal("cycle metrics not recorded")
	}
}

func TestRelayEmitsHooksOnRetry(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 21, Topic: "topic"}})
	sender := &fakeSender{err: errors.New("boom")}
	hooks := &hookSpy{}
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		BatchSize:    1,
		MaxAttempts:  3,
		PollInterval: 5 * time.Millisecond,
		Hooks:        hooks,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, store.retryCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.sendFailure == 0 {
		t.Fatal("expected send failure metric")
	}
	if hooks.retries == 0 {
		t.Fatal("expected retry metric")
	}
	if len(hooks.storeErrors) != 0 {
		t.Fatalf("storeErrors = %d, want 0", len(hooks.storeErrors))
	}
}

func TestRelayHooksStoreError(t *testing.T) {
	t.Parallel()
	store := newFakeStore([]txoutbox.Envelope{{ID: 31, Topic: "topic"}})
	store.retryErr = errors.New("db down")
	sender := &fakeSender{
		err:    errors.New("boom"),
		sendCh: make(chan struct{}, 1),
	}
	hooks := &hookSpy{}
	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		BatchSize:    1,
		MaxAttempts:  3,
		PollInterval: 5 * time.Millisecond,
		Hooks:        hooks,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() {
		errc <- relay.Run(ctx)
	}()

	waitFor(t, sender.sendCh)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Relay.Run() error = %v, want %v", err, context.Canceled)
	}

	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if len(hooks.storeErrors) == 0 {
		t.Fatal("expected store error metric")
	}
	if hooks.retries != 0 {
		t.Fatalf("retries = %d, want 0", hooks.retries)
	}
}

type fakeSender struct {
	err    error
	calls  []txoutbox.Envelope
	sendCh chan struct{}
}

func (s *fakeSender) Send(_ context.Context, msg txoutbox.Envelope) error {
	s.calls = append(s.calls, msg)
	if s.sendCh != nil {
		select {
		case s.sendCh <- struct{}{}:
		default:
		}
	}
	return s.err
}

type fakeStore struct {
	claimQueue [][]txoutbox.Envelope

	sendErr  error
	retryErr error
	failErr  error

	sendCalls []struct {
		id     int64
		sendAt time.Time
	}
	retryCalls []struct {
		id         int64
		retryCount int
		nextRetry  time.Time
	}
	failCalls []struct {
		id         int64
		retryCount int
	}

	sendCh  chan struct{}
	retryCh chan struct{}
	failCh  chan struct{}
}

func newFakeStore(claims ...[]txoutbox.Envelope) *fakeStore {
	return &fakeStore{
		claimQueue: claims,
		sendCh:     make(chan struct{}, 1),
		retryCh:    make(chan struct{}, 1),
		failCh:     make(chan struct{}, 1),
	}
}

func (f *fakeStore) Add(context.Context, txoutbox.Executor, txoutbox.Message) error {
	return nil
}

func (f *fakeStore) Claim(context.Context, string, int, time.Duration) ([]txoutbox.Envelope, error) {
	if len(f.claimQueue) == 0 {
		return nil, nil
	}
	resp := f.claimQueue[0]
	f.claimQueue = f.claimQueue[1:]
	return resp, nil
}

func (f *fakeStore) Send(_ context.Context, id int64, sendAt time.Time) error {
	f.sendCalls = append(f.sendCalls, struct {
		id     int64
		sendAt time.Time
	}{id: id, sendAt: sendAt})
	select {
	case f.sendCh <- struct{}{}:
	default:
	}
	if f.sendErr != nil {
		return f.sendErr
	}
	return nil
}

func (f *fakeStore) Retry(_ context.Context, id int64, retryCount int, nextRetry time.Time) error {
	if f.retryErr != nil {
		return f.retryErr
	}
	f.retryCalls = append(f.retryCalls, struct {
		id         int64
		retryCount int
		nextRetry  time.Time
	}{id: id, retryCount: retryCount, nextRetry: nextRetry})
	select {
	case f.retryCh <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeStore) Fail(_ context.Context, id int64, retryCount int) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.failCalls = append(f.failCalls, struct {
		id         int64
		retryCount int
	}{id: id, retryCount: retryCount})
	select {
	case f.failCh <- struct{}{}:
	default:
	}
	return nil
}

func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel")
	}
}

type hookSpy struct {
	mu          sync.Mutex
	claims      []claimMetric
	sendSuccess int
	sendFailure int
	retries     int
	fails       int
	storeErrors []storeError
	cycles      int
}

type claimMetric struct {
	batch   int
	claimed int
}

type storeError struct {
	op string
	id int64
}

func (m *hookSpy) OnClaim(_ context.Context, batchSize int, claimed int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claims = append(m.claims, claimMetric{batch: batchSize, claimed: claimed})
}

func (m *hookSpy) OnSendSuccess(context.Context, txoutbox.Envelope) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendSuccess++
}

func (m *hookSpy) OnSendFailure(context.Context, txoutbox.Envelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendFailure++
}

func (m *hookSpy) OnRetry(context.Context, txoutbox.Envelope, int, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries++
}

func (m *hookSpy) OnFail(context.Context, txoutbox.Envelope, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fails++
}

func (m *hookSpy) OnStoreError(_ context.Context, op string, id int64, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storeErrors = append(m.storeErrors, storeError{op: op, id: id})
}

func (m *hookSpy) OnCycle(context.Context, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cycles++
}
