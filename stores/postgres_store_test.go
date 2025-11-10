package stores_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/stores"
	"github.com/mickamy/txoutbox/test/database"
)

func TestPostgresStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	db := database.OpenPostgres(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := stores.NewPostgres(db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := store.Add(ctx, tx, txoutbox.Message{
		Topic: "order.created",
		Key:   "order-1",
		Body: map[string]any{
			"id":    1,
			"total": 100,
		},
	}); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	envs, err := store.Claim(ctx, "worker-1", 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim error: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envs))
	}

	if err := store.Retry(ctx, envs[0].ID, envs[0].RetryCount+1, time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("Retry error: %v", err)
	}
	if err := store.Fail(ctx, envs[0].ID, envs[0].RetryCount+2); err != nil {
		t.Fatalf("Fail error: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, "SELECT status FROM txoutbox WHERE id=$1", envs[0].ID).Scan(&status); err != nil {
		t.Fatalf("select status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("final status = %s, want failed", status)
	}
}

func TestPostgresStoreClaimAllowsExpiredLeases(t *testing.T) {
	ctx := context.Background()
	db := database.OpenPostgres(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := stores.NewPostgres(db)
	seedPostgresMessages(t, ctx, db, 1)

	firstClaim, err := store.Claim(ctx, "worker-initial", 1, time.Minute)
	if err != nil {
		t.Fatalf("initial Claim error: %v", err)
	}
	if len(firstClaim) != 1 {
		t.Fatalf("expected 1 envelope on first claim, got %d", len(firstClaim))
	}

	if _, err := db.ExecContext(ctx,
		`UPDATE txoutbox SET next_retry_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
		firstClaim[0].ID,
	); err != nil {
		t.Fatalf("set next_retry_at: %v", err)
	}

	secondClaim, err := store.Claim(ctx, "worker-reclaim", 1, time.Minute)
	if err != nil {
		t.Fatalf("second Claim error: %v", err)
	}
	if len(secondClaim) != 1 {
		t.Fatalf("expected 1 envelope after lease expiry, got %d", len(secondClaim))
	}
	if secondClaim[0].ID != firstClaim[0].ID {
		t.Fatalf("expected to reclaim id=%d, got %d", firstClaim[0].ID, secondClaim[0].ID)
	}
}

func TestPostgresStoreClaimConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	db := database.OpenPostgres(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	const (
		totalMessages = 6
		workers       = 3
		batchSize     = 2
	)

	store := stores.NewPostgres(db)
	seedPostgresMessages(t, ctx, db, totalMessages)

	start := make(chan struct{})
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[int64]struct{})
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			envs, err := store.Claim(ctx, fmt.Sprintf("worker-%d", worker), batchSize, time.Minute)
			if err != nil {
				t.Errorf("Claim worker-%d: %v", worker, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, env := range envs {
				if _, exists := claimed[env.ID]; exists {
					t.Errorf("duplicate claim id=%d", env.ID)
					continue
				}
				claimed[env.ID] = struct{}{}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(claimed) != totalMessages {
		t.Fatalf("claimed %d messages, want %d", len(claimed), totalMessages)
	}
}

func seedPostgresMessages(t *testing.T, ctx context.Context, db *sql.DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf(`{"id":%d}`, i)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO txoutbox (topic, payload) VALUES ($1, $2::jsonb)`,
			"order.created", payload,
		); err != nil {
			t.Fatalf("insert message %d: %v", i, err)
		}
	}
}
