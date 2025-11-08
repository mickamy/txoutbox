package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/store/mysql"
	"github.com/mickamy/txoutbox/test/database"
)

func TestStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	db := database.OpenMySQL(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := mysql.NewStore(db)

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
	if err := db.QueryRowContext(ctx, "SELECT status FROM txoutbox WHERE id=?", envs[0].ID).Scan(&status); err != nil {
		t.Fatalf("select status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("final status = %s, want failed", status)
	}
}

func TestStoreClaimEmpty(t *testing.T) {
	ctx := context.Background()
	db := database.OpenMySQL(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := mysql.NewStore(db)
	envs, err := store.Claim(ctx, "worker-empty", 5, time.Minute)
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if len(envs) != 0 {
		t.Fatalf("expected 0 envelopes, got %d", len(envs))
	}
}

func TestStoreClaimAfterRetry(t *testing.T) {
	ctx := context.Background()
	db := database.OpenMySQL(t)
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := mysql.NewStore(db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := store.Add(ctx, tx, txoutbox.Message{
		Topic: "order.created",
		Key:   "order-2",
		Body: map[string]any{
			"id":    2,
			"total": 42,
		},
	}); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// first claim to lock the message
	envs, err := store.Claim(ctx, "worker-lease", 1, time.Millisecond)
	if err != nil {
		t.Fatalf("Claim error: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envs))
	}

	// simulate retry by setting next_retry_at to the past
	if err := store.Retry(ctx, envs[0].ID, envs[0].RetryCount+1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("Retry error: %v", err)
	}

	envs2, err := store.Claim(ctx, "worker-lease", 1, time.Minute)
	if err != nil {
		t.Fatalf("Claim error: %v", err)
	}
	if len(envs2) != 1 {
		t.Fatalf("expected 1 envelope after retry, got %d", len(envs2))
	}
	if envs2[0].ID != envs[0].ID {
		t.Fatalf("expected to reclaim id=%d, got %d", envs[0].ID, envs2[0].ID)
	}
}
