package stores_test

import (
	"context"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/stores"
	"github.com/mickamy/txoutbox/test/database"
)

func TestSQLiteStoreLifecycle(t *testing.T) {
	t.Parallel()
	db := database.OpenSQLite(t)
	ctx := context.Background()

	store := stores.NewSQLiteStore(db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	msg := txoutbox.Message{Topic: "sqlite.event", Key: "id-1", Body: map[string]any{"foo": "bar"}}
	if err := store.Add(ctx, tx, msg); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	envs, err := store.Claim(ctx, "worker-sqlite", 5, time.Minute)
	if err != nil {
		t.Fatalf("Claim error: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envs))
	}

	var payload map[string]string
	if err := envs[0].Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["foo"] != "bar" {
		t.Fatalf("payload mismatch: %+v", payload)
	}

	if err := store.Retry(ctx, envs[0].ID, envs[0].RetryCount+1, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Retry error: %v", err)
	}
	if err := store.Fail(ctx, envs[0].ID, envs[0].RetryCount+2); err != nil {
		t.Fatalf("Fail error: %v", err)
	}
}

func TestSQLiteStoreClaimEmpty(t *testing.T) {
	t.Parallel()
	db := database.OpenSQLite(t)
	ctx := context.Background()

	store := stores.NewSQLiteStore(db)
	envs, err := store.Claim(ctx, "worker", 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim error: %v", err)
	}
	if len(envs) != 0 {
		t.Fatalf("expected 0 envelopes, got %d", len(envs))
	}
}
