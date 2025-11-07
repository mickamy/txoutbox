package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
	mysqlstore "github.com/mickamy/txoutbox/store/mysql"
	testdb "github.com/mickamy/txoutbox/test/database"
)

func TestStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testdb.OpenMySQL(t)

	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS txoutbox (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  topic VARCHAR(255) NOT NULL,
  `+"`key`"+` VARCHAR(255),
  payload JSON NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  retry_count INT NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  claimed_by VARCHAR(255),
  claimed_at TIMESTAMP NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at TIMESTAMP NULL
);`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, _ = db.ExecContext(ctx, `TRUNCATE txoutbox`)

	store := mysqlstore.NewStore(db)

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
