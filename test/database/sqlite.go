package database

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// OpenSQLite returns an in-memory SQLite DB with the txoutbox table ensured.
func OpenSQLite(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:txoutbox_%d?mode=memory&cache=shared&_foreign_keys=on", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping sqlite: %v", err)
	}
	schema := `CREATE TABLE IF NOT EXISTS txoutbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        key TEXT,
        payload BLOB NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        retry_count INTEGER NOT NULL DEFAULT 0,
        next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        claimed_by TEXT,
        claimed_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        sent_at TIMESTAMP
    );`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM txoutbox`); err != nil {
		t.Fatalf("truncate txoutbox: %v", err)
	}
	return db
}
