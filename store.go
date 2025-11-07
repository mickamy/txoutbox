package txoutbox

import (
	"context"
	"database/sql"
	"time"
)

// Executor is the minimal surface needed from *sql.Tx or *sql.DB.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Store encapsulates DB operations used by the relay.
type Store interface {
	// Add enqueues a message using the provided transaction/executor (typically *sql.Tx).
	Add(ctx context.Context, exec Executor, msg Message) error
	// Claim selects pending messages and leases them to a worker, returning envelopes to process.
	Claim(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]Envelope, error)
	// Send marks a message as successfully delivered.
	Send(ctx context.Context, id int64, sendAt time.Time) error
	// Retry releases the message to be retried later after incrementing the attempt counter.
	Retry(ctx context.Context, id int64, retryCount int, nextRetry time.Time) error
	// Fail flags the message as permanently failed so operators can inspect the row.
	Fail(ctx context.Context, id int64, retryCount int) error
}
