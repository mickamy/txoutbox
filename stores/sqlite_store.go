package stores

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/internal/sqlutil"
)

// SQLiteStore implements Store for SQLite databases.
type SQLiteStore struct {
	db    *sql.DB
	table string
	now   func() time.Time
}

// SQLiteOption configures a SQLiteStore.
type SQLiteOption func(*SQLiteStore)

// WithSQLiteTable overrides the default table name ("txoutbox").
func WithSQLiteTable(name string) SQLiteOption {
	return func(s *SQLiteStore) {
		if name != "" {
			s.table = name
		}
	}
}

// WithSQLiteNow overrides the clock used for Lease timestamps.
func WithSQLiteNow(now func() time.Time) SQLiteOption {
	return func(s *SQLiteStore) {
		if now != nil {
			s.now = now
		}
	}
}

// NewSQLiteStore creates a Store backed by SQLite.
func NewSQLiteStore(db *sql.DB, opts ...SQLiteOption) *SQLiteStore {
	store := &SQLiteStore{
		db:    db,
		table: "txoutbox",
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

// Add inserts a new message row within the caller's transaction.
func (s *SQLiteStore) Add(ctx context.Context, exec txoutbox.Executor, msg txoutbox.Message) error {
	payload, err := msg.MarshalPayload()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (topic, key, payload) VALUES (?, ?, ?)", s.tableIdent())
	var key any
	if msg.Key != "" {
		key = msg.Key
	}
	_, err = exec.ExecContext(ctx, query, msg.Topic, key, payload)
	return err
}

// Claim leases up to limit rows for the given worker.
func (s *SQLiteStore) Claim(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]txoutbox.Envelope, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("txoutbox: batch size must be positive")
	}
	now := s.now().UTC()
	leaseUntil := now.Add(leaseTTL)
	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT id FROM %s
    WHERE status IN ('pending','retry','sending')
      AND next_retry_at <= ?
    ORDER BY id
    LIMIT ?
)
UPDATE %s
SET status = 'sending',
    claimed_by = ?,
    claimed_at = ?,
    next_retry_at = ?
WHERE id IN (SELECT id FROM candidates)
RETURNING id, topic, key, payload, retry_count, created_at;`, s.tableIdent(), s.tableIdent())

	rows, err := s.db.QueryContext(ctx, query, now, limit, workerID, now, leaseUntil)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)

	var envelopes []txoutbox.Envelope
	for rows.Next() {
		var (
			id         int64
			topic      string
			key        sql.NullString
			payload    []byte
			retryCount int
			createdAt  time.Time
		)
		if err := rows.Scan(&id, &topic, &key, &payload, &retryCount, &createdAt); err != nil {
			return nil, err
		}
		envelopes = append(envelopes, txoutbox.Envelope{
			ID:         id,
			Topic:      topic,
			Key:        sqlutil.NullableString(key),
			Payload:    append([]byte(nil), payload...),
			RetryCount: retryCount,
			CreatedAt:  createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return envelopes, nil
}

// Send marks the row successful.
func (s *SQLiteStore) Send(ctx context.Context, id int64, sendAt time.Time) error {
	query := fmt.Sprintf("UPDATE %s SET status='sent', sent_at=?, claimed_by=NULL, claimed_at=NULL WHERE id=?", s.tableIdent())
	_, err := s.db.ExecContext(ctx, query, sendAt, id)
	return err
}

// Retry schedules the row for another attempt.
func (s *SQLiteStore) Retry(ctx context.Context, id int64, retryCount int, nextRetry time.Time) error {
	query := fmt.Sprintf(`
UPDATE %s
SET status='retry',
    retry_count=?,
    next_retry_at=?,
    claimed_by=NULL,
    claimed_at=NULL
WHERE id=?`, s.tableIdent())
	_, err := s.db.ExecContext(ctx, query, retryCount, nextRetry, id)
	return err
}

// Fail marks the row permanently failed.
func (s *SQLiteStore) Fail(ctx context.Context, id int64, retryCount int) error {
	query := fmt.Sprintf(`
UPDATE %s
SET status='failed',
    retry_count=?,
    claimed_by=NULL,
    claimed_at=NULL
WHERE id=?`, s.tableIdent())
	_, err := s.db.ExecContext(ctx, query, retryCount, id)
	return err
}

func (s *SQLiteStore) tableIdent() string {
	return sqlutil.QuoteIdentifier(s.table, `"`)
}
