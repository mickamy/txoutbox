package stores

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/internal/sqlutil"
)

type PostgresStore struct {
	db    *sql.DB
	table string
	now   func() time.Time
}

type PostgresOption func(*PostgresStore)

func WithPostgresTable(table string) PostgresOption {
	return func(s *PostgresStore) {
		if table != "" {
			s.table = table
		}
	}
}

func WithPostgresNow(now func() time.Time) PostgresOption {
	return func(s *PostgresStore) {
		if now != nil {
			s.now = now
		}
	}
}

func NewPostgresStore(db *sql.DB, opts ...PostgresOption) *PostgresStore {
	store := &PostgresStore{
		db:    db,
		table: "txoutbox",
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

func (s *PostgresStore) Add(ctx context.Context, exec txoutbox.Executor, msg txoutbox.Message) error {
	payload, err := msg.MarshalPayload()
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (topic, key, payload) VALUES ($1, $2, $3)",
		sqlutil.QuoteIdentifier(s.table, `"`),
	)
	var key any
	if msg.Key != "" {
		key = msg.Key
	}
	_, err = exec.ExecContext(ctx, query, msg.Topic, key, payload)
	return err
}

func (s *PostgresStore) Claim(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]txoutbox.Envelope, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("txoutbox: batch size must be positive")
	}
	now := s.now().UTC()
	leaseUntil := now.Add(leaseTTL)
	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT id FROM %s
    WHERE status IN ('pending','retry','sending')
      AND next_retry_at <= $1
    ORDER BY id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE %s AS o
SET status = 'sending',
    claimed_by = $3,
    claimed_at = $1,
    next_retry_at = $4
FROM candidates
WHERE o.id = candidates.id
RETURNING o.id, o.topic, o.key, o.payload, o.retry_count, o.created_at;
`, sqlutil.QuoteIdentifier(s.table, `"`), sqlutil.QuoteIdentifier(s.table, `"`))

	rows, err := s.db.QueryContext(ctx, query, now, limit, workerID, leaseUntil)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

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
			Payload:    bytes.Clone(payload),
			RetryCount: retryCount,
			CreatedAt:  createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return envelopes, nil
}

func (s *PostgresStore) Send(ctx context.Context, id int64, sendAt time.Time) error {
	query := fmt.Sprintf(
		"UPDATE %s SET status = 'sent', sent_at = $2, claimed_by = NULL, claimed_at = NULL WHERE id = $1",
		sqlutil.QuoteIdentifier(s.table, `"`),
	)
	_, err := s.db.ExecContext(ctx, query, id, sendAt)
	return err
}

func (s *PostgresStore) Retry(ctx context.Context, id int64, retryCount int, nextRetry time.Time) error {
	query := fmt.Sprintf(
		`
UPDATE %s
SET status = 'retry',
    retry_count = $2,
    next_retry_at = $3,
    claimed_by = NULL,
    claimed_at = NULL
WHERE id = $1`,
		sqlutil.QuoteIdentifier(s.table, `"`),
	)
	_, err := s.db.ExecContext(ctx, query, id, retryCount, nextRetry)
	return err
}

func (s *PostgresStore) Fail(ctx context.Context, id int64, retryCount int) error {
	query := fmt.Sprintf(
		`
UPDATE %s
SET status = 'failed',
    retry_count = $2,
    claimed_by = NULL,
    claimed_at = NULL
WHERE id = $1`,
		sqlutil.QuoteIdentifier(s.table, `"`),
	)
	_, err := s.db.ExecContext(ctx, query, id, retryCount)
	return err
}
