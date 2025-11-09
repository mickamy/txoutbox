package stores

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/internal/sqlutil"
)

type MySQLStore struct {
	db    *sql.DB
	table string
	now   func() time.Time
}

type MySQLOption func(*MySQLStore)

func WithMySQLTable(table string) MySQLOption {
	return func(s *MySQLStore) {
		if table != "" {
			s.table = table
		}
	}
}

func WithMySQLNow(now func() time.Time) MySQLOption {
	return func(s *MySQLStore) {
		if now != nil {
			s.now = now
		}
	}
}

func NewMySQLStore(db *sql.DB, opts ...MySQLOption) *MySQLStore {
	store := &MySQLStore{
		db:    db,
		table: "txoutbox",
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(store)
	}
	return store
}

func (s *MySQLStore) Add(ctx context.Context, exec txoutbox.Executor, msg txoutbox.Message) error {
	payload, err := msg.MarshalPayload()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (topic, `key`, payload) VALUES (?, ?, ?)", s.tableIdent())
	var key any
	if msg.Key != "" {
		key = msg.Key
	}
	_, err = exec.ExecContext(ctx, query, msg.Topic, key, payload)
	return err
}

func (s *MySQLStore) Claim(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]txoutbox.Envelope, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("txoutbox: batch size must be positive")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := s.now()
	ids, err := s.selectCandidateIDs(ctx, tx, limit)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	leaseUntil := now.Add(leaseTTL)
	if err := s.markSending(ctx, tx, ids, workerID, now, leaseUntil); err != nil {
		return nil, err
	}

	envelopes, err := s.fetchEnvelopes(ctx, tx, ids)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return envelopes, nil
}

func (s *MySQLStore) selectCandidateIDs(ctx context.Context, tx *sql.Tx, limit int) ([]int64, error) {
	query := fmt.Sprintf(`
SELECT id FROM %s
WHERE status IN ('pending','retry','sending')
  AND next_retry_at <= NOW(6)
ORDER BY id
LIMIT %d
FOR UPDATE SKIP LOCKED`, s.tableIdent(), limit)
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *MySQLStore) markSending(ctx context.Context, tx *sql.Tx, ids []int64, workerID string, claimedAt, leaseUntil time.Time) error {
	query := fmt.Sprintf(`
UPDATE %s
SET status = 'sending',
    claimed_by = ?,
    claimed_at = ?,
    next_retry_at = ?
WHERE id IN (%s)`, s.tableIdent(), placeholders(len(ids)))
	args := []any{workerID, claimedAt, leaseUntil}
	for _, id := range ids {
		args = append(args, id)
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func (s *MySQLStore) fetchEnvelopes(ctx context.Context, tx *sql.Tx, ids []int64) ([]txoutbox.Envelope, error) {
	query := fmt.Sprintf(`
SELECT id, topic, `+"`key`"+`, payload, retry_count, created_at
FROM %s
WHERE id IN (%s)`, s.tableIdent(), placeholders(len(ids)))

	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
		var keyPtr *string
		if key.Valid {
			val := key.String
			keyPtr = &val
		}
		envelopes = append(envelopes, txoutbox.Envelope{
			ID:         id,
			Topic:      topic,
			Key:        keyPtr,
			Payload:    append([]byte(nil), payload...),
			RetryCount: retryCount,
			CreatedAt:  createdAt,
		})
	}
	return envelopes, rows.Err()
}

func (s *MySQLStore) Send(ctx context.Context, id int64, sendAt time.Time) error {
	query := fmt.Sprintf("UPDATE %s SET status='sent', sent_at=?, claimed_by=NULL, claimed_at=NULL WHERE id=?", s.tableIdent())
	_, err := s.db.ExecContext(ctx, query, sendAt, id)
	return err
}

func (s *MySQLStore) Retry(ctx context.Context, id int64, retryCount int, nextRetry time.Time) error {
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

func (s *MySQLStore) Fail(ctx context.Context, id int64, retryCount int) error {
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

func (s *MySQLStore) tableIdent() string {
	return sqlutil.QuoteIdentifier(s.table, "`")
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}
