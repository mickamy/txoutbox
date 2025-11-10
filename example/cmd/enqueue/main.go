package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/stores"

	"github.com/mickamy/txoutbox/example/internal/config"
	"github.com/mickamy/txoutbox/example/internal/database"
)

type order struct {
	ID        string    `json:"id"`
	Total     float64   `json:"total"`
	Currency  string    `json:"currency"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	ctx := context.Background()
	cfg := config.Load()

	db, err := database.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	o := order{
		ID:        newID(),
		Total:     99.99,
		Currency:  "USD",
		CreatedAt: time.Now().UTC(),
	}

	if err := insertOrder(ctx, db, o); err != nil {
		log.Fatalf("insert order: %v", err)
	}

	store := stores.NewPostgres(db)
	if err := enqueue(ctx, store, db, o); err != nil {
		log.Fatalf("enqueue outbox: %v", err)
	}
	log.Printf("enqueued order %s", o.ID)
}

func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "order-unknown"
	}
	return hex.EncodeToString(b[:])
}

func insertOrder(ctx context.Context, db *sql.DB, o order) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO orders (id, total, currency, created_at) VALUES ($1,$2,$3,$4)`,
		o.ID, o.Total, o.Currency, o.CreatedAt)
	return err
}

func enqueue(ctx context.Context, store txoutbox.Store, db *sql.DB, o order) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	payload, err := json.Marshal(o)
	if err != nil {
		return err
	}

	if err := store.Add(ctx, tx, txoutbox.Message{
		Topic: "order.created",
		Key:   o.ID,
		Body:  json.RawMessage(payload),
	}); err != nil {
		return err
	}
	return tx.Commit()
}
