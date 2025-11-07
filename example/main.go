package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/store/postgres"
)

// mockSender prints messages instead of delivering them to a broker.
type mockSender struct{}

func (mockSender) Send(_ context.Context, msg txoutbox.Envelope) error {
	log.Printf("send topic=%s id=%d payload=%s", msg.Topic, msg.ID, msg.Payload)
	return nil
}

func main() {
	ctx := context.Background()

	db, err := sql.Open("pgx", "postgres://postgres:password@localhost:5432/txoutbox?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	store := postgres.NewStore(db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := store.Add(ctx, tx, txoutbox.Message{
		Topic: "example.event",
		Key:   "demo",
		Body: map[string]any{
			"message": "hello, txoutbox!",
			"ts":      time.Now().UTC(),
		},
	}); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	relay := txoutbox.NewRelay(store, mockSender{}, txoutbox.Options{
		BatchSize:   10,
		LeaseTTL:    30 * time.Second,
		MaxAttempts: 3,
	})
	if err := relay.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
