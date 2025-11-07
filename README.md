# txoutbox

Golang utilities for the Transactional Outbox pattern. Write messages inside your business transaction, then drain them
asynchronously with a relay that handles retries, leasing, and backoff.

## Features

- **Store / Sender separation**: queue messages with any SQL database, dispatch through pluggable senders (
  Kafka/SQS/Webhook/etc.).
- **Relay with leasing**: avoids duplicate deliveries via `Claim` + `LeaseTTL`, retries with configurable backoff and
  attempt limits.
- **DB-specific packages**: root module exposes the interfaces, while `store/postgres` (and future `store/mysql`,
  `store/sqlite`, …) bring their own SQL.
- **Observability-ready hooks**: leveled `Logger` interface, context propagation, and overridable clock (`Options.Now`)
  for deterministic tests.
- **Docker playground**: `compose.yaml` runs PostgreSQL + LocalStack SQS so you can try the flow locally.

## Quick Start

1. **Install**
   ```bash
   go get github.com/mickamy/txoutbox
   ```

2. **Create the table** – an example PostgreSQL schema exists under `postgres/init.sql`:
   ```sql
   CREATE TABLE txoutbox (
     id            BIGSERIAL PRIMARY KEY,
     topic         TEXT        NOT NULL,
     key           TEXT,
     payload       JSONB       NOT NULL,
     status        TEXT        NOT NULL DEFAULT 'pending',
     retry_count   INT         NOT NULL DEFAULT 0,
     next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     claimed_by    TEXT,
     claimed_at    TIMESTAMPTZ,
     created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     sent_at       TIMESTAMPTZ
   );
   ```

3. **See the runnable example (`example/main.go`)**
   ```go
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
     if err := enqueue(ctx, store, db); err != nil {
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

   func enqueue(ctx context.Context, store txoutbox.Store, db *sql.DB) error {
     tx, err := db.BeginTx(ctx, nil)
     if err != nil {
       return err
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
       return err
     }
     return tx.Commit()
   }
   ```
   Run `cd example && go run .` (after `docker compose up`) to see a complete transaction + relay loop.
