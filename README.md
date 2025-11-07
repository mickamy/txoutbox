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

3. **Run the richer example (`example/` module)**  
   After `docker compose up`, use the commands below in separate terminals:
   ```bash
   cd example
   go run ./cmd/webhook                   # receives POSTs on :8081/events and logs them
   go run ./cmd/enqueue                   # seeds an order row and queues an outbox message
   go run ./cmd/relay                     # leases messages and POSTs them to the webhook
   SENDER=sqs \
     QUEUE_URL=http://localhost:4566/000000000000/worker-queue \
     go run ./cmd/relay                   # alternative: send to LocalStack SQS
   SENDER=sqs \
     QUEUE_URL=http://localhost:4566/000000000000/worker-queue \
     go run ./cmd/consumer                # optional: drain SQS messages and log them
   ```
    - `POSTGRES_DSN` (default `postgres://postgres:password@localhost:5432/txoutbox?sslmode=disable`) controls database
      access.
    - `SENDER` chooses the dispatcher (`webhook` default, `sqs` for LocalStack). Use `WEBHOOK_URL` or `SQS_ENDPOINT`/
      `QUEUE_URL` to point at your infra.
    - `cmd/enqueue` creates an `orders` table (if needed), writes a fake order, and calls `store/postgres.Add` inside a
      transaction.
    - `cmd/relay` runs the shared relay with either the HTTP sender or the SQS sender so you can observe leasing/retries
      interacting with downstream processes (`cmd/webhook` or LocalStack SQS).
