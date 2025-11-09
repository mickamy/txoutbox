package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/mickamy/txoutbox"
	"github.com/mickamy/txoutbox/example/internal/database"
	"github.com/mickamy/txoutbox/stores"

	"github.com/mickamy/txoutbox/example/internal/config"
	"github.com/mickamy/txoutbox/example/internal/metrics"
	"github.com/mickamy/txoutbox/example/internal/sender/sqs"
	"github.com/mickamy/txoutbox/example/internal/sender/webhook"
)

func main() {
	ctx := context.Background()
	cfg := config.Load()

	db, err := database.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	store := stores.NewPostgresStore(db)
	hooks := metrics.NewStatsHook("txoutbox_relay")
	startMetricsServer()

	sender, err := newSender(ctx, cfg)
	if err != nil {
		log.Fatalf("init sender: %v", err)
	}

	relay := txoutbox.NewRelay(store, sender, txoutbox.Options{
		BatchSize:   50,
		LeaseTTL:    30 * time.Second,
		MaxAttempts: 5,
		Logger:      logAdapter{},
		Hooks:       hooks,
	})

	log.Printf("relay started (sender=%s)", cfg.Sender)
	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("relay stopped: %v", err)
	}
}

type logAdapter struct{}

func (logAdapter) Info(_ context.Context, format string, args ...any) {
	log.Printf("[INFO] "+format, args...)
}

func (logAdapter) Warn(_ context.Context, format string, args ...any) {
	log.Printf("[WARN] "+format, args...)
}

func (logAdapter) Error(_ context.Context, format string, args ...any) {
	log.Printf("[ERROR] "+format, args...)
}

func newSender(ctx context.Context, cfg config.Config) (txoutbox.Sender, error) {
	switch cfg.Sender {
	case "sqs":
		return sqs.NewSender(ctx, cfg.SQSEndpoint, cfg.QueueURL)
	case "webhook", "":
		return webhook.NewSender(cfg.WebhookURL), nil
	default:
		return nil, fmt.Errorf("unknown sender %q", cfg.Sender)
	}
}

func startMetricsServer() {
	const addr = ":2112"
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	go func() {
		log.Printf("metrics available at http://localhost%s/debug/vars", addr)
		if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
}
