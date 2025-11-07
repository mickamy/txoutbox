package config

import "os"

// Config holds runtime settings for the sample app.
type Config struct {
	PostgresDSN string
	WebhookURL  string
	SQSEndpoint string
	QueueURL    string
	Sender      string
}

// Load reads env vars and returns Config with sensible defaults for docker-compose.
func Load() Config {
	cfg := Config{
		PostgresDSN: "postgres://postgres:password@localhost:5432/txoutbox?sslmode=disable",
		WebhookURL:  "http://localhost:8081/events",
		SQSEndpoint: "http://localhost:4566",
		QueueURL:    "http://localhost:4566/000000000000/worker-queue",
		Sender:      "webhook",
	}
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.PostgresDSN = v
	}
	if v := os.Getenv("WEBHOOK_URL"); v != "" {
		cfg.WebhookURL = v
	}
	if v := os.Getenv("SQS_ENDPOINT"); v != "" {
		cfg.SQSEndpoint = v
	}
	if v := os.Getenv("QUEUE_URL"); v != "" {
		cfg.QueueURL = v
	}
	if v := os.Getenv("SENDER"); v != "" {
		cfg.Sender = v
	}
	return cfg
}
