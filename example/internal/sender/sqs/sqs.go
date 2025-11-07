package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/mickamy/txoutbox"
)

// Sender pushes envelopes to an SQS queue (works with LocalStack).
type Sender struct {
	queueURL string
	client   *sqs.Client
}

// NewSender creates an SQS client targeting the given endpoint and queue.
func NewSender(ctx context.Context, endpointURL, queueURL string) (*Sender, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
	})

	return &Sender{
		queueURL: queueURL,
		client:   client,
	}, nil
}

// Send implements txoutbox.Sender by posting the raw JSON payload to SQS.
func (s *Sender) Send(ctx context.Context, msg txoutbox.Envelope) error {
	body, err := json.Marshal(struct {
		Topic   string          `json:"topic"`
		Key     *string         `json:"key,omitempty"`
		Payload json.RawMessage `json:"payload"`
	}{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Payload: msg.Payload,
	})
	if err != nil {
		return err
	}

	_, err = s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.queueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}
