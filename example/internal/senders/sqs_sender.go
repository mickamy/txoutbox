package senders

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/mickamy/txoutbox"
	internalSQS "github.com/mickamy/txoutbox/example/internal/lib/aws/sqs"
)

// SQSSender pushes envelopes to an SQS queue (works with LocalStack).
type SQSSender struct {
	queueURL string
	client   *sqs.Client
}

// NewSQS creates an SQS client targeting the given endpoint and queue.
func NewSQS(ctx context.Context, endpointURL, queueURL string) (*SQSSender, error) {
	client, err := internalSQS.New(ctx, endpointURL)
	if err != nil {
		return nil, err
	}
	return &SQSSender{
		queueURL: queueURL,
		client:   client,
	}, nil
}

// Send implements txoutbox.Sender by posting the raw JSON payload to SQS.
func (s *SQSSender) Send(ctx context.Context, msg txoutbox.Envelope) error {
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
