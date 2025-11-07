// Package txoutbox provides primitives for the Transactional Outbox pattern.
package txoutbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Message represents an application-level event queued inside a DB transaction.
type Message struct {
	// Topic identifies the logical event or routing destination (e.g. "order.created").
	Topic string
	// Key optionally provides a partition/idempotency key; leave empty if unused.
	Key string
	// Body is the user payload that will be marshaled to JSON.
	Body any
}

// validate ensures the minimal contract for inserting an outbox row.
func (m Message) validate() error {
	if m.Topic == "" {
		return errors.New("txoutbox: topic is required")
	}
	if m.Body == nil {
		return errors.New("txoutbox: body is required")
	}
	return nil
}

// MarshalPayload turns the body into JSON for storage.
func (m Message) MarshalPayload() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(m.Body)
	if err != nil {
		return nil, fmt.Errorf("txoutbox: failed to marshal payload: %w", err)
	}
	return payload, nil
}

// Envelope represents a row leased by the relay for delivery.
type Envelope struct {
	// ID is the primary key of the outbox row.
	ID int64
	// Topic is copied from the original Message for routing/logging.
	Topic string
	// Key is optional metadata used by senders for partitioning/idempotency.
	Key *string
	// Payload is the raw JSON message stored in the outbox.
	Payload json.RawMessage
	// RetryCount tracks how many attempts have been made (before this lease).
	RetryCount int
	// CreatedAt records when the row was inserted.
	CreatedAt time.Time
}

// Decode unmarshals the payload into the provided destination.
func (e Envelope) Decode(dest any) error {
	return json.Unmarshal(e.Payload, dest)
}
