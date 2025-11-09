package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mickamy/txoutbox"
)

// Sender posts envelopes to an HTTP endpoint.
type Sender struct {
	client *http.Client
	target string
}

func NewSender(target string) *Sender {
	return &Sender{
		target: target,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *Sender) Send(ctx context.Context, msg txoutbox.Envelope) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) { _ = Body.Close() }(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook responded with %s", resp.Status)
	}
	return nil
}
