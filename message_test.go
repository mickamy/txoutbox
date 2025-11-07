package txoutbox_test

import (
	"errors"
	"testing"

	"github.com/mickamy/txoutbox"
)

func TestMessageMarshalPayload(t *testing.T) {
	t.Parallel()
	msg := txoutbox.Message{
		Topic: "order.created",
		Body:  struct{ ID int }{ID: 42},
	}
	payload, err := msg.MarshalPayload()
	if err != nil {
		t.Fatalf("MarshalPayload returned error: %v", err)
	}
	expected := `{"ID":42}`
	if string(payload) != expected {
		t.Fatalf("MarshalPayload() = %s, want %s", string(payload), expected)
	}
}

func TestMessageMarshalPayloadValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		msg     txoutbox.Message
		wantErr error
	}{
		{
			name:    "missing topic",
			msg:     txoutbox.Message{Body: struct{}{}},
			wantErr: errors.New("txoutbox: topic is required"),
		},
		{
			name:    "missing body",
			msg:     txoutbox.Message{Topic: "order.created"},
			wantErr: errors.New("txoutbox: body is required"),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := tt.msg.MarshalPayload(); err == nil || err.Error() != tt.wantErr.Error() {
				t.Fatalf("MarshalPayload() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
