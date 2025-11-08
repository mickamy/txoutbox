package txoutbox_test

import (
	"testing"
	"time"

	"github.com/mickamy/txoutbox"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := txoutbox.Exponential(100*time.Millisecond, 2, time.Second)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -1, want: 100 * time.Millisecond},
		{attempt: 0, want: 100 * time.Millisecond},
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 400 * time.Millisecond},
		{attempt: 5, want: time.Second}, // capped by max
		{attempt: 10, want: time.Second},
	}

	for _, tt := range tests {
		if got := backoff(tt.attempt); got != tt.want {
			t.Fatalf("backoff(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}
