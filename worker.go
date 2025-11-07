package txoutbox

import (
	"crypto/rand"
	"encoding/hex"
)

// randomWorkerID generates a short identifier for logging/claiming rows.
func randomWorkerID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "worker-unknown"
	}
	return "worker-" + hex.EncodeToString(buf[:])
}
