package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		defer func(Body io.ReadCloser) { _ = Body.Close() }(r.Body)
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("webhook received: %s", mustJSON(payload))
		w.WriteHeader(http.StatusNoContent)
	})

	log.Println("webhook listening on :8081")
	if err := http.ListenAndServe(":8081", mux); err != nil {
		log.Fatalf("webhook server failed: %v", err)
	}
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "<invalid json>"
	}
	return string(data)
}
