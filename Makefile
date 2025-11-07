.PHONY: fmt lint test

fmt:
	gofmt -w -l .

lint:
	go vet ./...
	go tool staticcheck ./...

test:
	go test ./...
