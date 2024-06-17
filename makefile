all: .format

.PHONY: all

.format:
	go mod tidy
	golint ./...
	gofmt -w .
	goimports -w .
	go vet ./...
	gonote ./...