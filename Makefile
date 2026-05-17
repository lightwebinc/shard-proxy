BINARY := bitcoin-shard-proxy
SEND   := send-test-frames
RECV   := recv-test-frames
PERF   := perf-test

.PHONY: all test test-e2e lint hooks clean

all: $(BINARY) $(SEND) $(RECV) $(PERF)

$(BINARY):
	go build -o $(BINARY) .

$(SEND):
	go build -o $(SEND) ./cmd/send-test-frames/

$(RECV):
	go build -o $(RECV) ./cmd/recv-test-frames/

$(PERF):
	go build -o $(PERF) ./cmd/perf-test/

test:
	go test ./...

test-e2e: $(BINARY) $(SEND) $(RECV)
	PATH="$(CURDIR):$$PATH" sh test/run-e2e.sh

lint:
	golangci-lint run ./...

hooks:
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed (git config core.hooksPath .githooks)"

clean:
	rm -f $(BINARY) $(SEND) $(RECV) $(PERF)
