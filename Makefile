BINARY := shard-proxy
SEND   := send-test-frames
RECV   := recv-test-frames
PERF   := perf-test

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
TAG     ?= $(VERSION)
IMAGE   ?= ghcr.io/lightwebinc/$(BINARY)
COMMON  ?= ../shard-common

# Dagger pipeline lives in its own module under ci/. GOWORK=off avoids
# pulling the parent go.work into the ci/ build.
DAGGER_RUN := GOWORK=off go run .

.PHONY: all build test test-e2e lint hooks clean \
        ci ci-unit ci-lint ci-vuln ci-tidy ci-build ci-image ci-export ci-publish ci-shell \
        fmt help

all: build

build: $(BINARY) $(SEND) $(RECV) $(PERF)

$(BINARY):
	go build -o $(BINARY) .

$(SEND):
	go build -o $(SEND) ./cmd/send-test-frames/

$(RECV):
	go build -o $(RECV) ./cmd/recv-test-frames/

$(PERF):
	go build -o $(PERF) ./cmd/perf-test/

test:                  ## go test ./... (host)
	go test ./...

test-e2e: $(BINARY) $(SEND) $(RECV)  ## host-side end-to-end smoke
	PATH="$(CURDIR):$$PATH" sh test/run-e2e.sh

lint:                  ## golangci-lint on host
	golangci-lint run ./...

hooks:                 ## install git pre-commit hook
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed (git config core.hooksPath .githooks)"

clean:
	rm -f $(BINARY) $(SEND) $(RECV) $(PERF)
	rm -rf build

# --- Dagger CI (containerised, reproducible) ---

ci:                    ## full pipeline: tidy + lint + vuln + unit + build + image
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) all

ci-unit:               ## go test -race ./... inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) unit

ci-lint:               ## go vet + golangci-lint inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) lint

ci-vuln:               ## govulncheck inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) vuln

ci-tidy:               ## go mod tidy diff check inside Dagger
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) tidy

ci-build:              ## go build ./... inside Dagger (no image)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) build

ci-image:              ## build OCI image (cached only)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) image

ci-export:             ## export image to build/$(BINARY)-$(TAG).tar
	@mkdir -p build
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) \
	  -export=../build/$(BINARY)-$(TAG).tar image

ci-publish:            ## publish image to $(IMAGE):$(TAG)
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) -version=$(VERSION) \
	  -address=$(IMAGE):$(TAG) image

ci-shell:              ## interactive shell in the builder container
	cd ci && $(DAGGER_RUN) -src=.. -common=../$(COMMON) dev-shell

fmt:                   ## gofmt -w
	gofmt -w .

help:                  ## list targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
