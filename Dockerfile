# syntax=docker/dockerfile:1.7
#
# Canonical multi-stage Dockerfile for bitcoin-shard-proxy.
# Final image: distroless/static:nonroot, no in-image ENV defaults
# (configure via Helm values.yaml or container env at runtime).

FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

# Module cache layer
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build
COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false \
      -ldflags "-s -w -X github.com/lightwebinc/bitcoin-shard-proxy/metrics.Version=${VERSION}" \
      -o /out/bitcoin-shard-proxy .

FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
COPY --from=builder /out/bitcoin-shard-proxy /usr/local/bin/bitcoin-shard-proxy
ENTRYPOINT ["/usr/local/bin/bitcoin-shard-proxy"]
