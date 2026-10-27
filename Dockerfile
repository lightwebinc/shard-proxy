# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Canonical multi-stage Dockerfile for shard-proxy.
# Final image: distroless/static:nonroot, no in-image ENV defaults
# (configure via Helm values.yaml or container env at runtime).

FROM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS builder
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
      -ldflags "-s -w -X github.com/lightwebinc/shard-proxy/metrics.Version=${VERSION}" \
      -o /out/shard-proxy .

FROM gcr.io/distroless/static:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
USER nonroot:nonroot
COPY --from=builder /out/shard-proxy /usr/local/bin/shard-proxy
ENTRYPOINT ["/usr/local/bin/shard-proxy"]
