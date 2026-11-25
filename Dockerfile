# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
#
# Canonical multi-stage Dockerfile for shard-proxy.
# Final image: distroless/static:nonroot, no in-image ENV defaults
# (configure via Helm values.yaml or container env at runtime).

FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder
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

FROM gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5
USER nonroot:nonroot
COPY --from=builder /out/shard-proxy /usr/local/bin/shard-proxy
# The licenses travel with the image, not only with the source tree. The binary
# above is statically linked, so it contains the code of every module in
# LICENSE-THIRD-PARTY and none of their license files. Apache-2.0 section 4,
# MIT and BSD-3 all require their text to reach the recipient of a binary.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/shard-proxy/
ENTRYPOINT ["/usr/local/bin/shard-proxy"]
