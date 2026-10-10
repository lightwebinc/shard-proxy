# shard-proxy

[![CI](https://github.com/lightwebinc/shard-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/shard-proxy/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/shard-proxy/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/shard-proxy/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/shard-proxy)](https://github.com/lightwebinc/shard-proxy/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/shard-proxy.svg)](https://pkg.go.dev/github.com/lightwebinc/shard-proxy)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project — see the main repository for the full architecture, design docs, and BRC specifications.

A high-throughput proxy that receives Bitcoin SV (BSV Blockchain) transactions
(BRC-124/BRC-128 or legacy BRC-12 frames, or bare header-stripped transactions)
and BRC-148/149 BEEF submission records over UDP (or TCP for reliable
delivery), derives an IPv6 multicast group address from the transaction ID (or
the BEEF TopicID on the object plane), and retransmits to subscribers of the
corresponding group. Further traffic segmentation is provided
via subtree-level sharding. Reliable delivery to multicast receivers is supported
via monotonic transmission flow sequencing. The TCP ingress also forwards
BRC-127 SubtreeGroupAnnounce datagrams to the control-plane multicast group.
Opt-in BRC-142 coalescing packs many small transactions into a single bundle
datagram at the origin edge to cut egress packets-per-second.

Inspiration: [Multicast within Multicast: Anycast](https://singulargrit.substack.com/p/multicast-within-multicast-anycast), [Multicast as the Only Viable Architecture](https://singulargrit.substack.com/p/multicast-as-the-only-viable-architecture)

```text
sender  ──UDP/TCP──►  shard-proxy  ──UDP multicast──►  FF05::B:<shard>  (iface 0)
                      (forwarder pipeline) └─────────────────►  FF05::B:<shard>  (iface 1)
                                                                 (subset of subscribers)
```

Ingress is transaction-only; the miner multicast port is deprecated (see
[Ingress is transaction-only](docs/configuration.md#ingress-is-transaction-only-miner-port-deprecated)).

## Quick start

```bash
make
./shard-proxy \
  -iface            eth0 \
  -shard-bits       8    \
  -scope            site \
  -udp-listen-port  8725 \
  -tcp-listen-port  8725 \
  -egress-port      9001
```

More invocations (SSM, BRC-142 coalescing, BRC-139 auto-shard-config, JSON
logging, graceful drain) are in
[docs/configuration.md § Example Invocations](docs/configuration.md#example-invocations).

## Build

```bash
make            # builds shard-proxy, send-test-frames, recv-test-frames, perf-test
make test       # runs unit tests
make test-e2e   # end-to-end test (builds all binaries, runs test/run-e2e.sh)
make clean      # removes built binaries
```

`cmd/latency-sink` (one-way latency receiver for `perf-test -latency-stamp`)
is built with `go build ./cmd/latency-sink`.

## Requirements

- Go 1.26 or later (`go.mod` floor: 1.26.2)
- Linux kernel 3.9+, FreeBSD 12.3+ (for `SO_REUSEPORT`), MacOS
- IPv6 enabled on the egress interface(s)
- Multicast routing / MLD snooping configured for your subscriber fabric

## Layout

```
main.go               — entry point
config/               — flags and environment variables
forwarder/            — ingress, stamping, egress pipeline
worker/               — per-CPU UDP workers and TCP / push-lane ingress listeners
manifest/             — BRC-139 manifest consumer (auto-shard-config)
metrics/              — Prometheus/OTel metrics, health endpoints
cmd/                  — test and perf tools (send/recv-test-frames, perf-test, latency-sink)
test/                 — end-to-end test harness
docs/                 — architecture, configuration, metrics
```

## Documentation

- [Architecture](docs/architecture.md) — system overview, multi-CPU design, graceful shutdown, BRC-139 manifest consumer, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, ingress modes, drain timeout, example invocations
- [Metrics Reference](docs/metrics.md) — every exported `bsp_` series
- [Protocol specification](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md)

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `bundle`, `objfmt`, `shard`, `seqhash`, `pow`, `cache`, `txidset`, `netjoin`, `manifest`, `logging`, `hostinfo`, `tracing` packages

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with the
single static binary at `/usr/local/bin/shard-proxy`. No in-image
`ENV` defaults are set — configure via Helm `values.yaml`, container
environment variables, or CLI flags.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`charts/shard-proxy`](https://github.com/lightwebinc/charts/tree/main/charts/shard-proxy)
- Install: `helm install shard-proxy oci://ghcr.io/lightwebinc/charts/shard-proxy`

Flags are exposed under `.config` in the chart's `values.yaml` — see the chart README for the covered set and `values.schema.json` for validation rules.

## Releases

Releases and release notes live on [GitHub Releases](https://github.com/lightwebinc/shard-proxy/releases); there is no CHANGELOG.

## License

Apache 2.0 - See LICENSE file.
