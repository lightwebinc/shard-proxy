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

## Documentation

- [Architecture](docs/architecture.md) — system overview, multi-CPU design, graceful shutdown, BRC-139 manifest consumer, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, ingress modes, drain timeout

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `bundle`, `objfmt`, `shard`, `seqhash`, `pow`, `cache`, `txidset`, `netjoin`, `manifest`, `logging`, `hostinfo`, `tracing` packages

## Requirements

- Go 1.26 or later (`go.mod` floor: 1.26.2)
- Linux kernel 3.9+, FreeBSD 12.3+ (for `SO_REUSEPORT`), MacOS
- IPv6 enabled on the egress interface(s)
- Multicast routing / MLD snooping configured for your subscriber fabric
- Bitcoin SV ingress transaction packets in BRC-12 (legacy) or BRC-124/BRC-128 frame format.

## Build

```bash
make            # builds shard-proxy, send-test-frames, recv-test-frames, perf-test
make test       # runs unit tests
make test-e2e   # end-to-end test (builds all binaries, runs test/run-e2e.sh)
make clean      # removes built binaries
```

`cmd/latency-sink` (a multicast receiver that reports one-way latency
percentiles from `perf-test -latency-stamp` senders) is built directly with
`go build ./cmd/latency-sink`.

## Run

```bash
./shard-proxy \
  -iface            eth0 \
  -shard-bits       8    \
  -scope            site \
  -udp-listen-port  8725 \
  -egress-port      9001
```

With TCP ingress enabled:

```bash
./shard-proxy \
  -iface            eth0 \
  -udp-listen-port  8725 \
  -tcp-listen-port  8725
```

Ingress is **transaction-only** at the component boundary: port 8725 accepts
BRC-12/124/128 transactions, framed or bare (an anchor is an ordinary
transaction), plus BRC-148 BEEF submission records and FrameVer `0x09` frames
(an open class; `-beef-listen-port`, standard 8728, is an optional dedicated
BEEF lane for flow separation only). The old
privileged **miner multicast port was deprecated (2026-07-07)** — blocks and
subtrees are no longer submitted as multicast frames. They enter only as
BRC-144 (block) / BRC-143 (subtree) push frames on the proxy's tunnel-bound
push ports; multicast is fabric-internal transport. See the
[design direction](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#ingress-authorization-miner-tier-gate).

With Source-Specific Multicast (RFC 4607) — see [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm):

```bash
./shard-proxy \
  -iface            eth0 \
  -shard-bits       2 \
  -scope            site \
  -source-mode      ssm \
  -bind-source      fd20::a01    # MUST be unique per replica
```

`-source-mode=ssm` switches the data plane to the `FF3x::/32` SSM range
(FF35 for site scope, FF3E for global per RFC 8815). `-bind-source` is
mandatory in SSM mode and MUST differ across replicas — anycast or
ECMP-shared sources break PIM-SSM RPF.

With opt-in BRC-142 coalescing (pack many small transactions per bundle
datagram at the origin edge; a relay spine forwards bundles verbatim) — see
[docs/configuration.md](docs/configuration.md#brc-142-coalescing-origin-side):

```bash
./shard-proxy \
  -iface              eth0 \
  -coalesce \                    # opt-in; off by default
  -coalesce-max-bytes 1500       # path MTU for a bundle datagram (IPv6+UDP included)
```

With opt-in BRC-139 auto-shard-config (manifest-driven `ShardBits` adoption) — see [Automatic Shard Configuration Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#automatic-shard-configuration):

```bash
./shard-proxy \
  -iface                       eth0 \
  -manifest-consumer-enabled \          # opt-in; off by default
  -manifest-bootstrap          required \  # fail closed until quorum
  -pilot-quorum                2
```

Default behavior is restart-on-adopt; add `-live-resharding` for the dual-emit bridging path. See [docs/architecture.md](docs/architecture.md#brc-139-manifest-consumer-auto-shard-config) for the consumer subsystem.

With JSON structured logging for fleet aggregation (and opt-in tracing) — see [Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md):

```bash
./shard-proxy \
  -iface          eth0 \
  -log-format     json \   # one JSON object per line on stdout
  -log-level      info \   # runtime-togglable via POST /loglevel and SIGHUP
  -trace-sampling 0        # >0 + -otlp-endpoint enables control-plane traces
```

Each binary emits a one-shot `host.inventory` event (OS/CPU/mem/NIC incl. IPv4+IPv6) at startup and a `bsp_host_info` gauge. See [docs/architecture.md](docs/architecture.md#logging--tracing).

See [docs/configuration.md](docs/configuration.md) for all flags and environment variable equivalents.

## Container image

The Dockerfile produces a `gcr.io/distroless/static:nonroot` image with the
single static binary at `/usr/local/bin/shard-proxy`. No in-image
`ENV` defaults are set — configure via Helm `values.yaml`, container
environment variables, or CLI flags.

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`lightwebinc/shard-proxy-helm`](https://github.com/lightwebinc/shard-proxy-helm)
- HTTPS:
  ```
  helm repo add bsp https://lightwebinc.github.io/shard-proxy-helm
  helm install proxy bsp/shard-proxy
  ```
- OCI: `helm install proxy oci://ghcr.io/lightwebinc/charts/shard-proxy`

Flags are exposed under `.config` in the chart's `values.yaml` — see the chart README for the covered set and `values.schema.json` for validation rules.

## License

Apache 2.0 - See LICENSE file.
