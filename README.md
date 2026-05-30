# shard-proxy

[![CI](https://github.com/lightwebinc/shard-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/shard-proxy/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/shard-proxy/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/shard-proxy/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/shard-proxy)](https://github.com/lightwebinc/shard-proxy/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/shard-proxy.svg)](https://pkg.go.dev/github.com/lightwebinc/shard-proxy)
[![Go Report Card](https://goreportcard.com/badge/github.com/lightwebinc/shard-proxy)](https://goreportcard.com/report/github.com/lightwebinc/shard-proxy)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A high-throughput proxy that receives Bitcoin SV (BSV Blockchain) transaction
frames (BRC-124, BRC-128, or legacy BRC-12) over UDP (or TCP for reliable delivery), derives
an IPv6 multicast group address from the transaction ID, and retransmits to
subscribers of the corresponding group. Further traffic segmentation is provided
via subtree-level sharding. Reliable delivery to multicast receivers is supported
via monotonic transmission flow sequencing. The TCP ingress also forwards
BRC-127 SubtreeAnnounce datagrams to the control-plane multicast group.

Inspiration: [Multicast within Multicast: Anycast](https://singulargrit.substack.com/p/multicast-within-multicast-anycast), [Multicast as the Only Viable Architecture](https://singulargrit.substack.com/p/multicast-as-the-only-viable-architecture)

```text
sender  ──UDP/TCP──►  shard-proxy  ──UDP multicast──►  FF05::<shard>  (iface 0)
                      (forwarder pipeline) └─────────────────►  FF05::<shard>  (iface 1)
                                                                 (subset of subscribers)
```

## Documentation

- [Architecture](docs/architecture.md) — system overview, multi-CPU design, graceful shutdown, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, ingress modes, drain timeout

## Dependencies

- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common) — `frame`, `shard`, `seqhash` packages

## Requirements

- Go 1.25 or later
- Linux kernel 3.9+, FreeBSD 12.3+ (for `SO_REUSEPORT`), MacOS
- IPv6 enabled on the egress interface(s)
- Multicast routing / MLD snooping configured for your subscriber fabric
- Bitcoin SV ingress transaction packets in BRC-12 (legacy) or BRC-124/BRC-128 frame format.

## Build

```bash
make            # builds shard-proxy, send-test-frames, recv-test-frames
make test       # runs unit tests
make test-e2e   # end-to-end test (builds all binaries, runs test/run-e2e.sh)
make clean      # removes built binaries
```

## Run

```bash
./shard-proxy \
  -iface            eth0 \
  -shard-bits       16   \
  -scope            site \
  -udp-listen-port  9000 \
  -egress-port      9001
```

With TCP ingress enabled:

```bash
./shard-proxy \
  -iface            eth0 \
  -udp-listen-port  9000 \
  -tcp-listen-port  9100
```

With Source-Specific Multicast (RFC 4607) — see [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/SourceSpecificMulticast/ssm-support-plan.md):

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
- OCI: `helm install proxy oci://ghcr.io/lightwebinc/charts/shard-proxy --version 0.1.0`

Every flag accepted by this binary is exposed under `.config` in the chart's `values.yaml`. See the chart README for the full reference and `values.schema.json` for validation rules.

## License

Apache 2.0 - See LICENSE file.
