# bitcoin-shard-proxy

[![CI](https://github.com/lightwebinc/bitcoin-shard-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/bitcoin-shard-proxy/actions/workflows/ci.yml)

A high-throughput proxy that receives Bitcoin SV (BSV Blockchain) transaction
frames (BRC-124 or legacy BRC-12) over UDP (or TCP for reliable delivery), derives
an IPv6 multicast group address from the transaction ID, and retransmits to
subscribers of the corresponding group. Further traffic segmentation is provided
via subtree-level sharding. Reliable delivery to multicast receivers is supported
via monotonic transmission flow sequencing. The TCP ingress also forwards
BRC-127 SubtreeAnnounce datagrams to the control-plane multicast group.

Inspiration: [Multicast within Multicast: Anycast](https://singulargrit.substack.com/p/multicast-within-multicast-anycast), [Multicast as the Only Viable Architecture](https://singulargrit.substack.com/p/multicast-as-the-only-viable-architecture)

```text
sender  ──UDP/TCP──►  bitcoin-shard-proxy  ──UDP multicast──►  FF05::<shard>  (iface 0)
                      (forwarder pipeline) └─────────────────►  FF05::<shard>  (iface 1)
                                                                 (subset of subscribers)
```

## Documentation

- [Architecture](docs/architecture.md) — system overview, multi-CPU design, graceful shutdown, package structure
- [Configuration](docs/configuration.md) — all flags, environment variables, ingress modes, drain timeout

## Dependencies

- [`github.com/lightwebinc/bitcoin-shard-common`](https://github.com/lightwebinc/bitcoin-shard-common) — `frame`, `shard`, `seqhash` packages

## Requirements

- Go 1.25 or later
- Linux kernel 3.9+, FreeBSD 12.3+ (for `SO_REUSEPORT`), MacOS
- IPv6 enabled on the egress interface(s)
- Multicast routing / MLD snooping configured for your subscriber fabric
- Bitcoin SV ingress transaction packets in BRC-12 (v1) or BRC-124 frame format.

## Build

```bash
make            # builds bitcoin-shard-proxy, send-test-frames, recv-test-frames
make test       # runs unit tests
make test-e2e   # end-to-end test (builds all binaries, runs test/run-e2e.sh)
make clean      # removes built binaries
```

## Run

```bash
./bitcoin-shard-proxy \
  -iface            eth0 \
  -shard-bits       16   \
  -scope            site \
  -udp-listen-port  9000 \
  -egress-port      9001
```

With TCP ingress enabled:

```bash
./bitcoin-shard-proxy \
  -iface            eth0 \
  -udp-listen-port  9000 \
  -tcp-listen-port  9100
```

See [docs/configuration.md](docs/configuration.md) for all flags and environment variable equivalents.

## License

Apache 2.0 - See LICENSE file.
