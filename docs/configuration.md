# Configuration Reference

All parameters are accepted from CLI flags first; environment variables serve
as fallbacks; hard-coded defaults apply when neither is present.

## Flags and Environment Variables

| Flag | Env var | Default | Description |  |  |  |
|--------------------|-------------------|--------------------|----------------------------------------------------------------------------------------------------|----------|---------|----------|
| `-listen` | `LISTEN_ADDR` | `[::]` | Ingress bind address (without port) |  |  |  |
| `-udp-listen-port` | `UDP_LISTEN_PORT` | `9000` | UDP listen port for incoming BSV transaction frames (BRC-12, BRC-124, or BRC-128) |  |  |  |
| `-tcp-listen-port` | `TCP_LISTEN_PORT` | `0` | TCP ingress port for reliable delivery (0 = disabled) |  |  |  |
| `-iface` | `MULTICAST_IF` | `eth0` | Comma-separated NIC names for multicast egress |  |  |  |
| `-egress-port` | `EGRESS_PORT` | `9001` | Destination UDP port for multicast groups |  |  |  |
| `-shard-bits` | `SHARD_BITS` | `2` | Key bit width (1–15) |  |  |  |
| `-scope` | `MC_SCOPE` | `site` | Multicast scope: `link` \ | `site` \ | `org` \ | `global` |
| `-mc-group-id` | `MC_GROUP_ID` | `0x000B` | IANA group-id (bytes 12–13); default = IANA Bitcoin allocation `FF0X::B` |  |  |  |
| `-workers` | `NUM_WORKERS` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |  |  |  |
| `-debug` | `DEBUG` | `false` | Enable per-packet debug logging and multicast loopback |  |  |  |
| `-drain-timeout` | `DRAIN_TIMEOUT` | `0s` | Pre-drain delay before closing sockets; `/readyz` returns 503 during this window (`0s` = disabled) |  |  |  |
| `-metrics-addr` | `METRICS_ADDR` | `:9100` | HTTP bind address for `/metrics`, `/healthz`, `/readyz` |  |  |  |
| `-instance` | `INSTANCE_ID` | hostname | OTel `service.instance.id` for federation |  |  |  |
| `-otlp-endpoint` | `OTLP_ENDPOINT` | `""` | OTLP gRPC endpoint (empty = disabled) |  |  |  |
| `-otlp-interval` | `OTLP_INTERVAL` | `30s` | OTLP push interval |  |  |  |
| `-frag-mtu` | `FRAG_MTU` | `0` | Path MTU for BRC-130 fragmentation (0 = disabled) |  |  |  |
| `-recv-batch` | `BSP_RECV_BATCH` | `32` | Datagrams per `recvmmsg` syscall (1 = per-packet legacy path) |  |  |  |
| `-recv-buf-bytes` | `BSP_RECV_BUF_BYTES` | `0` | Per-worker `SO_RCVBUF` in bytes (`0` = system default; capped by `net.core.rmem_max`) |  |  |  |

---

## Ingress Modes

The proxy supports two ingress transports. Both feed the same forwarding
pipeline; you may run both simultaneously.

### UDP ingress (default)

UDP ingress uses `SO_REUSEPORT` to distribute incoming datagrams across all
worker goroutines with no userspace coordination. Each worker pulls up to
`-recv-batch` datagrams per `recvmmsg(2)` syscall and flushes the
corresponding egress queue once per batch via `sendmmsg(2)` (one syscall per
target interface), amortising syscall cost across the batch. On platforms
without `recvmmsg`/`sendmmsg` (macOS, FreeBSD) the `golang.org/x/net/ipv6`
library transparently falls back to per-packet send/recv; the proxy is
functionally identical but does not gain the syscall-amortisation speed-up.
This is the high-throughput path.

```
-udp-listen-port 9000   # (default)
```

### TCP ingress (optional)

TCP ingress provides reliable, ordered delivery for senders that require it
(e.g. over lossy links). Each accepted connection carries a stream of BRC-12, BRC-124, or BRC-128
frames concatenated end-to-end. The proxy reads 44 bytes first, then extends
to 92 bytes if BRC-124 (`PayLen` at bytes 88–91), then reads `PayLen` payload bytes.

TCP ingress is disabled by default. Enable it with:

```
-tcp-listen-port 9100
```

Both transports can run at the same time:

```
shard-proxy \
  -iface eth0 \
  -udp-listen-port 9000 \
  -tcp-listen-port 9100
```

---

## Shard Bits

`-shard-bits N` configures the number of txid prefix bits used to derive the
multicast group index. The total number of groups is 2^N.

| Bits | Groups | Typical use |
|------|------------|--------------------|
| 1 | 2 | Minimal / testing |
| 2 | 4 | Default |
| 8 | 256 | Medium deployments |
| 16 | 65 536 | Large deployments |
| 24 | 16 777 216 | Maximum |

Increasing bits by 1 splits every existing group into two child groups
(consistent hashing). Subscribers need only join additional groups.

---

## Forwarding

For **BRC-124/BRC-128 frames**, if `SeqNum` (bytes 48–55) is already non-zero the
sender has pre-stamped the frame and the proxy forwards it verbatim. If `SeqNum`
is zero the proxy stamps `HashKey` (bytes 40–47) as
`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)` and `SeqNum` (bytes 48–55) as a
monotonic per-flow counter, in-place. The `SubtreeID` field is always passed
through unchanged.

For **BRC-12 (legacy) frames**, the proxy always forwards the original bytes verbatim without
any modification.

For **BRC-134 anchor frames** (`FrameVerV6`), the proxy forwards to `GroupBlockBroadcast`
(`FF0X::B:FFFE`). Anchor frames use a virtual `groupIdx` of `0xFFF9` for HashKey derivation
so anchors are accounted as an independent flow (label `brc134`) distinct from BRC-131 block
control. See [bsv-multicast/docs/brc-134-anchor-transactions.md](../../../bsv-multicast/docs/brc-134-anchor-transactions.md).

---

## Multicast Scope

| Value | Prefix | Reach |
|----------|--------|-----------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local (default; crosses routers within a site) |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-routable |

---

## Metrics Endpoints

The metrics HTTP server (default `:9100`) exposes:

- **`/metrics`** — Prometheus text format
- **`/healthz`** — Always `200 OK` if the process is running
- **`/readyz`** — `200` when all workers are ready; `503` while starting or draining

---

## Graceful Drain

When a shutdown signal is received the proxy performs a two-phase shutdown:

1. **Drain phase** — `/readyz` immediately returns `503` (status `"draining"`), signalling the load balancer to stop routing new connections. The process sleeps for `-drain-timeout`. Workers continue forwarding in-flight packets during this window.
2. **Quiesce phase** — The ingress socket is closed, each worker exits its receive loop, and the process waits for all goroutines to finish before exiting.

Setting `-drain-timeout 0s` (the default) skips the sleep and closes sockets immediately after marking draining — suitable for single-node or development deployments.

For production with a load balancer or BGP, set `-drain-timeout` to at least the LB health-check interval plus one check period:

```bash
# LB health-check every 5 s — allow two missed checks before closing
shard-proxy -iface eth0 -drain-timeout 15s
```

> **`TimeoutStopSec` note:** systemd will send `SIGKILL` after `TimeoutStopSec` if the process has not exited. Ensure `TimeoutStopSec > drain-timeout + 15s` (OTLP flush + worker drain buffer). The default service unit sets `TimeoutStopSec=30`.

---

## Example Invocations

### Minimal (single NIC, defaults)

```bash
shard-proxy -iface eth0
```

### Multi-NIC, custom shard bits, OTLP

```bash
shard-proxy \
  -iface eth0,eth1 \
  -shard-bits 8 \
  -udp-listen-port 9000 \
  -egress-port 9001 \
  -otlp-endpoint collector:4317
```

### With TCP ingress

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 9000 \
  -tcp-listen-port 9100
```

### With graceful drain (behind a load balancer)

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 9000 \
  -drain-timeout 15s
```

## IANA group-id

The proxy follows IANA's IPv6 multicast allocation practice (96-bit
boundary) and the IANA-assigned Bitcoin group `FF0X::B`. The `-mc-group-id`
flag configures the 16-bit group-id occupying bytes 12–13 of every
generated multicast address. The default `0x000B` produces addresses of
the form `FF0X::B:<shard_index>` (IANA Bitcoin).

```bash
./shard-proxy \
  -mc-group-id 0x000B \
  -scope site \
  -shard-bits 8
```

Operators MAY override the group-id for testing or private deployments
(e.g. `-mc-group-id 0xCAFE`). Conformant production deployments use
`0x000B`.

## Fan-out to multiple interfaces

Every forwarded datagram is written to all listed interfaces in order,
with no copying and no extra goroutines on the hot path:

```bash
./shard-proxy \
  -iface       eth0,eth1 \
  -shard-bits  8         \
  -scope       site      \
  -udp-listen-port 9000  \
  -egress-port 9001
```

## Subscriber join

Each subscriber calls `IPV6_JOIN_GROUP` (or `setsockopt MCAST_JOIN_GROUP`)
for the multicast group address(es) covering its desired shard range:

```text
FF05::B:<shard_index>             # Default (IANA Bitcoin group-id 0x000B)
FF05::CAFE:<shard_index>          # With overridden group-id 0xCAFE
```

`SHARD_BITS` is a fixed, deployment-wide setting shared by all subscribers.
Doubling `SHARD_BITS` splits every existing group into two children —
subscribers join additional groups without invalidating existing ones,
so scale-up requires no redesign.

## Ingress TxID dedup

The proxy can optionally suppress duplicate ingress frames before stamping
and multicasting. A two-tier claim store is consulted on every BRC-124/128
(V2), BRC-131 block (V4), BRC-132 subtree data (V5), and BRC-134 anchor (V6)
frame. Legacy BRC-12 (V1) frames bypass the gate.

- **Tier 1** — in-process LRU keyed by TxID. Memory bounded by
  `-txid-dedup-local-cap` (default 1 048 576 entries, ~50 MiB). A hit
  short-circuits and the frame is dropped without contacting Redis.
- **Tier 2** — Redis `SETNX EX`. Activated only when
  `-txid-dedup-redis-addr` is non-empty. On a tier-1 miss the proxy claims
  `<prefix><hex-txid>` in Redis; on win it forwards, on loss it drops.
  Errors fail open (frame is forwarded; a metric is recorded).

### Flags

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `-txid-dedup-redis-addr` | `TXID_DEDUP_REDIS_ADDR` | `""` | Empty disables tier-2 (local-only) |
| `-txid-dedup-prefix` | `TXID_DEDUP_PREFIX` | `bsp:tx:` | Must match the local listener's `-ingress-set-prefix` for collapsed deployments |
| `-txid-dedup-ttl` | `TXID_DEDUP_TTL` | `10m` | Range 1m – 30m typical |
| `-txid-dedup-local-cap` | `TXID_DEDUP_LOCAL_CAP` | `1048576` | 0 disables the feature entirely |

### Topology guidance

- **Single proxy, no Redis** — leave `-txid-dedup-redis-addr` empty. The
  tier-1 LRU still suppresses local repeats (e.g. multiple upstream peers
  forwarding the same TxID).
- **Multiple proxies at one site** — point all proxies at the same Redis.
  Whichever proxy wins the SETNX multicasts; siblings drop.
- **Listener marks the ingress set** — when the local listener has
  `-mark-ingress-set` enabled, its courtesy SETNX populates the same
  namespace, so a TxID delivered via cross-site bridge prevents the local
  proxy from re-multicasting it.

### Metrics

- `bsp_ingress_deduped_total{frame_type, worker, network.interface.name}` —
  frames suppressed by the gate.
- `bsp_txid_claim_local_hit_total{prefix}` — tier-1 short-circuits.
- `bsp_txid_claim_won_total{prefix}` / `bsp_txid_claim_lost_total{prefix}` —
  tier-2 SETNX outcomes.
- `bsp_txid_claim_errors_total{prefix}` — Redis errors (fail-open).

## Helm chart

Every flag documented in this file is exposed under `.config` in the corresponding Helm chart's `values.yaml`. See the chart repository for installation snippets and the `values.schema.json` for validation rules.

Chart: [`lightwebinc/shard-proxy-helm`](https://github.com/lightwebinc/shard-proxy-helm)
