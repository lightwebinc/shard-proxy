# Configuration Reference

All parameters are accepted from CLI flags first; environment variables serve
as fallbacks; hard-coded defaults apply when neither is present.

## Flags and Environment Variables

| Flag | Env var | Default | Description |  |  |  |
|--------------------|-------------------|--------------------|----------------------------------------------------------------------------------------------------|----------|---------|----------|
| `-listen` | `LISTEN_ADDR` | `[::]` | Ingress bind address (without port) |  |  |  |
| `-udp-listen-port` | `UDP_LISTEN_PORT` | `9000` | UDP listen port for incoming BSV transaction frames (v1 or BRC-124) |  |  |  |
| `-tcp-listen-port` | `TCP_LISTEN_PORT` | `0` | TCP ingress port for reliable delivery (0 = disabled) |  |  |  |
| `-iface` | `MULTICAST_IF` | `eth0` | Comma-separated NIC names for multicast egress |  |  |  |
| `-egress-port` | `EGRESS_PORT` | `9001` | Destination UDP port for multicast groups |  |  |  |
| `-shard-bits` | `SHARD_BITS` | `2` | Key bit width (1–24) |  |  |  |
| `-scope` | `MC_SCOPE` | `site` | Multicast scope: `link` \ | `site` \ | `org` \ | `global` |
| `-mc-base-addr` | `MC_BASE_ADDR` | `""` | Base IPv6 address for assigned multicast address space (bytes 2–12) |  |  |  |
| `-workers` | `NUM_WORKERS` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |  |  |  |
| `-debug` | `DEBUG` | `false` | Enable per-packet debug logging and multicast loopback |  |  |  |
| `-drain-timeout` | `DRAIN_TIMEOUT` | `0s` | Pre-drain delay before closing sockets; `/readyz` returns 503 during this window (`0s` = disabled) |  |  |  |
| `-metrics-addr` | `METRICS_ADDR` | `:9100` | HTTP bind address for `/metrics`, `/healthz`, `/readyz` |  |  |  |
| `-instance` | `INSTANCE_ID` | hostname | OTel `service.instance.id` for federation |  |  |  |
| `-otlp-endpoint` | `OTLP_ENDPOINT` | `""` | OTLP gRPC endpoint (empty = disabled) |  |  |  |
| `-otlp-interval` | `OTLP_INTERVAL` | `30s` | OTLP push interval |  |  |  |

---

## Ingress Modes

The proxy supports two ingress transports. Both feed the same forwarding
pipeline; you may run both simultaneously.

### UDP ingress (default)

UDP ingress uses `SO_REUSEPORT` to distribute incoming datagrams across all
worker goroutines with no userspace coordination. This is the high-throughput
path.

```
-udp-listen-port 9000   # (default)
```

### TCP ingress (optional)

TCP ingress provides reliable, ordered delivery for senders that require it
(e.g. over lossy links). Each accepted connection carries a stream of v1 or BRC-124
frames concatenated end-to-end. The proxy reads 44 bytes first, then extends
to 92 bytes if BRC-124 (`PayLen` at bytes 88–91), then reads `PayLen` payload bytes.

TCP ingress is disabled by default. Enable it with:

```
-tcp-listen-port 9100
```

Both transports can run at the same time:

```
bitcoin-shard-proxy \
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

For **BRC-124 frames**, the proxy stamps the `SenderID` field (bytes 40–43)
in-place with the CRC32c of the ingress source IPv6 address before forwarding.
All other fields, including `SeqNum` and `SubtreeID`, are passed through
unchanged exactly as the sender set them.

For **v1 frames**, the proxy forwards the original bytes verbatim without any
modification.

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
bitcoin-shard-proxy -iface eth0 -drain-timeout 15s
```

> **`TimeoutStopSec` note:** systemd will send `SIGKILL` after `TimeoutStopSec` if the process has not exited. Ensure `TimeoutStopSec > drain-timeout + 15s` (OTLP flush + worker drain buffer). The default service unit sets `TimeoutStopSec=30`.

---

## Example Invocations

### Minimal (single NIC, defaults)

```bash
bitcoin-shard-proxy -iface eth0
```

### Multi-NIC, custom shard bits, OTLP

```bash
bitcoin-shard-proxy \
  -iface eth0,eth1 \
  -shard-bits 8 \
  -udp-listen-port 9000 \
  -egress-port 9001 \
  -otlp-endpoint collector:4317
```

### With TCP ingress

```bash
bitcoin-shard-proxy \
  -iface eth0 \
  -udp-listen-port 9000 \
  -tcp-listen-port 9100
```

### With graceful drain (behind a load balancer)

```bash
bitcoin-shard-proxy \
  -iface eth0 \
  -udp-listen-port 9000 \
  -drain-timeout 15s
```

## Assigned address space

The `-mc-base-addr` flag allows use of assigned IPv6 address space instead
of the generic zero-filled middle section. Useful when specific multicast
address ranges have been allocated by a numbers authority.

```bash
./bitcoin-shard-proxy \
  -mc-base-addr "2001:db8:1234::" \
  -scope site \
  -shard-bits 16
```

This generates addresses like `FF05:2001:db8:1234::<group_index>` instead
of the default `FF05::<group_index>`.

The base address can be any valid IPv6 address; only bytes 2–12 are used
for the middle section. The first two bytes are replaced by the multicast
prefix and scope.

## Fan-out to multiple interfaces

Every forwarded datagram is written to all listed interfaces in order,
with no copying and no extra goroutines on the hot path:

```bash
./bitcoin-shard-proxy \
  -iface       eth0,eth1 \
  -shard-bits  16        \
  -scope       site      \
  -udp-listen-port 9000  \
  -egress-port 9001
```

## Subscriber join

Each subscriber calls `IPV6_JOIN_GROUP` (or `setsockopt MCAST_JOIN_GROUP`)
for the multicast group address(es) covering its desired shard range:

```text
FF05::<group_index>                   # Default format
FF05:2001:db8:1234::<group_index>     # With assigned address space
```

`SHARD_BITS` is a fixed, deployment-wide setting shared by all subscribers.
Doubling `SHARD_BITS` splits every existing group into two children —
subscribers join additional groups without invalidating existing ones,
so scale-up requires no redesign.
