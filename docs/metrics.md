# Metrics Reference

Every series shard-proxy exposes on `/metrics` (default `:9100`; see
[Metrics Endpoints](configuration.md#metrics-endpoints)). Source of truth:
[`metrics/metrics.go`](../metrics/metrics.go) and
[`metrics/hostinfo.go`](../metrics/hostinfo.go). Series registered through the
OTel SDK also carry `otel_scope_*` labels (omitted below). `iface` means the
`network_interface_name` label; `worker` is the worker index.

Go runtime (`go_*`) and process (`process_*`) collectors are also exported.

## Data path (per packet)

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bsp_packets_received_total` | counter | worker, iface | Datagrams received |
| `bsp_bytes_received_total` | counter | worker, iface | Raw bytes received |
| `bsp_packets_dropped_total` | counter | worker, iface, reason | Datagrams dropped, by reason (e.g. `decode_error`, `write_error`, `truncated`, `bundle_malformed`, `stamped_ingress`, `ingress_not_ef`, `beef_oversize`) |
| `bsp_packet_size_bytes` | histogram | worker, iface | Datagram size distribution (64 B to 256 KiB buckets) |
| `bsp_packets_forwarded_total` | counter | worker, iface | Datagrams forwarded to multicast |
| `bsp_bytes_forwarded_total` | counter | worker, iface | Raw bytes forwarded |
| `bsp_egress_errors_total` | counter | worker, iface | Egress socket write errors |
| `bsp_ingress_errors_total` | counter | worker, iface | Non-fatal ingress socket read errors |
| `bsp_ingress_class_bytes_total` | counter | class, tier | Accepted ingress bytes by frame class (`tx`, `anchor`, `beef`, `block`, `subtree`, `coinbase`) and tier (`transaction`, `privileged`) |
| `bsp_ingress_class_packets_total` | counter | class, tier | Accepted ingress frames, same labels |
| `bsp_flow_packets_total` | counter | iface, group | Packets per shard group (active groups only) |
| `bsp_flow_bytes_total` | counter | iface, group | Bytes per shard group (active groups only) |

## Process state

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bsp_active_groups` | gauge | iface | Distinct shard groups seen since startup |
| `bsp_workers_active` | gauge | none | Running worker goroutines |
| `bsp_uptime_seconds` | gauge | none | Seconds since process start |
| `bsp_host_info` | gauge (always 1) | hostname, kernel_version, cpu_logical, mem_bytes, rmem_max, nic, speed_mbps, version | Static host facts; join with the `host.inventory` log event |

## Fragmentation and coalescing

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bsp_frames_fragmented_total` | counter | worker, iface | Frames split into BRC-130 fragments |
| `bsp_fragments_emitted_total` | counter | worker, iface | BRC-130 fragment datagrams sent |
| `bsp_coalesce_bundles_total` | counter | worker, iface | BRC-142 bundle datagrams flushed |
| `bsp_coalesce_members_total` | counter | worker, iface | Member transactions packed into bundles |
| `bsp_coalesce_flush_total` | counter | worker, iface, reason | Flushes by reason: `batch` (origin), `relay` (verbatim spine re-emit), `encode_error` |
| `bsp_coalesce_members_per_bundle` | histogram | worker, iface | Members per bundle (buckets 1 to 256) |

## Ingress admission and control plane

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bsp_privileged_frame_rejected_total` | counter | frame_type | Privileged frames dropped on a transaction-only socket (see [miner port deprecation](configuration.md#ingress-is-transaction-only-miner-port-deprecated)) |
| `bsp_block_pow_rejected_total` | counter | none | BRC-131 block announces failing the PoW gate |
| `bsp_subtree_root_checks_total` | counter | result | `-verify-subtree-root` outcomes: `ok`, `mismatch`, `malformed` (last two dropped) |
| `bsp_beef_submissions_total` | counter | result | BRC-148 submission admission: `ok`, `malformed`, `oversize`, `bad_marker`, `disabled` |
| `bsp_beef_topics_total` | counter | topic_role | Topic names on admitted records: `deliverable` (matched at the edge) or `label` (carried, never matched or billed) |
| `bsp_control_frames_forwarded_total` | counter | ctrl_group | BRC-127 control datagrams forwarded to multicast |
| `bsp_tcp_connections_total` | counter | none | TCP ingress connections accepted |
| `bsp_tcp_bytes_received_total` | counter | none | Bytes read from TCP ingress connections |
| `bsp_tee_failed_total` | counter | kind | Datagrams the loopback tee could not deliver (retry cache or local listener mirror); non-zero means co-located cache misses |

## Ingress TxID dedup

See [Ingress TxID dedup](configuration.md#ingress-txid-dedup).

| Series | Type | Labels | Meaning |
|--------|------|--------|---------|
| `bsp_ingress_deduped_total` | counter | worker, iface, frame_type | Frames suppressed by the dedup gate |
| `bsp_txid_claim_local_hit_total` | counter | prefix | Tier-1 LRU short-circuits |
| `bsp_txid_claim_won_total` | counter | prefix | Tier-2 SETNX wins (frame forwarded) |
| `bsp_txid_claim_lost_total` | counter | prefix | Tier-2 SETNX losses (frame dropped) |
| `bsp_txid_claim_errors_total` | counter | prefix | Backend errors (fail-open: frame forwarded) |

## BRC-139 manifest consumer

These use the `multicast_manifest_*` family shared with shard-listener, not
the `bsp_` prefix: `multicast_manifest_received_total`,
`multicast_manifest_pilots_known`, `multicast_manifest_quorum_met_bits`,
`multicast_manifest_divergence_total`, `multicast_manifest_adoption_total`,
`multicast_manifest_last_divergence_epoch{field}`,
`multicast_manifest_resharding_state`,
`multicast_manifest_resharding_window_seconds`,
`multicast_manifest_resharding_emit_duplicates_total`. See
[Auto-Shard-Config](configuration.md#auto-shard-config-brc-139).
