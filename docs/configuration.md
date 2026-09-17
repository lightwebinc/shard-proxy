# Configuration Reference

All parameters are accepted from CLI flags first; environment variables serve
as fallbacks; hard-coded defaults apply when neither is present.

## Flags and Environment Variables

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `-listen` | `LISTEN_ADDR` | `[::]` | Ingress bind address (without port) |
| `-udp-listen-port` | `UDP_LISTEN_PORT` | `8725` | UDP transaction ingress — **framed** (BRC-124/128/legacy BRC-12), **bare** header-stripped transactions, or a **BEEF submission record** (leading `0xBEEF` tag), auto-detected from the leading bytes (one submission per datagram). See [Transaction ingress](architecture.md#transaction-ingress-framed-bare-and-ef-native) |
| `-tcp-listen-port` | `TCP_LISTEN_PORT` | `0` | TCP ingress port for reliable delivery (0 = disabled) |
| `-subtree-listen-port` | `SUBTREE_LISTEN_PORT` | `0` | TCP port accepting BRC-143 subtree push frames (privileged; bind tunnel-side; standard 8726; 0 = disabled) |
| `-block-listen-port` | `BLOCK_LISTEN_PORT` | `0` | TCP port accepting BRC-144 block push frames (privileged; bind tunnel-side; standard 8727; 0 = disabled) |
| `-beef-listen-port` | `BEEF_LISTEN_PORT` | `0` | Optional dedicated TCP lane for BRC-148 BEEF submission records (flow separation only — BEEF also rides the tx port; standard 8728; 0 = disabled). See [BRC-148 BEEF object plane](#brc-148-beef-object-plane) |
| `-beef-shard-bits` | `BEEF_SHARD_BITS` | `0` | BRC-148 BEEF plane shard-bit width (band `0x1000 + 2^bits` groups; 0–12, `0` = single group) |
| `-beef-max-object-bytes` | `BEEF_MAX_OBJECT_BYTES` | `1048576` | Maximum accepted BEEF object size in bytes (BRC-149 ingress bound), on every acceptance path |
| `-require-block-pow` | `REQUIRE_BLOCK_POW` | `true` | Gate BRC-131 block announces on a cheap stateless proof-of-work check of the in-frame header. Permissionless (validates work, not identity). **Default ON** — set `=false` to admit unvalidated announces. See [Block-announce proof-of-work](#block-announce-proof-of-work) |
| `-require-ef` | `REQUIRE_EF` | `false` | **EF-native ingress**: reject raw BRC-12/BRC-124 transaction submissions; only Extended Format (BRC-30) is admitted. Applies to stamped frames too — `SeqNum` is sender-chosen and must not waive the EF posture. See [EF-native ingress](architecture.md#ef-native-ingress--require-ef) |
| `-allow-stamped-ingress` | `ALLOW_STAMPED_INGRESS` | `false` | Admit framed BRC-124/BRC-128 input that already carries a SeqNum (another proxy's output). **Off by default**: an ingress proxy accepts submissions, not relay. Enable on a spine collect lane or relay hop. See [Stamped ingress](#stamped-ingress) |
| `-verify-payload-hash` | `VERIFY_PAYLOAD_HASH` | `false` | Verify the canonical TxID of **framed** BRC-124/BRC-128 input against its payload and drop mismatches before the ingress dedup claim. Bare submissions are unaffected (the proxy derives their TxID itself). Costs one SHA256d per framed transaction. See [Payload-hash verification](#payload-hash-verification) |
| `-min-pow-bits` | `MIN_POW_BITS` | `0` | PoW difficulty floor in Bitcoin compact `nBits` form (e.g. `0x1d00ffff`); `0` = header self-consistency only (weak) |
| `-iface` | `MULTICAST_IF` | `eth0` | Comma-separated NIC names for multicast egress |
| `-egress-port` | `EGRESS_PORT` | `9001` | Destination UDP port for multicast groups |
| `-egress-hoplimit` | `EGRESS_HOPLIMIT` | `1` | IPv6 multicast hop limit (`IPV6_MULTICAST_HOPS`) written on egress. `1` = single L2 segment; raise above 1 for routed/tunneled mesh fabrics where the datagram must cross ip6gre / PIM hops |
| `-egress-loop` | `EGRESS_MULTICAST_LOOP` | `false` | Enable `IPV6_MULTICAST_LOOP` on egress. Required on collapsed / mesh-router nodes so locally-originated multicast is picked up by the kernel MFC and forwarded to tunnel / consumer OIFs. Off for plain L2 egress |
| `-shard-bits` | `SHARD_BITS` | `2` | Key bit width (1–12 per BRC-129) |
| `-scope` | `MC_SCOPE` | `site` | Multicast scope: `link` \| `site` \| `org` \| `global` |
| `-mc-group-id` | `MC_GROUP_ID` | `0x000B` | IANA group-id (bytes 12–13); default = IANA Bitcoin allocation `FF0X::B` |
| `-source-mode` | `SOURCE_MODE` | `asm` | Multicast addressing model: `asm` (FF0x; default) or `ssm` (FF3x per RFC 4607). SSM derives the prefix via `shard.Prefix(SSM, scope)` → FF35 site / FF3E global; RFC 8815 deprecates ASM at global scope and is rejected. Requires PIM-SSM in the fabric. See [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm). |
| `-bind-source` | `BIND_SOURCE` | `""` | IPv6 literal bound on every multicast egress socket via `syscall.Bind` in `openEgressSocket`. **Required when `-source-mode=ssm`** and MUST be distinct per replica — anycast/ECMP-shared sources break PIM-SSM RPF. For single-identity deployments use VRRP active-standby. |
| `-stamp-source` | `STAMP_SOURCE` | `true` | Authoritatively stamp the BRC HashKey from the **observed** packet source IP, overriding any sender-supplied value, so the per-flow identity is the real ingress source. This is what makes own-traffic exclusion per-consumer at a direct/collapsed edge (the proxy sees each consumer's distinct source). Set `false` **only** behind a source-rewriting load balancer, where every consumer would otherwise appear as the LB address and the upstream-supplied HashKey is trusted instead. |
| `-workers` | `NUM_WORKERS` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |
| `-debug` | `DEBUG` | `false` | Enable per-packet debug logging and multicast loopback (single-host testing); deprecated alias for `-log-level=debug` |
| `-drain-timeout` | `DRAIN_TIMEOUT` | `0s` | Pre-drain delay before closing sockets; `/readyz` returns 503 during this window (`0s` = disabled) |
| `-metrics-addr` | `METRICS_ADDR` | `:9100` | HTTP bind address for `/metrics`, `/healthz`, `/readyz` |
| `-instance` | `INSTANCE_ID` | hostname | OTel `service.instance.id` for federation |
| `-otlp-endpoint` | `OTLP_ENDPOINT` | `""` | OTLP gRPC endpoint (empty = disabled) |
| `-otlp-interval` | `OTLP_INTERVAL` | `30s` | OTLP push interval |
| `-log-format` | `LOG_FORMAT` | `text` | Log output format: `text` (stderr, dev default) or `json` (stdout, for fleet aggregation). See [Unified Logging](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md) |
| `-log-level` | `LOG_LEVEL` | `info` | Log level: `debug` \| `info` \| `warn` \| `error`. Runtime-togglable via `POST /loglevel?level=` and SIGHUP. `-debug` is a deprecated alias for `debug` |
| `-trace-sampling` | `TRACE_SAMPLING` | `0` | Distributed-trace head sampling ratio `0`–`1` (`0` = tracing off, no-op tracer; exports via `-otlp-endpoint`; control-plane only, never the packet hot path) |
| `-frag-mtu` | `FRAG_MTU` | `1500` | Path MTU for BRC-130 fragmentation; default 1500 (Ethernet baseline). Set to the **smallest** MTU on the egress path — on a tunnelled fabric that is the tunnel inner MTU, not the local NIC. `0` = disabled, which strands every payload above MTU−140 (1360 at 1500) as an undeliverable oversize datagram |
| `-coalesce` | `COALESCE` | `false` | Opt-in BRC-142 origin-side frame coalescing (pack many small same-`(group, subtree)` tx per datagram to cut egress pps). See [BRC-142 Coalescing](#brc-142-coalescing-origin-side) |
| `-coalesce-max-bytes` | `COALESCE_MAX_BYTES` | `1500` | Path MTU budget for a coalesced bundle, **on the wire** (1500 = Ethernet MTU baseline; 9000 for jumbo on a controlled underlay). The 48-byte IPv6+UDP header is subtracted before packing, exactly like `-frag-mtu` |
| `-coalesce-max-members` | `COALESCE_MAX_MEMBERS` | `0` | Max member transactions per bundle (`0` = MTU-bound only) |
| `-coalesce-carry-txid` | `COALESCE_CARRY_TXID` | `false` | Carry each member's 32-byte TxID on the wire (for downstream dedup / operator accounting) instead of recomputing it on receipt |
| `-recv-batch` | `BSP_RECV_BATCH` | `32` | Datagrams per `recvmmsg` syscall (1 = per-packet legacy path) |
| `-retry-tee` | `BSP_RETRY_TEE` | `""` | Mirror each egressed DATA datagram to a co-located retry endpoint's cache-ingest address (e.g. `[::1]:9001`) — needed on a node that originates frames and hosts its own retry cache, since it must never (S,G)-join its own source. Copies are batched into one `sendmmsg` per egress batch. Empty = disabled |
| `-recv-buf-bytes` | `BSP_RECV_BUF_BYTES` | `0` | Per-worker `SO_RCVBUF` in bytes (`0` = the worker default of 4 MiB; the kernel caps the request at `net.core.rmem_max`) |
| `-ingress-dedup` | `INGRESS_DEDUP` | `true` | Enable ingress TxID dedup. `false` bypasses the dedup gate entirely — only sound for single-proxy ingest topologies. See [Ingress TxID Deduplication](#ingress-txid-dedup) |
| `-pprof` | `BSP_PPROF` | `false` | Mount `net/http/pprof` at `/debug/pprof/*` on the metrics server (profiling only) |

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
-udp-listen-port 8725   # (default)
```

### TCP ingress (optional)

TCP ingress provides reliable, ordered delivery for senders that require it
(e.g. over lossy links). Grammar is detected **once per connection** from the
leading bytes: a `0xBEEF` record tag selects a BEEF submission-record stream;
the BSV network magic selects a framed stream (BRC-12, BRC-124, or BRC-128
frames concatenated end-to-end — a 44-byte header read, extended to 92 bytes
for 92-byte-header versions, then `PayLen` payload bytes); anything else is a
bare transaction stream.

TCP ingress is disabled by default. Enable it with:

```
-tcp-listen-port 8725
```

Both transports can run at the same time:

```
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
  -tcp-listen-port 8725
```

---

## Ingress is transaction-only (miner port deprecated)

Ingress accepts the **open class set only**: BRC-12 / 124 / 128 transactions,
framed or bare (an anchor is an ordinary transaction), and BRC-148 BEEF objects
(submission records or FrameVer `0x09`). Privileged control-plane frames — block
announce (BRC-131), coinbase (BRC-133), subtree data (BRC-132) — arriving on an
ingress socket are **dropped** and counted (`bsp_privileged_frame_rejected_total`).

| Socket | Flag | Accepts |
|--------|------|---------|
| Transaction ingress | `-udp-listen-port` (8725), `-tcp-listen-port` | BRC-12 / 124 / 128 transactions (framed or bare) + BRC-134 anchor + BRC-148 BEEF (record or `0x09`) + BRC-142 bundle relay (UDP only). Privileged BRC-131/133/132 frames are dropped. |
| BEEF lane (optional) | `-beef-listen-port` (8728) | BRC-148 BEEF **submission records only** (an `objfmt.ClassBEEF` record stream, so no framed input; a non-record stream desyncs the reader and closes the connection). |
| Push lanes (privileged) | `-subtree-listen-port` (8726), `-block-listen-port` (8727) | BRC-143 subtree / BRC-144 block push objects, reframed to BRC-132 / BRC-131. |

> **Deprecated 2026-07-07 — the miner multicast port is gone.** The former
> `-miner-listen-port` / `-miner-tcp-listen-port` / `-tx-accept-privileged`
> flags have been removed. Blocks and subtrees are no longer submitted as
> multicast frames: a miner is a tunnel consumer that submits **BRC-144 block**
> and **BRC-143 subtree** push frames on the proxy's tunnel-bound push
> ports (8726 / 8727), which the proxy reframes into the fabric internally.
> Multicast (BRC-131/132/133/134) is fabric-internal transport only. See
> [DESIGN.md § Ingress authorization (miner-tier gate)](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#ingress-authorization-miner-tier-gate).

---

## Block-announce proof-of-work

Transaction ingress is open by default. The proof-of-work gate is the
**permissionless** control on block announces that reach the proxy over the
fabric (inter-domain announces never pass a local ingress socket): it validates
the
*artifact*, not the emitter. With `-require-block-pow`, a BRC-131 block
announce is forwarded only if its in-frame 80-byte header hashes under the
target its own `nBits` claims, and that target is at least as hard as
`-min-pow-bits` (the difficulty floor). Anyone may announce a block; forging a
passing header costs work proportional to the floor, while the proxy verifies
it with a single double-SHA256 — that asymmetry is the spam gate, with no
allowlist and nothing to coordinate across domains.

```
-require-block-pow -min-pow-bits 0x1d00ffff
```

Scope and limits:
- Applies to **block announces only**. Coinbase (BRC-133) and subtree data
  (BRC-132) carry no in-frame header, so they stay on the admission gate.
- Set a real `-min-pow-bits` floor in production: `0` only checks the header is
  self-consistent, which a forger satisfies trivially by claiming easy
  difficulty.
- This is **not** full consensus validation (no chain context — it cannot
  verify `nBits` is the correct retarget for the height). It rejects spam at
  ingress; the consuming node (Teranode) does full validation.

Rejections increment `bsp_block_pow_rejected_total`.

---

## Stamped ingress

`-allow-stamped-ingress` decides whether this proxy accepts framed
BRC-124/BRC-128 input that already carries a `SeqNum`. **Off by default.**

A stamped frame is one another proxy already admitted, sharded and emitted. On
a submission lane that is not something a submitter can legitimately send, and
`SeqNum` is just eight bytes in the frame that the sender chooses. Rejections
increment `bsp_packets_dropped_total{reason="stamped_ingress"}`.

Enable it only where the lane really does carry fabric traffic:

| Deployment | Setting |
|------------|---------|
| Public / customer submission lane (8725) | leave off |
| `shard-proxy-1bsv -mode collapsed` \| `ingress` | leave off |
| `shard-proxy-1bsv -mode spine` | **forced on** — the collect lane exists to re-emit stamped fabric frames |
| A relay hop forwarding another proxy's output | on |

Enabling it does not waive any other gate: a stamped frame admitted this way is
still subject to `-require-ef` and `-verify-payload-hash`.

**Gap-injection load rigs need this flag.** `subtx-generator` in unicast mode
leaves `SeqNum` zero *unless* gap injection is active, in which case it stamps
per-flow SeqNums itself so the listener sees engineered gaps. Those frames are
stamped input by definition, so a gap-injection run against a proxy requires
`-allow-stamped-ingress=true`. Its direct-multicast mode bypasses the proxy and
is unaffected either way.

### Why `-require-ef` no longer exempts stamped frames

`-require-ef` used to skip its check when `SeqNum != 0`, reasoning that a
relayed frame had been validated at its own ingress. That reasoning holds for
genuine relay traffic, but nothing distinguished it from a submitter who simply
set a non-zero `SeqNum` — making the EF posture opt-out with a one-byte edit.

The exemption is gone. A lane that legitimately carries relayed frames declares
itself with `-allow-stamped-ingress`, and on an EF-native fabric its traffic is
Extended Format anyway, so the check costs it nothing.

> **Behaviour change.** A deployment relying on the old exemption to relay
> non-EF payloads with `-require-ef` on will now see them dropped as
> `ingress_not_ef`. On an EF-native fabric there is no such traffic.

Scope: the gate covers FrameVer `0x02` (BRC-124/BRC-128). BRC-12 (V1) decodes
with a zero `SeqNum` and is unaffected; BRC-130 fragments (`0x03`) and the
bundle/BEEF/block/subtree classes are outside it, as they are outside every
other V2 gate.

---

## Payload-hash verification

`-verify-payload-hash` requires a **framed** BRC-124/BRC-128 (FrameVer `0x02`)
datagram to carry the canonical transaction id of its own payload: SHA256d over
the standard serialization, EF extras excluded (`objfmt.TxID`). That is the id
producers stamp and consumers derive, so honest Extended Format traffic
verifies cleanly — a gate that hashed the raw EF bytes instead would drop 100%
of it.

Off by default. Two checks run, both ahead of the dedup claim and group
derivation:

| Condition | Drop reason |
|-----------|-------------|
| Payload is not exactly one transaction (unwalkable, truncated, or trailing bytes) | `payload_not_one_tx` |
| Payload walks, but its canonical TxID ≠ the frame's TxID | `payload_hash_mismatch` |

Both increment `bsp_packets_dropped_total{reason=...}`.

### Why this is not a duplicate of the listener's flag

The listener has a flag of the same name, but it cannot cover the ingress
exposure. Two things key off the wire TxID **at the proxy**, before any
listener sees the frame:

- **Group derivation** is the top `-shard-bits` of `TxID[0:4]`, so an
  unverified TxID lets a submitter choose its own shard.
- **The ingress dedup claim** is keyed on the TxID, and `-ingress-dedup`
  defaults to `true`. A frame carrying an honest transaction's TxID over a
  forged payload wins the claim, and the honest transaction is then suppressed
  at ingress — it never reaches a listener, so the listener's own verification
  cannot undo it.

The gate therefore runs **before** `claimIngress`, not after.

### Scope and exemptions

- **Bare submissions are exempt and cost nothing.** The proxy derives their
  TxID itself when reframing, so there is nothing to verify.
- **BRC-12 (V1) frames are forwarded verbatim** regardless — the legacy wire
  format carries no payload-bound identifier.
- **Already-stamped frames are *not* exempt.** `SeqNum` is chosen by whoever
  sent the frame, so exempting on it would be a one-byte bypass of the gate —
  the same reasoning that removed the former `-require-ef` exemption (see
  [Stamped ingress](#stamped-ingress)). A stamped frame admitted via
  `-allow-stamped-ingress` is still verified here.
- BRC-142 bundles (`0x08`), BEEF (`0x09`), and the block/subtree/anchor classes
  are unaffected; they carry different identifiers and their own gates.

### Cost

One SHA256d over the payload per framed transaction, plus one structural walk.
`objfmt.TxID` streams the standard serialization into the hash rather than
materialising it, so the check allocates nothing — but the hash itself is not
free, and on a CPU **without SHA-NI** it can exceed the cost of the entire
forward path. Measured on a Xeon E5-2699 v3 (Haswell, no SHA-NI), a 232-byte
1-in/1-out EF transaction: ~1.84 µs to verify against ~1.0 µs to forward.

Any CPU with SHA-NI (Zen 1+, Ice Lake+, Graviton2+) hashes far faster, and Go's
`crypto/sha256` uses it automatically. **Measure on the target edge CPU before
enabling on a hot lane** — the ratio above is close to the worst case, not the
typical one.

Enable it on permissionless / miner-lane ingress and on commercial edges where
submitters are not trusted. Leave it off on spine and relay hops, where the
frame was already verified at its own ingress.

The listener implements the identical check behind its own
`-verify-payload-hash`, including the exactly-one-transaction bound, and applies
it to BRC-130 reassembled payloads too. See `shard-listener`
`docs/configuration.md`.

---

## Shard Bits

`-shard-bits N` configures the number of txid prefix bits used to derive the
multicast group index. The total number of groups is 2^N.

| Bits | Groups | Typical use |
|------|------------|--------------------|
| 1 | 2 | Minimal / testing |
| 2 | 4 | Default |
| 8 | 256 | Medium deployments |
| 12 | 4 096 | Maximum (BRC-129 bound) |

BRC-129 zoning bounds shard group indices to `0x0000`–`0x0FFF`, so conformant
deployments use at most 12 bits. (The flag validator enforces the same bound:
`[1, 12]`.)

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
control. See [bsv-multicast/docs/brc-134-anchor-transactions.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-134-anchor-transactions.md).

---

## BRC-142 Coalescing (origin-side)

Coalescing is the inverse of BRC-130 fragmentation: instead of splitting one
large frame across many datagrams, it packs **many small transactions into one
datagram** — a BRC-142 *bundle* frame (`FrameVer 0x08`, 66-byte header). It is
opt-in (`-coalesce`, off by default) and trades a small latency/CPU cost for a
large reduction in egress packets-per-second when the workload is many tiny
transactions.

Within a single receive batch the worker buckets eligible BRC-124/BRC-128
transactions by their `(sender, group, subtree)` flow and, at batch end, packs
each bucket into one or more bundle datagrams (up to the byte/member budget)
before the egress flush. The byte budget is a **path MTU**: the packer first
subtracts the 48-byte IPv6+UDP header, so the bundle body is capped at
`-coalesce-max-bytes − 48` (1452 at the 1500 default) and the datagram that
leaves the NIC is never larger than the configured MTU. Each bundle draws its `HashKey`/`SeqNum` from the **same
per-flow counter** that stamps individual frames, so a flow's bundles and any
individual frames it also emits share one contiguous sequence space — a listener
gap-tracks the `(group, subtree, HashKey)` flow uniformly regardless of frame
version, and NACK/retry treats a bundle as a single "fat frame."

**Origin-only rule.** Coalescing runs **only at the emitting (origin) proxy** —
the collapsed or ingress edge that first sees the individual transactions. A
regional spine that *relays* an already-coalesced bundle forwards it
**verbatim**: it is not re-coalesced, not re-stamped, and not split. A bundle is
a complete, self-describing multicast frame bound to the single group it was
built for, so a relay re-emits it unchanged (one bundle in → one multicast
datagram out). Never enable origin coalescing on a
node whose role is to relay bundles.

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `-coalesce` | `COALESCE` | `false` | Master switch. Off = every transaction egresses as its own frame (legacy) |
| `-coalesce-max-bytes` | `COALESCE_MAX_BYTES` | `1500` | Path MTU budget for the emitted bundle datagram, IPv6+UDP headers included. `1500` = public-internet Ethernet MTU (realistic baseline); `9000` for jumbo on a controlled underlay. `0` also means 1500. Set it to the SMALLEST MTU on the egress path, as with `-frag-mtu` |
| `-coalesce-max-members` | `COALESCE_MAX_MEMBERS` | `0` | Hard cap on member transactions per bundle. `0` = bounded only by `-coalesce-max-bytes` (capped by the wire `TxCount` uint16) |
| `-coalesce-carry-txid` | `COALESCE_CARRY_TXID` | `false` | When set, each member carries its 32-byte TxID on the wire (for downstream dedup / operator accounting) — an all-or-none flag across the bundle. When unset the receiver recomputes TxIDs, saving 32 B/member |

```bash
# origin edge: coalesce small tx to cut egress pps, Ethernet-MTU bundles
shard-proxy \
  -iface eth0 \
  -coalesce \
  -coalesce-max-bytes  1500 \
  -coalesce-carry-txid           # carry TxIDs for accounting/dedup downstream
```

Wire format (bundle header + member section), the `Coalescer`/`Decoalesce`/
`Rebucketer` transforms, and the all-or-none TxID flag are specified once in the
canonical BRC-142 spec — see
[`shard-common/docs/protocol.md` § BRC-142](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md#3a-brc-142-coalescing-bundle-frame-format)
and the public
[bsv-multicast/docs/brc-142-coalescing-frame.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-142-coalescing-frame.md).
Bundles are re-aligned to a receiver's shard generation (re-bucket) and split
back to individual frames (decoalesce) at the **delivery edge** (listener), not
in this proxy.

Bundle metrics: `bsp_coalesce_bundles_total` (bundle datagrams flushed) and
`bsp_coalesce_members_total` (member transactions packed) count origin packing,
with the per-bundle member count distributed in `bsp_coalesce_members_per_bundle`.
`bsp_coalesce_flush_total{reason}` records flushes by reason — `batch` for
origin-packed bundles, `relay` for verbatim spine re-emits, `encode_error` on a
skipped bundle.

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
  -udp-listen-port 8725 \
  -egress-port 9001 \
  -otlp-endpoint collector:4317
```

### With TCP ingress

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
  -tcp-listen-port 8725
```

### With graceful drain (behind a load balancer)

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
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
  -udp-listen-port 8725  \
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
(V2), BRC-131 block (V4), BRC-132 subtree data (V5), BRC-134 anchor (V6), and
BRC-149 BEEF (V9, keyed on `SHA-256(ContentID ∥ TopicID)`) frame. Legacy
BRC-12 (V1) frames and BRC-142 bundle relays bypass the gate.

- **Tier 1** — in-process LRU keyed by TxID, sharded across 64 stripes
  (each with its own mutex) to keep the dedup path from serialising all
  workers. Memory bounded by `-txid-dedup-local-cap` (default 1 048 576
  entries, ~50 MiB). A hit short-circuits and the frame is dropped without
  contacting Redis.
- **Tier 2** — a modular `shard-common/cache` backend `SetNX`
  (`redis|aerospike|memory`). Selected by `-txid-dedup-backend` (or inferred:
  `redis` when `-txid-dedup-redis-addr` is set, else `none`). On a tier-1 miss
  the proxy claims `<prefix><hex-txid>`; on win it forwards, on loss it drops.
  Errors fail open (frame is forwarded; a metric is recorded). See
  [`shard-common/docs/cache-backend.md`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md).

The whole gate runs per packet, so it costs CPU. After the localSet
sharding + direct-Prometheus counter work the dedup-on overhead is small
(measured ~6 % fewer pps at 256 B vs dedup-off on a 25 GbE single-host
loopback), but `-ingress-dedup=false` bypasses it entirely for
deployments that provably never ingest duplicates (a single proxy with a
single upstream feed). Leave it on for any multi-proxy or bridged topology.

### Flags

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `-ingress-dedup` | `INGRESS_DEDUP` | `true` | `false` bypasses the entire gate. Only safe for single-proxy ingest |
| `-txid-dedup-backend` | `TXID_DEDUP_BACKEND` | inferred | `redis\|aerospike\|memory\|none`. Empty → `redis` if addr set, else `none` |
| `-txid-dedup-redis-addr` | `TXID_DEDUP_REDIS_ADDR` | `""` | Redis/Valkey/Dragonfly address; empty disables tier-2 (local-only) |
| `-txid-dedup-aerospike-hosts` | `TXID_DEDUP_AEROSPIKE_HOSTS` | `""` | Comma-separated `host:port`; required when backend=`aerospike` |
| `-txid-dedup-aerospike-namespace` | `TXID_DEDUP_AEROSPIKE_NAMESPACE` | `cache` | Aerospike namespace (must be provisioned) |
| `-txid-dedup-aerospike-set` | `TXID_DEDUP_AEROSPIKE_SET` | `bsp` | Aerospike set |
| `-txid-dedup-prefix` | `TXID_DEDUP_PREFIX` | `bsp:tx:` | Must match the local listener's `-ingress-set-prefix` for collapsed deployments |
| `-txid-dedup-ttl` | `TXID_DEDUP_TTL` | `10m` | Range 1m – 30m typical |
| `-txid-dedup-local-cap` | `TXID_DEDUP_LOCAL_CAP` | `1048576` | 0 also disables the feature; prefer `-ingress-dedup=false` for clarity |

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

## Auto-Shard-Config (BRC-139)

Optional consumer of BRC-139 ShardManifest announcements. Default off
(legacy behavior unchanged); opt-in via `-manifest-consumer-enabled`.
Unlike the listener, the proxy does not currently join the beacon
group — enabling auto-config adds a dedicated beacon-receive socket.
See the [Automatic Shard Configuration Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#automatic-shard-configuration)
for the system-level design.

### `-manifest-consumer-enabled` / `MANIFEST_CONSUMER_ENABLED` (default: `false`)

Master switch. When true, the proxy opens an IPv6 multicast socket on
the beacon group (FFxx::B:FFFD) on the configured scope, decodes
incoming ShardManifest datagrams (MsgType 0x40; non-manifest MsgTypes
are silently dropped), and runs the manifest evaluator on a 1 s tick.

### `-manifest-bootstrap` / `MANIFEST_BOOTSTRAP` (default: `optional`)

`optional` ⇒ start with CLI/env values. `required` ⇒ refuse to bind
data-plane egress until quorum is reached for `ShardBits` (and
`SourceModeSSM` when SSM).

### `-pilot-quorum` / `PILOT_QUORUM` (default: `2`)

Minimum distinct authoritative announcers required for adoption. `1`
is permitted (logs a warning).

### `-pilot-hysteresis` / `PILOT_HYSTERESIS` (default: `0`)

Duration a candidate value must hold quorum before adoption. `0`
selects `2 × AnnounceInterval` of the candidate manifest.

### `-manifest-beacon-scope` / `MANIFEST_BEACON_SCOPE` (default: empty)

Multicast scope for the beacon-group join. Empty inherits `-scope`.

### `-manifest-beacon-port` / `MANIFEST_BEACON_PORT` (default: `9001`)

UDP port on which the proxy joins the beacon group to receive BRC-139
manifests. Matches the shard-manifest daemon's `-port`.

### `-live-resharding` / `LIVE_RESHARDING` (default: `false`)

Opt-in BRC-139 bridging mode. When false (default), a `ShardBits` or
`SourceModeSSM` adoption triggers a graceful restart (the manifest
applier writes into the internal restart-signal channel, which the
main signal-select handles as an early SIGTERM so the standard drain
path runs). When true, the applier installs a secondary
`forwarder.BridgingEngine` on the forwarder so the per-frame fast
path emits to BOTH the active and successor groups; the
listener-side egress dedup absorbs the duplicate frames.

### `-bridging-window` / `BRIDGING_WINDOW` (default: `0`)

Local floor on the bridging duration. `0` ⇒ honour the pilot's
`TransitionEpoch` verbatim.

## Helm chart

Flags are exposed under `.config` in the corresponding Helm chart's `values.yaml` — see the chart README for the covered set. See the chart repository for installation snippets and the `values.schema.json` for validation rules.

Chart: [`lightwebinc/shard-proxy-helm`](https://github.com/lightwebinc/shard-proxy-helm)

## BRC-148 BEEF object plane

BEEF is an **open ingress class**: submission records (leading `0xBEEF` tag —
the third grammar on the tx port beside framed magic and bare tx) and framed
`FrameVer 0x09` input are accepted on the public port regardless of these
knobs. Canonical spec: `bsv-multicast/docs/brc-148-shard-domain-beef-plane.md`.

| Flag / Env | Default | Description |
|------------|---------|-------------|
| `-beef-listen-port` / `BEEF_LISTEN_PORT` | `0` (off) | Optional dedicated single-class TCP lane (standard **8728**) for 5-tuple flow separation / load balancing — never admission. Included in the TCP-port collision check |
| `-beef-shard-bits` / `BEEF_SHARD_BITS` | `0` | BEEF plane width (band `0x1000 + 2^bits` groups); valid 0–12 (`0` = single group); must match listeners/retry |
| `-beef-max-object-bytes` / `BEEF_MAX_OBJECT_BYTES` | `1048576` | Maximum accepted object size (BRC-149's ingress MUST-bound); larger submissions are rejected and counted (`bsp_beef_submissions_total{result="oversize"}`) |

The forwarder expands one record into one stamped frame per topic
(`SubmitBEEF` → `ProcessBEEF`): HashKey = XXH64(sender ∥ banded groupIdx ∥
**zeros** — TopicID is excluded from the flow key per the spec), ingress
dedup claims the **(ContentID, TopicID) pair** as `SHA-256(ContentID ∥ TopicID)`,
and objects exceeding `-frag-mtu` fragment via BRC-130 with `OrigFrameVer 0x09`.

### Admission

- **Single-topic records only, by default.** The record grammar carries
  `topicCount` 1..15, but multi-topic fan-out (one object → N frames, up to
  15× amplification) is an authenticated capability per BRC-149. With no submit
  policy installed (`Forwarder.SetBEEFSubmitPolicy`, the open-ingress default)
  a record naming more than one topic is rejected
  (`bsp_beef_submissions_total{result="multi_topic"}`). A downstream build
  installs a policy that admits 2..15 topics per source and may lift the
  object bound for that source (never below `-beef-max-object-bytes`).
- **Submission results** (`bsp_beef_submissions_total{result}`): `ok`,
  `disabled`, `malformed`, `oversize`, `bad_marker`, `multi_topic`.
- **Pre-framed `0x09` input** must meet the same conformance as a record, on
  every acceptance path and before the dedup claim: object bound
  (`beef_oversize`), non-zero TopicID (`beef_no_topic`), BEEF marker
  (`beef_bad_marker`), and — for submitted, not relayed, frames — a ContentID
  that matches `SHA-256d(payload)` (`beef_bad_contentid`), so a submitter
  cannot pre-claim an object it does not have. All count in
  `bsp_packets_dropped_total{reason}`.
- **Rate budgets** (per source and whole-plane token buckets, objects and
  bytes; `beef_rate_source` / `beef_rate_plane`) are charged before the dedup
  claim and exempt relay/spine re-emission. Every budget is **off** in this
  build (no flags; `Forwarder.SetBEEFRateLimit` is the API a downstream build
  wires).
- **Dedup store.** By default BEEF claims share the transaction dedup store
  and its `-txid-dedup-prefix`. `Forwarder.SetBEEFDedup` gives the object
  plane its own store so an open-class flood cannot evict transaction claims;
  no flag exposes it in this build.
