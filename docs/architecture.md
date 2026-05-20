# Architecture

## Overview

bitcoin-shard-proxy receives BSV transaction frames (BRC-12, BRC-124, BRC-128, BRC-131, or BRC-132)
over UDP (and optionally TCP), derives a deterministic multicast group address from each
transaction's txid (or routes to a fixed control-plane group for BRC-131/BRC-132), then
retransmits the original bytes verbatim to all configured egress interfaces.

See [docs/protocol.md](protocol.md) for the complete wire format specification.

```text
sender  ──UDP/TCP──►  bitcoin-shard-proxy  ──UDP multicast──►  FF05::B:<shard>  (data plane)
                      (forwarder pipeline) ├─────────────────►  FF05::B:FFFE     (CtrlGroupControl, BRC-131)
                                           ├─────────────────►  FF05::B:FFFB     (CtrlGroupSubtreeAnnounce, BRC-132)
                                           └─────────────────►  FF05::B:FFFC     (CtrlGroupSubtreeGroupAnnounce, BRC-127)
```

## Shard Address Derivation

```text
groupIndex = (txid[0:4] as uint32 BE) >> (32 - shardBits)        // 16-bit max
IPv6 group = [FF0X:0:0:0:0:0:GroupID:groupIndex]                  // X = scope nibble
                                                                  // GroupID = 0x000B (IANA Bitcoin)
```

The top bits of the first four bytes of the txid are used as the group key.
Using top bits rather than modulo gives consistent-hashing: when `shardBits`
increases by 1, every existing group splits into exactly two child groups.
Subscribers join additional groups; existing subscriptions remain valid.

## Control Groups

BRC-131 and BRC-132 frames are routed to fixed control-plane multicast groups rather than
shard-derived data-plane groups. The reserved indices (top of the 16-bit space, above
`shard-bits` maximum of 15) are defined in `bitcoin-shard-common/shard/control.go`:

| Constant | Index | Address (site scope, group-id `0x000B`) | Purpose |
|---|---|---|---|
| `CtrlGroupBlockHeader` | 0xFFFA | FF05::B:FFFA | Block header egress channel (stripped BRC-131 headers) |
| `CtrlGroupSubtreeAnnounce` | 0xFFFB | FF05::B:FFFB | BRC-132 subtree data frames |
| `CtrlGroupSubtreeGroupAnnounce` | 0xFFFC | FF05::B:FFFC | BRC-127 subtree group announcements |
| `CtrlGroupBeacon` | 0xFFFD | FF05::B:FFFD | ADVERT beacon (BRC-126 discovery) |
| `CtrlGroupControl` | 0xFFFE | FF05::B:FFFE | BRC-131 block control frames |

The `-shard-bits` limit of 15 ensures user shard indices (`0x0000`–`0x7FFF`) never overlap
with control groups (`0xFFFA`–`0xFFFE`).

## BRC-131 Block Control Frames (FrameVerV4)

BRC-131 frames may arrive via UDP or TCP ingress. UDP workers inspect version byte `0x04`
and call `Forwarder.ProcessBlock`; `handleConn` does the same on the TCP path.

`ProcessBlock`:
- Validates via `frame.DecodeBlock`.
- Stamps `HashKey` as `XXH64(senderIPv6 ∥ 0xFFFE ∥ zeros)` and `SeqNum` as a monotonic
  per-flow counter when both are zero in the incoming frame.
- Forwards the raw bytes verbatim to `CtrlGroupControl` (`FF0X::B:FFFE`) on all egress interfaces.
- If the payload exceeds the BRC-130 fragment threshold, calls `fragmentBlock()` instead.
  Each fragment carries `OrigFrameVer=0x04` so listeners route the reassembled payload to
  their block processing path.

`ProcessAnchor`:
- Validates via `frame.DecodeAnchor`.
- Stamps `HashKey` as `XXH64(senderIPv6 ∥ 0xFFFE ∥ zeros)` and `SeqNum` as a monotonic
  per-flow counter when both are zero in the incoming frame.
- Forwards the raw bytes verbatim to `CtrlGroupControl` (`FF0X::B:FFFE`) on all egress interfaces.
- No BRC-130 fragmentation (anchor transactions are expected to be small).

Two `MsgType` values are defined (byte 7 of the header):

| MsgType | Value | Payload |
|---|---|---|
| BlockAnnounce | 0x01 | 80-byte block header + CoinbaseTxID + subtree hashes |
| CoinbaseTx | 0x02 | Raw serialised coinbase transaction |

## BRC-132 Subtree Data Frames (FrameVerV5)

BRC-132 frames may arrive via UDP or TCP ingress; version byte `0x05`. UDP workers and
`handleConn` both call `Forwarder.ProcessSubtreeData`.

## BRC-134 Chained Anchor Transaction Frames (FrameVerV6)

BRC-134 frames may arrive via UDP or TCP ingress; version byte `0x06`. Anchor
transactions are the root of a chain of dependent transactions and must reach every
subscriber regardless of which shard their TxID would otherwise hash to. UDP workers and
`handleConn` both call `Forwarder.ProcessAnchor`.

`ProcessSubtreeData`:
- Validates via `frame.DecodeSubtreeData`.
- Stamps `HashKey` as `XXH64(senderIPv6 ∥ 0xFFFB ∥ subtreeID)` and `SeqNum` as a monotonic
  per-flow counter. The flow key incorporates `subtreeID` so each distinct subtree is
  sequenced independently.
- Forwards the raw bytes to `CtrlGroupSubtreeAnnounce` (`FF0X::B:FFFB`) on all egress interfaces.
- If the payload exceeds the BRC-130 fragment threshold, calls `fragmentSubtreeData()`.
  Each fragment carries `OrigFrameVer=0x05` and preserves the `MsgType` byte (offset 7).

Two `MsgType` values are defined:

| MsgType | Value | Payload |
|---|---|---|
| HashesOnly | 0x01 | 32 bytes per subtree node (SHA256 hash only) |
| FullNodes | 0x02 | 48 bytes per subtree node (hash + fee + size metadata) |

## Multi-CPU Design

Each UDP worker goroutine owns one ingress socket bound via `SO_REUSEPORT` plus
one egress socket per configured interface. The kernel distributes incoming
datagrams across all workers with no userspace coordination. Forwarding logic
is centralised in the shared `forwarder.Forwarder`.

### TCP ingress

When `-tcp-listen-port` is non-zero, a single `TCPIngress` goroutine accepts
connections and dispatches each connection to a per-connection goroutine. TCP
and UDP share the same `forwarder.Forwarder` and egress targets.

`handleConn` reads 44 bytes first (minimum header), then branches on the version byte:

| Version byte | Frame type | Header total | Additional read | Dispatch |
|---|---|---|---|---|
| `0x01` (BRC-12) | Transaction | 44 bytes | `PayLen` bytes | `Process` |
| `0x02` (BRC-124/BRC-128) | Transaction | 92 bytes | 48 more + `PayLen` | `Process` |
| `0x04` (BRC-131) | Block control | 92 bytes | 48 more + `PayLen` | `ProcessBlock` |
| `0x05` (BRC-132) | Subtree data | 92 bytes | 48 more + `PayLen` | `ProcessSubtreeData` |
| `0x06` (BRC-134) | Anchor tx | 92 bytes | 48 more + `PayLen` | `ProcessAnchor` |
| `0x07` (BRC-127) | SubtreeAnnounce | 64 bytes | 20 more (no payload) | `ForwardControl` |

```
senders (UDP/TCP)          proxy (N UDP workers + 1 TCP listener)
─────────────────          ─────────────────────────────────────
tx_a  ──UDP──▶ [worker 0] ─▶ forwarder ─▶ FF05::B:3    ──▶ sub_X   (shard, data-plane)
tx_b  ──UDP──▶ [worker 1] ─▶ forwarder ─▶ FF05::B:1    ──▶ sub_Y
blk_c ──UDP──▶ [worker N] ─▶ forwarder ─▶ FF05::B:FFFE ──▶ sub_Z   (CtrlGroupControl, BRC-131)
blk_d ──TCP──▶ [tcp conn] ─▶ forwarder ─▶ FF05::B:FFFE ──▶ sub_Z
anc_e ──UDP──▶ [worker N] ─▶ forwarder ─▶ FF05::B:FFFE ──▶ sub_Z   (CtrlGroupControl, BRC-134)
sub_f ──TCP──▶ [tcp conn] ─▶ forwarder ─▶ FF05::B:FFFB ──▶ sub_W   (CtrlGroupSubtreeAnnounce, BRC-132)
```

## Wire Format

### BRC-124/BRC-128 (current — 92 bytes)

```text
Offset  Size  Align  Field
------  ----  -----  -----
     0     4   —     Network magic         0xE3E1F3E8
     4     2   —     Protocol ver          0x02BF
     6     1   —     Frame version         0x02 (BRC-124/BRC-128)
     7     1   —     Reserved              0x00
     8    32   8B    Transaction ID        raw 256-bit txid (internal byte order)
    40     8   8B    HashKey               stable per-flow XXH64 identifier; 0 = unset
    48     8   8B    SeqNum                monotonic per-flow counter; 0 = unset
    56    32   8B    Subtree ID            32-byte batch identifier; zeros = unset
    88     4   8B    Payload length        uint32 BE
    92     *   —     BSV tx payload        BRC-12 raw or BRC-30 EF (BRC-128)
```

### BRC-12 (legacy — 44 bytes, accepted, forwarded verbatim)

```text
Offset  Size  Align  Field            Value / notes
------  ----  -----  -----            -------------
     0     4   —     Network magic    0xE3E1F3E8
     4     2   —     Protocol ver     0x02BF = 703
     6     1   —     Frame version    0x01
     7     1   —     Reserved         0x00
     8    32   —     Transaction ID   raw 256-bit txid (internal byte order)
    40     4   —     Payload length   uint32 BE
    44     *   —     BSV tx payload   raw serialised transaction bytes
```

BRC-12 frames carry no `HashKey`, `SeqNum`, or `SubtreeID` fields.
The proxy accepts them and forwards the original bytes unchanged.

### BRC-131 (FrameVerV4 — 92-byte header, block control)

Layout is identical to BRC-124/BRC-128 except for the version byte (0x04), the MsgType
in the Reserved field (byte 7), and the ContentID semantics (block hash or coinbase txid
instead of a transaction ID).

```text
Offset  Size  Align  Field          Value / notes
------  ----  -----  -----          -------------
     0     4   —     Network magic  0xE3E1F3E8
     4     2   —     Protocol ver   0x02BF
     6     1   —     Frame version  0x04 (BRC-131)
     7     1   —     MsgType        0x01 = BlockAnnounce, 0x02 = CoinbaseTx
     8    32   8B    ContentID      Block hash (Announce) or CoinbaseTxID (Coinbase)
    40     8   8B    HashKey        Stamped by proxy; XXH64(senderIPv6 ∥ 0xFFFE ∥ zeros)
    48     8   8B    SeqNum         Monotonic per (sender, 0xFFFE, zeros) flow; 0 = unset
    56    32   8B    Reserved32     All zeros
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        BlockAnnounce or CoinbaseTx payload
```

### BRC-132 (FrameVerV5 — 92-byte header)

```text
Offset  Size  Align  Field          Value / notes
------  ----  -----  -----          -------------
     0     4   —     Network magic  0xE3E1F3E8
     4     2   —     Protocol ver   0x02BF
     6     1   —     Frame version  0x05 (BRC-132)
     7     1   —     MsgType        0x01 = HashesOnly, 0x02 = FullNodes
     8    32   8B    SubtreeID      SHA-256 Merkle root; also used as ContentID
    40     8   8B    HashKey        Stamped by proxy; XXH64(senderIPv6 ∥ 0xFFFB ∥ subtreeID)
    48     8   8B    SeqNum         Monotonic per (sender, 0xFFFB, subtreeID) flow; 0 = unset
    56    32   8B    LayoutPad32    All zeros
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        Subtree node data
```

The flow key includes `SubtreeID` so each distinct subtree is sequenced independently.

### BRC-134 (FrameVerV6 — 92-byte header, anchor transaction)

Layout is identical to BRC-124/BRC-128 except for the version byte (`0x06`). The
`LayoutPad32` field at bytes 56–87 is always zeros — anchor frames have no subtree scope.

```text
Offset  Size  Align  Field          Value / notes
------  ----  -----  -----          -------------
     0     4   —     Network magic  0xE3E1F3E8
     4     2   —     Protocol ver   0x02BF
     6     1   —     Frame version  0x06 (BRC-134)
     7     1   —     Reserved       0x00
     8    32   8B    TxID           Anchor transaction ID (SHA256d, internal byte order)
    40     8   8B    HashKey        Stamped by proxy; XXH64(senderIPv6 ∥ 0xFFFE ∥ zeros)
    48     8   8B    SeqNum         Monotonic per (sender, 0xFFFE, zeros) flow; 0 = unset
    56    32   8B    LayoutPad32    All zeros
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        Raw serialised anchor transaction
```

## Hot Path

The hot path below applies to BRC-12/BRC-124/BRC-128 frames received via UDP:

1. `frame.Decode(raw)` — extract the TxID; drop on bad magic or unknown version.
2. **HashKey/SeqNum stamp (BRC-124/BRC-128 only)** — if `raw[48:56]` (SeqNum) is
   non-zero the sender has pre-stamped the frame; forward verbatim. Otherwise
   stamp `raw[40:48]` (HashKey) as `XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)` and
   `raw[48:56]` (SeqNum) as a monotonic per-flow counter, in-place. BRC-12 frames
   are always untouched.
3. `WriteTo(raw)` — write the raw bytes to every egress target.

No re-encoding, no per-worker encode buffer.

BRC-131, BRC-132, and BRC-134 frames received via UDP or TCP follow parallel paths
through `ProcessBlock`, `ProcessSubtreeData`, and `ProcessAnchor` respectively.
These functions perform the same in-place HashKey/SeqNum stamping (and optional
BRC-130 fragmentation for BRC-131/BRC-132), but route to fixed control-plane
groups rather than shard-derived addresses.

## Graceful Shutdown

Shutdown proceeds in two phases when `SIGINT` or `SIGTERM` is received:

1. **Drain** — `rec.SetDraining()` is called immediately, flipping `/readyz`
   to `503` so load balancers stop routing new connections. If `-drain-timeout`
   is non-zero, the process sleeps for that duration while workers continue
   forwarding in-flight packets.

2. **Quiesce** — The `done` channel is closed. Each UDP worker and the TCP
   listener close their ingress sockets, unblocking any pending `ReadFrom` /
   `Accept` calls. Active TCP connections are force-closed so `handleConn`
   goroutines do not hang. `main` waits for all goroutines via
   `sync.WaitGroup`, then flushes the OTLP exporter before returning.

## Package Structure

```
bitcoin-shard-proxy/
  main.go            entry point; wires config → engine → forwarder → workers
  config/            runtime configuration (flags + env vars + validation)
  forwarder/         decode → zero-copy verbatim forward pipeline;
                     Process (BRC-12/BRC-124/BRC-128), ProcessBlock (BRC-131),
                     ProcessSubtreeData (BRC-132), BRC-130 fragmentation
  worker/            per-CPU SO_REUSEPORT UDP ingress loop with frame-version dispatch
                     for BRC-131/BRC-132/BRC-134 (worker.go);
                     TCP ingress listener with BRC-127 routing (tcp.go)
  metrics/           OTel + Prometheus instrumentation
```

Protocol primitives are provided by
[`github.com/lightwebinc/bitcoin-shard-common`](https://github.com/lightwebinc/bitcoin-shard-common):

```
bitcoin-shard-common/
  frame/             BRC-12/BRC-124/BRC-128/BRC-131/BRC-132 wire format: Decode, Encode, constants
  shard/             txid → group index → IPv6 multicast address derivation;
                     control group constants and ControlGroupAddr
  seqhash/           XXH64-based hash chain stamping
```
