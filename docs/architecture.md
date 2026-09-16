# Architecture

## Overview

shard-proxy receives BSV transaction frames (BRC-12, BRC-124, BRC-128, or
BRC-134), bare header-stripped transactions, and BRC-148/149 BEEF objects
(submission records or FrameVer `0x09`) over UDP (and optionally TCP), derives
a deterministic multicast group address from each transaction's txid (the
TopicID on the BEEF plane; a fixed control-plane group for BRC-134 anchors),
then retransmits the original bytes verbatim to all configured egress
interfaces. An already-coalesced BRC-142 bundle arriving over UDP is relayed
verbatim to the group in its header. Block (BRC-131) and subtree-data (BRC-132) frames
never enter through the public ingress — those sockets are transaction-class
and drop them (`bsp_privileged_frame_rejected_total`). They originate from the
privileged BRC-143/144 push lanes (`-subtree-listen-port` / `-block-listen-port`),
which reframe push objects into BRC-132/BRC-131 before the same forwarder
pipeline routes them to the control groups.

BRC wire formats live in
[bsv-multicast/docs/](https://github.com/lightwebinc/bsv-multicast/tree/main/docs).

```text
tx sender ──UDP/TCP (8725)──────►  shard-proxy  ──UDP multicast──►  FF05::B:<shard>  (data plane, configurable scope)
BEEF ─(8725, or 8728 lane)──────►  (forwarder pipeline) ├────────►  FF05::B:1<xxx>   (BRC-148 BEEF plane, IDX 0x1000 + shard(TopicID))
miner ──push lanes (8726/8727)──►                       ├────────►  FF05::B:FFFE     (GroupBlockBroadcast, BRC-131/134, at the configured scope)
                                                        ├────────►  FF05::B:FFFB     (GroupSubtreeDataAnnounce, BRC-132)
                                                        └────────►  FF05::B:FFFC     (GroupSubtreeGroupAnnounce, BRC-127)
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

The scope nibble `X` is derived from `(source-mode, scope)` via
`shard.Prefix()`:

| Mode | Site scope (intra-domain) | Global scope (inter-domain) |
| ---- | ------------------------- | --------------------------- |
| ASM  | `FF05::B:idx` (default)   | rejected (RFC 8815)         |
| SSM  | `FF35::B:idx`             | `FF3E::B:idx`               |

`-source-mode=ssm` switches every data-plane group to the `FF3x::/32`
range (RFC 4607). The egress socket is bound to `-bind-source`
(a distinct IPv6 per replica) so SSM receivers can pre-declare this
proxy in their `(S,G)` join calls — anycast / ECMP-shared sources
break PIM-SSM RPF. See the
[SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm)
for fabric prerequisites (PIM-SSM, MLDv2, raised `mld_max_msf`).

## Control Groups

BRC-131 and BRC-132 frames are routed to fixed control-plane multicast groups rather than
shard-derived data-plane groups. The reserved indices (top of the 16-bit space, far above
the BRC-129 shard zone `0x0000`–`0x0FFF`) are defined in `shard-common/shard/control.go`:

| Constant | Index | Canonical Address (group-id `0x000B`) | Purpose |
|---|---|---|---|
| `GroupBlockHeader` | 0xFFFA | egress-scope `FF0X::<egress-gid>:FFFA` | Block header egress channel (BRC-135) — emitted by shard-listener's header egress; the proxy never sends on it |
| `GroupSubtreeDataAnnounce` | 0xFFFB | FF05::B:FFFB (data-plane scope) | BRC-132 subtree data frames |
| `GroupSubtreeGroupAnnounce` | 0xFFFC | FF05::B:FFFC (data-plane scope) | BRC-127 subtree group announcements |
| `GroupBeacon` | 0xFFFD | FF05::B:FFFD (site) / FF0E::B:FFFD (global) | ADVERT beacon (BRC-126 discovery) + BRC-139 shard manifests |
| `GroupBlockBroadcast` | 0xFFFE | FF05::B:FFFE (data-plane scope; FF0E::B:FFFE when deployed global) | BRC-131 block control + BRC-133 coinbase + BRC-134 anchor frames |

BRC-129 names global scope (FF0E) as the deployment posture for `GroupBlockBroadcast`, because block headers, coinbase, and anchor transactions must reach every subscriber across organisational boundaries. The implementation derives the address from the configured `-scope` like every other control group: a site-scoped deployment emits `FF05::B:FFFE`, and an inter-domain one sets `-scope global`.

Per BRC-129 zoning, shard group indices are bounded to `0x0000`–`0x0FFF` — `-shard-bits`
is at most 12 for conformant deployments (the flag validator enforces `[1, 12]`) — so
user shard indices never overlap the control groups (`0xFFFA`–`0xFFFE`).

## BRC-131 Block Control Frames (FrameVerV4)

BRC-131 frames do not arrive via the public UDP/TCP ingress — those sockets are
transaction-class, and `DispatchClass` drops version byte `0x04` frames there
(counted in `bsp_privileged_frame_rejected_total`). They originate from the
privileged block push lane (`-block-listen-port`, standard 8727): `objingress`
reframes each BRC-144 push object into a BRC-131 frame and dispatches it as
privileged, routing to `Forwarder.ProcessBlock`.

`ProcessBlock`:
- Validates via `frame.DecodeBlock`.
- Stamps `HashKey` as `XXH64(senderIPv6 ∥ 0xFFFE ∥ zeros)` and `SeqNum` as a monotonic
  per-flow counter when both are zero in the incoming frame.
- Forwards the raw bytes verbatim to `GroupBlockBroadcast` (`FF0X::B:FFFE`) on all egress interfaces.
- If the payload exceeds the BRC-130 fragment threshold, calls `fragmentBlock()` instead.
  Each fragment carries `OrigFrameVer=0x04` so listeners route the reassembled payload to
  their block processing path.

`ProcessAnchor`:
- Validates via `frame.DecodeAnchor`.
- Stamps `HashKey` as `XXH64(senderIPv6 ∥ 0xFFF9 ∥ zeros)` and `SeqNum` as a monotonic
  per-flow counter when both are zero in the incoming frame. The virtual index `0xFFF9`
  gives anchor frames an independent flow identity from BRC-131 block frames.
- Forwards the raw bytes verbatim to `GroupBlockBroadcast` (`FF0X::B:FFFE`) on all egress interfaces.
- No BRC-130 fragmentation (anchor transactions are expected to be small).

Two `MsgType` values are defined (byte 7 of the header):

| MsgType | Value | Payload |
|---|---|---|
| BlockAnnounce | 0x01 | 80-byte block header + CoinbaseTxID + subtree hashes |
| CoinbaseTx | 0x02 | Raw serialised coinbase transaction |

## BRC-132 Subtree Data Frames (FrameVerV5)

BRC-132 frames (version byte `0x05`) are likewise dropped on the public
transaction-class ingress. They originate from the privileged subtree push lane
(`-subtree-listen-port`, standard 8726): `objingress` reframes each BRC-143 push
object into a BRC-132 frame and dispatches it as privileged, routing to
`Forwarder.ProcessSubtreeData`.

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
- Forwards the raw bytes to `GroupSubtreeDataAnnounce` (`FF0X::B:FFFB`) on all egress interfaces.
- If the payload exceeds the BRC-130 fragment threshold, calls `fragmentSubtreeData()`.
  Each fragment carries `OrigFrameVer=0x05` and preserves the `MsgType` byte (offset 7).

Two `MsgType` values are defined:

| MsgType | Value | Payload |
|---|---|---|
| HashesOnly | 0x01 | 32 bytes per subtree node (SHA256 hash only) |
| FullNodes | 0x02 | 48 bytes per subtree node (hash + fee + size metadata) |

## BRC-142 Bundle (FrameVerV8)

BRC-142 is the inverse of BRC-130 fragmentation: it packs many small
BRC-124/BRC-128 transactions of one `(sender, group, subtree)` flow into a
single *bundle* datagram (`FrameVer 0x08`, 66-byte header) to cut egress
packets-per-second. It is opt-in via `-coalesce` (off by default). The wire
format is decoded by `shard-common/bundle` (not `frame.Decode`, which returns
`ErrBadVer` for `0x08`); it is specified once in the canonical
[BRC-142 spec](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md#3a-brc-142-coalescing-bundle-frame-format)
and the public
[bsv-multicast/docs/brc-142-coalescing-frame.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-142-coalescing-frame.md)
— not restated here.

**Within-batch origin coalescing.** Coalescing is bounded to a single receive
batch. As a worker dispatches a batch, eligible transactions are bucketed by
their `(sender, group, subtree)` flow in a per-`Egress` `coalBuffer`
(`forwarder/coalesce.go`); member payloads alias the worker's reused receive
buffers. At batch end — immediately before `Egress.Flush` overwrites those
buffers — `Forwarder.FlushCoalesced` packs each bucket into one or more owned
bundle datagrams (up to `-coalesce-max-bytes` / `-coalesce-max-members`) and
enqueues them. Each bundle draws its `HashKey`/`SeqNum` from the **same
per-flow striped counter** (`nextSeq`) that stamps individual frames, so a
flow's bundles interleave contiguously with any individual frames it also
emits; a listener gap-tracks the `(group, subtree, HashKey)` flow uniformly
regardless of frame version, and BRC-126 NACK/retry treats a bundle as one "fat
frame."

**Verbatim spine relay (origin-only rule).** Coalescing runs **only at the
emitting origin** — the collapsed / ingress proxy that first sees the individual
transactions. A regional spine that *relays* an already-coalesced bundle takes
the `Forwarder.ProcessBundle` path: it does a cheap structural check (BSV magic +
self-consistent `PayloadLen`), reads the destination `GroupIdx` from the bundle
header (offset 56 — a bundle is bound to the single group it was built for, not
re-derived from a member TxID), and re-emits the datagram **unchanged**. A bundle
is never re-coalesced, re-stamped, or split in this proxy: one bundle in → one
multicast datagram out. Decoalesce (split back to
individual frames) and re-bucket (re-align to the receiver's shard generation)
happen at the **delivery edge** (listener), not here.

## Multi-CPU Design

Each UDP worker goroutine owns one ingress socket bound via `SO_REUSEPORT`
plus one egress socket per configured interface. The kernel distributes
incoming datagrams across all workers with no userspace coordination.
Forwarding logic is centralised in the shared `forwarder.Forwarder`.

Workers process datagrams in batches: each iteration pulls up to
`-recv-batch` ingress packets via `ipv6.PacketConn.ReadBatch` (one
`recvmmsg(2)` syscall on Linux), dispatches each packet through the
forwarder pipeline which enqueues outbound datagrams into a per-worker
`forwarder.Egress`, then flushes that queue via `ipv6.PacketConn.WriteBatch`
(one `sendmmsg(2)` syscall per egress interface). Per-flow `SeqNum`
counters live in a 64-stripe map keyed on a hash of the sender IP, so
concurrent workers contend on independent shards; once a flow's counter
exists the increment is lock-free via `atomic.AddUint64`.

### TCP ingress

When `-tcp-listen-port` is non-zero, a single `TCPIngress` goroutine accepts
connections and dispatches each connection to a per-connection goroutine. TCP
and UDP share the same `forwarder.Forwarder` and egress targets.

`handleConn` reads 44 bytes first (minimum header), then branches on the version byte:

| Version byte | Frame type | Header total | Additional read | Dispatch |
|---|---|---|---|---|
| `0x01` (BRC-12) | Transaction | 44 bytes | `PayLen` bytes | `Process` |
| `0x02` (BRC-124/BRC-128) | Transaction | 92 bytes | 48 more + `PayLen` | `Process` |
| `0x04` (BRC-131) | Block control | 92 bytes | 48 more + `PayLen` | dropped — `DispatchClass` rejects privileged frames on the transaction-class port |
| `0x05` (BRC-132) | Subtree data | 92 bytes | 48 more + `PayLen` | dropped — `DispatchClass` rejects privileged frames on the transaction-class port |
| `0x06` (BRC-134) | Anchor tx | 92 bytes | 48 more + `PayLen` | `ProcessAnchor` |
| `0x09` (BRC-149) | BEEF object | 92 bytes | 48 more + `PayLen` (bounded by `-beef-max-object-bytes`) | `ProcessBEEF` |
| `0x30` (MsgType, BRC-127) | SubtreeGroupAnnounce | 64 bytes | 20 more (no payload) | `ForwardControl` |

> The dispatcher branches on `hdrBuf[6]`. For BRC-12/124/131/132/134/149 this byte is the Frame Version (`0x01`/`0x02`/`0x04`/`0x05`/`0x06`/`0x09`); for BRC-127 it is the MsgType byte (`0x30 = MsgTypeSubtreeGroupAnnounce`). Any other version byte (including a BRC-142 bundle, `0x08`, which is relayed over UDP only) closes the connection.
>
> The framed table applies only to a connection whose first bytes are the BSV magic. Grammar is detected once per connection: a leading `0xBEEF` tag selects a BEEF submission-record stream (`objfmt.ClassBEEF` reader → `SubmitBEEF`), and anything else selects a bare-transaction stream (`objfmt.ClassTx` reader → `DispatchBareTx`).

```
senders                       proxy (N UDP workers + 1 TCP listener + push lanes)
───────                       ──────────────────────────────────────────────────
tx_a  ──UDP 8725──▶ [worker 0]  ─▶ forwarder ─▶ FF05::B:3    ──▶ sub_X   (shard, data-plane)
tx_b  ──UDP 8725──▶ [worker 1]  ─▶ forwarder ─▶ FF05::B:1    ──▶ sub_Y
anc_c ──UDP 8725──▶ [worker N]  ─▶ forwarder ─▶ FF05::B:FFFE ──▶ sub_Z   (GroupBlockBroadcast, BRC-134)
beef  ──UDP 8725──▶ [worker 1]  ─▶ SubmitBEEF → 0x09 ─▶ forwarder ─▶ FF05::B:1000 ──▶ sub_V   (BRC-148 BEEF plane, -beef-shard-bits 0)
blk_d ──TCP 8727──▶ [push lane] ─▶ BRC-144 → BRC-131 ─▶ forwarder ─▶ FF05::B:FFFE ──▶ sub_Z   (GroupBlockBroadcast)
sub_e ──TCP 8726──▶ [push lane] ─▶ BRC-143 → BRC-132 ─▶ forwarder ─▶ FF05::B:FFFB ──▶ sub_W   (GroupSubtreeDataAnnounce)
blk_f ──UDP 8725──▶ [worker N]  ─▶ dropped (privileged frame on transaction-class ingress)
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

BRC-12 frames carry no `HashKey`, `SeqNum`, or `SubtreeID` fields. By default the
proxy accepts them and forwards the original bytes unchanged. Under `-require-ef`
(EF-native ingress) legacy BRC-12 is **rejected** — it carries a raw transaction
and the fabric is Extended-Format-only. See
[Transaction ingress: framed, bare, and EF-native](#transaction-ingress-framed-bare-and-ef-native).

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
    40     8   8B    HashKey        Stamped by proxy; XXH64(senderIPv6 ∥ 0xFFF9 ∥ zeros)
    48     8   8B    SeqNum         Monotonic per (sender, 0xFFF9, zeros) flow; 0 = unset
    56    32   8B    LayoutPad32    All zeros
    88     4   —     PayloadLen     uint32 BE
    92     *   —     Payload        Raw serialised anchor transaction
```

## Transaction ingress: framed, bare, and EF-native

The transaction ingress port (`-udp-listen-port`, default 8725) accepts a
submission in three shapes, distinguished by the leading bytes:

- **Framed** — a BRC-124/BRC-128 (or legacy BRC-12) frame, which begins with the
  BSV network magic `0xE3E1F3E8`. Decoded, dedup-claimed by TxID, stamped, and
  forwarded — the relay hot path.
- **Bare** — a header-stripped transaction (no frame). The magic is absent, so
  `DispatchClass` routes it to the bare path, wraps it into an unstamped
  BRC-124/128 frame, and drives it through the same forward path. One
  transaction per datagram. This retires the need for a separate raw-tx port:
  submitters send bare transactions to the same 8725 as framed traffic.
- **BEEF submission record** — a leading `0xBEEF` record tag (BRC-149).
  `DispatchClass` routes it to `SubmitBEEF`, which expands the record into one
  stamped `FrameVer 0x09` frame per submitted topic. BEEF is an open class, so
  records are admitted on every ingress class.

The magic pre-check is a single `uint32` compare in the same cache line as the
frame-version byte the router already reads, so the framed relay hot path is
byte-identical; the bare branch is cold (submissions only).

### EF-native ingress (`-require-ef`)

Teranode requires **Extended Format** (EF — the BRC-30 marker
`00 00 00 00 00 EF` at payload bytes 4–9): a raw transaction (BRC-12) omits each
input's funding value and locking script, which the stateless fabric cannot
supply (extension needs a UTXO lookup — a wallet/submitter concern). With
`-require-ef` set the ingress is EF-native: a transaction must be EF, or it is
rejected —

- a legacy BRC-12 (44-byte, FrameVerV1) frame,
- a raw BRC-124 (92-byte, FrameVerV2 without the marker) frame, and
- a bare non-EF transaction

are dropped (`bsp` packet-dropped counters `reason=ingress_not_ef` /
`bare_tx_not_ef`). When `-require-ef` is off (default) both raw and EF are
accepted and forwarded verbatim (legacy contract preserved). The `requireEF`
guard short-circuits to a single predicted branch when off, so there is no
measurable hot-path cost either way (`forwarder` micro-benchmarks: relay and
submission both within run-to-run noise of baseline).

**Stamped frames are not exempt.** The check used to skip frames carrying a
`SeqNum`, on the reasoning that a relayed frame was validated at its own
ingress. Nothing distinguished that from a submitter setting a non-zero
`SeqNum`, which made the EF posture opt-out with a one-byte edit. A lane that
legitimately carries relayed frames now declares itself with
`-allow-stamped-ingress`; every other lane rejects stamped input outright
(`reason=stamped_ingress`). See
[Stamped ingress](configuration.md#stamped-ingress).

Because a raw transaction and its extended form share the **same TxID** (the ID
is over the standard serialisation), a raw→EF "re-transmit" service on the fabric
would collide with ingress TxID dedup; EF must therefore be produced at the
wallet, at signing time, where the input UTXOs are already in hand.

## Hot Path

The hot path below applies to BRC-12/BRC-124/BRC-128 frames received via UDP:

1. `frame.Decode(raw)` — extract the TxID; drop on bad magic or unknown version.
2. **HashKey/SeqNum stamp (BRC-124/BRC-128 only)** — if `raw[48:56]` (SeqNum) is
   non-zero the sender has pre-stamped the frame; forward verbatim. Otherwise
   stamp `raw[40:48]` (HashKey) as `XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)` and
   `raw[48:56]` (SeqNum) as a monotonic per-flow counter, in-place. BRC-12 frames
   are always untouched.
3. `Egress.EnqueueData(raw)` — fan the raw bytes out into the worker's
   outbound queue, one entry per egress interface.
4. After every packet in the current receive batch has been processed,
   `Egress.Flush()` dispatches one `WriteBatch` (sendmmsg) per target.

No re-encoding, no per-worker encode buffer. The verbatim path references
the receive-batch buffers directly; buffer reuse is safe because Flush
completes before the next ReadBatch overwrites the same memory.

BRC-134 anchor frames received via UDP or TCP follow a parallel path through
`ProcessAnchor`. BRC-131 and BRC-132 frames follow the same pattern through
`ProcessBlock` and `ProcessSubtreeData`, but enter only via the privileged
push lanes — the public ingress rejects them. These functions perform the same
in-place HashKey/SeqNum stamping (and optional BRC-130 fragmentation for
BRC-131/BRC-132), but route to fixed control-plane groups rather than
shard-derived addresses.

## Graceful Shutdown

Shutdown proceeds in two phases when `SIGINT` or `SIGTERM` is received:

1. **Drain** — `rec.SetDraining()` is called immediately, flipping `/readyz`
   to `503` so load balancers stop routing new connections. If `-drain-timeout`
   is non-zero, the process sleeps for that duration while workers continue
   forwarding in-flight packets.

2. **Quiesce** — The `done` channel is closed. Each UDP worker and the TCP
   listener close their ingress sockets, unblocking any pending `ReadBatch` /
   `Accept` calls. Each UDP worker runs a deferred `Egress.Flush()` so the
   last receive batch's egress is not lost. Active TCP connections are
   force-closed so `handleConn` goroutines do not hang. `main` waits for
   all goroutines via `sync.WaitGroup`, then flushes the OTLP exporter
   before returning.

## BRC-139 Manifest Consumer (auto-shard-config)

Off by default; enabled with `-manifest-consumer-enabled`. When on, the
proxy opens a **dedicated beacon-receive socket** on the beacon group
(`FFxx::B:FFFD`) — unlike the listener, the proxy is not otherwise a
multicast subscriber — decodes BRC-139 ShardManifest datagrams into the
shared `shard-common/manifest` `Registry`, and runs the `Evaluator` on a
1 s tick. Adoption requires `≥ -pilot-quorum` distinct `Authoritative`
announcers agreeing for the hysteresis window; manual CLI values always
win. With `-manifest-bootstrap=required` the proxy fails closed: it
refuses to bind data-plane egress until `ShardBits` (and `SourceModeSSM`
under SSM) reach quorum.

Two adoption modes, selected by `-live-resharding`:

- **Restart-on-adopt (default).** A `ShardBits`/`SourceModeSSM` change
  makes the `manifest` applier write the internal restart-signal
  channel, which `main` handles as an early SIGTERM so the standard
  [Graceful Shutdown](#graceful-shutdown) drain runs and the
  orchestrator rolls the pod with the new parameters.
- **Bridging (opt-in).** The applier installs a `forwarder.BridgingEngine`
  so the per-frame fast path dual-emits to BOTH the active and successor
  groups for the bridging window; listener-side egress dedup absorbs the
  duplicates. Cutover follows the pilot's Successor-block `TransitionEpoch`
  (floored by `-bridging-window`).

Flag reference and fail-closed rules: [configuration.md](configuration.md#auto-shard-config-brc-139).
System-level design: [Automatic Shard Configuration Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#automatic-shard-configuration).

## Logging & Tracing

The proxy uses the shared `shard-common/logging` package: `main` calls
`logging.Init` once, which installs a process-wide `slog` default carrying the
`service.{name,instance.id,version}` identity triple (the same tuple the OTLP
metrics resource attributes use, so logs and metrics join on shared
dimensions). `-log-format json` emits one JSON object per line on stdout for
fleet aggregation; `text` (default) is human-readable on stderr. `-log-level`
is runtime-togglable without a restart via `POST /loglevel?level=<lvl>` on the
metrics server and via SIGHUP (toggles debug↔configured level).

At startup the proxy emits a one-shot `host.inventory` event (OS, CPU, memory,
per-NIC facts incl. IPv4+IPv6, multicast sysctls) and mirrors the slim numerics
as a `bsp_host_info` gauge.

**Category-8 OS/NIC logs** (the kernel conditions metrics show only as counts)
are emitted, throttled, at the syscall sites that observe them:

- `forwarder/egress.go` — `Egress.Flush` classifies `WriteBatch`/`sendmmsg`
  errors, logging `ENOBUFS` (kernel send-buffer / qdisc backpressure) with
  errno + dropped count, throttled per interface (log-once-then-count).
- `worker/worker.go` — warns when the kernel clamps `SO_RCVBUF` below the
  requested size (undersized `net.core.rmem_max`).

**Tracing** is opt-in (`-trace-sampling > 0` with an `-otlp-endpoint`) and
control-plane only — the forwarder receive/send loops take no span. Export is
out-of-process via the collector. See the
[Unified Logging Plan](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md).

## Package Structure

```
shard-proxy/
  main.go            entry point; wires config → engine → forwarder → workers
  config/            runtime configuration (flags + env vars + validation)
  forwarder/         decode → zero-copy verbatim forward pipeline;
                     Process (BRC-12/BRC-124/BRC-128), ProcessBlock (BRC-131),
                     ProcessSubtreeData (BRC-132), ProcessAnchor (BRC-134),
                     BRC-130 fragmentation; per-worker Egress batcher with
                     sync.Pool fragment buffers (egress.go); opt-in
                     BridgingEngine dual-emit for live resharding;
                     opt-in BRC-142 coalescing (coalesce.go): within-batch
                     coalBuffer + FlushCoalesced origin packing, ProcessBundle
                     verbatim spine relay; BRC-148/149 BEEF submission-record
                     expansion + FrameVer 0x09 process path (beef.go);
                     -retry-tee egress mirror to a co-located retry cache
                     (retrytee.go)
  worker/            per-CPU SO_REUSEPORT UDP ingress loop using recvmmsg
                     (ReadBatch) with frame-version dispatch for BRC-131/
                     BRC-132/BRC-134 (worker.go); TCP ingress listener with
                     BRC-127 routing (tcp.go); single-class BRC-143/144 push
                     lanes (objingress.go)
  manifest/          opt-in BRC-139 consumer: beacon-receive listener +
                     applier (restart-on-adopt or BridgingEngine dual-emit)
  metrics/           OTel + Prometheus instrumentation
```

Protocol primitives are provided by
[`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common):

```
shard-common/
  frame/             BRC-12/BRC-124/BRC-128/BRC-130/BRC-131/BRC-132/BRC-134/
                     BRC-149 wire formats + BRC-127/BRC-139 control datagrams:
                     Decode, Encode, constants
  bundle/            BRC-142 coalescing bundle frame (FrameVer 0x08): Bundle
                     Encode/Decode, Member, MemberOverhead, Coalescer/Decoalesce
  objfmt/            bare/push object codecs (ClassTx/ClassSubtree/ClassBlock/
                     ClassBEEF): stream reader, MulticastBytes reframe, TxID,
                     BEEF submission-record codec, TopicID/ContentID
  shard/             txid → group index → IPv6 multicast address derivation;
                     control group constants, GroupAddr, BRC-148 PlaneEngine
  seqhash/           XXH64 per-flow HashKey computation (senderIPv6 ∥ groupIdx ∥ subtreeID)
  pow/               stateless block-header proof-of-work gate (-require-block-pow)
  cache/, txidset/   modular tier-2 backend + two-tier TxID claim store (ingress dedup)
  netjoin/           IPv6 multicast join helpers (beacon-receive socket)
  manifest/          BRC-139 Registry + Evaluator (auto-shard-config consumer)
  logging/, hostinfo/, tracing/
                     unified slog init, host.inventory, opt-in OTLP tracer
```
