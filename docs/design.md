# Design Notes

## Open questions

- Should a different hash algorithm be applied to the TXID prior to determining the shard group?
- What multicast group address should be used for control messages?
- What frame format should be used for control messages? Should the proxy differentiate?
- NACK retransmission via multicast retry endpoints: wire protocol finalised (56-byte NACK datagram per BRC-124); retry endpoint implementation TBD.
- FEC: deferred — full frame atomicity makes partial repair unproductive; full re-multicast preferred.

## Roadmap

- [x] Test coverage
- [x] Metrics collection and reporting (Prometheus + OTLP)
- [x] Health check endpoints (`/healthz`, `/readyz`)
- [x] Comprehensive structured logging
- [x] Multiple egress interface fan-out
- [x] Docker image and CI/CD pipeline
- [x] Subtree sharding cross-linking fields in BRC-124 frame header
- [x] TCP ingress for reliable ingress delivery (`-tcp-listen-port`)
- [x] Configurable pre-drain period for load-balancer-safe rolling restarts (`-drain-timeout`)
- [ ] Sequence number generation (either external, or internal, or both)
- [x] BRC-124 frame format: 92-byte header, CRC32c SenderID, 4-byte sequence fields
- [ ] NACK / gap-detection via multicast retry endpoints (see bitcoin-shard-listener)
- [ ] Retry endpoint service (cache + re-multicast on NACK)
- [ ] FEC (forward error correction) option for lossy links
- [ ] Shard manifest protocol (publish current shard map to subscribers)
- [ ] Add support for base control group frames (subtree, headers, manifests, etc.)
