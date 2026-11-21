// Package latstamp defines the latency-stamp layout shared by perf-test and
// latency-sink: magic (8B) + tx CLOCK_REALTIME nanos (8B BE) + seq (4B BE),
// embedded at the head of an otherwise opaque BRC-124 payload. The frame
// format and the datapath under test are unaware of it.
//
// Any other sender, and a host-side pcap analyzer, may use the same layout.
// CLOCK_REALTIME makes stamps cross-host comparable only when clocks are
// disciplined (chrony/NTP); single-clock pcap analysis ignores the field.
package latstamp

import (
	"bytes"
	"encoding/binary"
)

// Magic is the 8-byte marker that identifies a stamped payload.
const Magic = "1BSVLAT1"

// Size is the total stamp length at the head of the payload.
const Size = 20

// Put writes a stamp at the head of p. Returns false if p is too short.
func Put(p []byte, txNanos int64, seq uint32) bool {
	if len(p) < Size {
		return false
	}
	copy(p, Magic)
	binary.BigEndian.PutUint64(p[8:], uint64(txNanos))
	binary.BigEndian.PutUint32(p[16:], seq)
	return true
}

// Get parses a stamp anchored at the head of p.
func Get(p []byte) (txNanos int64, seq uint32, ok bool) {
	if len(p) < Size || string(p[:8]) != Magic {
		return 0, 0, false
	}
	return int64(binary.BigEndian.Uint64(p[8:])), binary.BigEndian.Uint32(p[16:]), true
}

// Scan searches b for a stamp at any offset (e.g. inside an encapsulated or
// undecoded datagram) and parses the first occurrence.
func Scan(b []byte) (txNanos int64, seq uint32, ok bool) {
	i := bytes.Index(b, []byte(Magic))
	if i < 0 || i+Size > len(b) {
		return 0, 0, false
	}
	return Get(b[i:])
}
