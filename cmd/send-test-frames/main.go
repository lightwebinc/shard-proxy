// Command send-test-frames crafts and sends well-formed BSV-over-UDP frames
// to shard-proxy for local integration testing.
//
// Usage:
//
//	send-test-frames [-addr host:port] [-count N] [-interval ms] [-shard-bits N] [-spread]
//	                 [-frag-mtu N] [-payload-size N]
//
// Each frame's txid prefix increments by 1, fanning traffic across shard groups.
// With -spread, one frame is sent per group per cycle using maximally-spaced txids.
// -count controls the number of spread cycles (0 = infinite). The predicted
// destination multicast group is printed for each frame so output can be compared
// against recv-test-frames.
//
// When -frag-mtu > 0 each frame's payload is split into BRC-130 fragment
// datagrams using the given path MTU. Use -payload-size to control the
// unencoded payload size (default 28 bytes).
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

const ipv6UDPOverhead = 40 + 8 + 104 // IPv6 + UDP + BRC-130 header

func main() {
	addr := flag.String("addr", "[::1]:9000", "proxy listen address (host:port)")
	count := flag.Int("count", 16, "number of frames to send (0 = infinite)")
	intervalMs := flag.Int("interval", 200, "milliseconds between frames")
	shardBits := flag.Uint("shard-bits", 2, "shard-bits the proxy is configured with (for predicted group display)")
	spread := flag.Bool("spread", false, "send one frame per group per cycle with maximally-spaced txids; -count sets cycles (0 = infinite)")
	fragMTU := flag.Int("frag-mtu", 0, "if >0, split each frame payload into BRC-130 fragments using this path MTU")
	payloadSize := flag.Int("payload-size", 28, "payload size in bytes")
	blockAnnounce := flag.Bool("block-announce", false, "send BRC-131 BlockAnnounce frames instead of BRC-124 transaction frames")
	flag.Parse()

	conn, err := net.Dial("udp6", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("close conn: %v", err)
		}
	}()

	e := shard.New(0xFF05, shard.DefaultGroupID, *shardBits)
	payload := make([]byte, *payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	interval := time.Duration(*intervalMs) * time.Millisecond

	// If -frag-mtu, compute fragment data size and use fragment send path.
	fragDataSize := 0
	if *fragMTU > ipv6UDPOverhead {
		fragDataSize = *fragMTU - ipv6UDPOverhead
	}

	// BRC-131 BlockAnnounce mode: send block control frames instead of tx frames.
	if *blockAnnounce {
		fmt.Printf("sending BRC-131 BlockAnnounce to %s  count=%d\n\n", *addr, *count)
		for i := 0; *count == 0 || i < *count; i++ {
			// Build a minimal BlockAnnounce payload: 80B header + 32B coinbase + 4B subtree count.
			var blockHeader [80]byte
			binary.BigEndian.PutUint32(blockHeader[0:4], 0x20000000)  // version
			binary.BigEndian.PutUint32(blockHeader[76:80], uint32(i)) // nonce = sequence
			var coinbaseTxID [32]byte
			coinbaseTxID[0] = byte(i)
			announce := &frame.BlockAnnouncePayload{
				Header:       blockHeader,
				CoinbaseTxID: coinbaseTxID,
			}
			announceBuf := frame.EncodeBlockAnnounce(announce)

			// ContentID = SHA256d of block header (simulated block hash).
			contentID := sha256dOf(blockHeader[:])

			bf := &frame.BlockFrame{
				MsgType:   frame.BlockMsgAnnounce,
				ContentID: contentID,
				HashKey:   0xDEADBEEF01020304,
				SeqNum:    uint64(i + 1),
				Payload:   announceBuf,
			}
			buf := make([]byte, frame.HeaderSize+len(announceBuf))
			n, err := frame.EncodeBlock(bf, buf)
			if err != nil {
				log.Fatalf("EncodeBlock %d: %v", i, err)
			}
			if _, err := conn.Write(buf[:n]); err != nil {
				log.Fatalf("send block frame %d: %v", i, err)
			}
			fmt.Printf("block-announce %d  contentID=%08X  payload=%d bytes\n",
				i, binary.BigEndian.Uint32(contentID[0:4]), len(announceBuf))
			if interval > 0 && (*count == 0 || i < *count-1) {
				time.Sleep(interval)
			}
		}
		return
	}

	fmt.Printf("sending to %s  shard_bits=%d  spread=%v  frag_mtu=%d\n\n",
		*addr, *shardBits, *spread, *fragMTU)
	fmt.Printf("%-6s  %-10s  %-6s  %s\n", "frame", "txid[0:4]", "group", "expected_dst")

	if *spread {
		numGroups := int(e.NumGroups())
		step := uint32(1) << (32 - *shardBits)
		for cycle := 0; *count == 0 || cycle < *count; cycle++ {
			for g := 0; g < numGroups; g++ {
				var txID [32]byte
				binary.BigEndian.PutUint32(txID[0:4], uint32(g)*step)
				// SHA256d payload into txID so listener hash-verify passes.
				txID = sha256dOf(payload)
				// Stamp group routing bits back (preserve group idx in top bits).
				binary.BigEndian.PutUint32(txID[0:4], uint32(g)*step)
				if fragDataSize > 0 && len(payload) > fragDataSize {
					sendFragments(conn, txID, payload, fragDataSize, uint64(cycle*numGroups+g+1))
				} else {
					f := &frame.Frame{TxID: txID, Payload: payload}
					buf := make([]byte, frame.HeaderSize+len(payload))
					sendFrame(conn, e, f, buf, cycle*numGroups+g, interval)
				}
				if interval > 0 {
					time.Sleep(interval)
				}
			}
		}
		return
	}

	for i := 0; *count == 0 || i < *count; i++ {
		txID := sha256dOf(payload)
		binary.BigEndian.PutUint32(txID[0:4], uint32(i))
		if fragDataSize > 0 && len(payload) > fragDataSize {
			sendFragments(conn, txID, payload, fragDataSize, uint64(i+1))
		} else {
			f := &frame.Frame{TxID: txID, Payload: payload}
			buf := make([]byte, frame.HeaderSize+len(payload))
			sendFrame(conn, e, f, buf, i, interval)
		}
		if interval > 0 {
			time.Sleep(interval)
		}
	}
}

// sha256dOf returns SHA256d(data) as a [32]byte.
func sha256dOf(data []byte) [32]byte {
	first := sha256.Sum256(data)
	return sha256.Sum256(first[:])
}

// sendFragments splits payload into BRC-130 fragments and sends each as a
// separate UDP datagram. hashKey and seqBase are used to stamp each fragment.
func sendFragments(conn net.Conn, txID [32]byte, payload []byte, dataSize int, seqBase uint64) {
	origLen := uint32(len(payload))
	k := (len(payload) + dataSize - 1) / dataSize
	var subID [32]byte
	buf := make([]byte, frame.HeaderSizeV3+dataSize)
	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]
		n, err := frame.EncodeFragment(buf, txID, subID, 0xDEADBEEF01020304, seqBase+uint64(i), origLen, uint16(i), uint16(k), 0, fragData)
		if err != nil {
			log.Fatalf("EncodeFragment: %v", err)
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			log.Fatalf("send fragment %d/%d: %v", i, k, err)
		}
		fmt.Printf("frag    %08X    -      fragment %d/%d\n",
			binary.BigEndian.Uint32(txID[0:4]), i+1, k)
	}
}

func sendFrame(conn net.Conn, e *shard.Engine, f *frame.Frame, buf []byte, seq int, interval time.Duration) {
	n, err := frame.Encode(f, buf)
	if err != nil {
		log.Fatalf("encode frame %d: %v", seq, err)
	}
	if _, err := conn.Write(buf[:n]); err != nil {
		log.Fatalf("send frame %d: %v", seq, err)
	}
	groupIdx := e.GroupIndex(&f.TxID)
	dst := e.Addr(groupIdx, 9001)
	fmt.Printf("%-6d  %08X    %-6d  %s\n",
		seq, binary.BigEndian.Uint32(f.TxID[0:4]), groupIdx, dst.IP)
	if interval > 0 {
		time.Sleep(interval)
	}
}
