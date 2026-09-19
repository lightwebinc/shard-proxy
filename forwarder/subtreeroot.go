package forwarder

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"runtime"
	"sync"

	"github.com/lightwebinc/shard-common/frame"
)

// Subtree root verification (-verify-subtree-root).
//
// A BRC-132 subtree data frame names its subtree by merkle root (SubtreeID)
// and carries the node hashes that root commits to. Recomputing the root from
// the nodes proves the list is the one the root names. It validates no
// transaction, and it says nothing about per-node fee or size in a FullNodes
// payload: the root commits to hashes only.
//
// The computation matches Teranode's (go-subtree BuildMerkleTreeStoreFromBytes):
// a level with an odd count pairs its last hash with itself, a single node is
// its own root, and an all-zero hash stands for an absent node (a pair whose
// left side is zero hashes to zero; a zero right side is treated as absent).

var (
	errSubtreeMalformed    = errors.New("subtree payload malformed")
	errSubtreeRootMismatch = errors.New("subtree nodes do not hash to the subtree root")
)

const (
	// subtreeRootParallelMin is the level width, in parent hashes, from which
	// a level is split across goroutines. Below it one goroutine is faster.
	subtreeRootParallelMin = 1 << 14

	// subtreeRootMaxWorkers bounds the goroutines one verification uses, so a
	// maximum-size subtree does not take every core from the transaction path.
	subtreeRootMaxWorkers = 4
)

// verifySubtreePayload checks that the node hashes in a BRC-132 payload hash
// to root. msgType selects the node stride (32 bytes HashesOnly, 48 FullNodes);
// the caller has already rejected any other value. A payload with no nodes, or
// fewer node bytes than its NodeCount claims, is malformed.
func verifySubtreePayload(root [32]byte, msgType byte, payload []byte) error {
	stride := frame.SubtreeNodeHashSize
	if msgType == frame.SubtreeMsgFullNodes {
		stride = frame.SubtreeNodeFullSize
	}
	if len(payload) < frame.SubtreeDataPayloadHeaderSize {
		return errSubtreeMalformed
	}
	count := binary.BigEndian.Uint64(payload[16:24])
	avail := uint64(len(payload)-frame.SubtreeDataPayloadHeaderSize) / uint64(stride)
	if count == 0 || count > avail {
		return errSubtreeMalformed
	}
	nodes := payload[frame.SubtreeDataPayloadHeaderSize:]
	if subtreeRoot(nodes, stride, int(count)) != root {
		return errSubtreeRootMismatch
	}
	return nil
}

// subtreeRoot returns the merkle root of the n node hashes in nodes, each the
// first 32 bytes of a stride-byte record. n must be at least 1.
func subtreeRoot(nodes []byte, stride, n int) [32]byte {
	var root [32]byte
	if n == 1 {
		copy(root[:], nodes[:32])
		return root
	}
	// Two buffers, alternating as source and destination: a level cannot be
	// computed in place once it is split across goroutines.
	width := (n + 1) / 2
	src := make([]byte, width*32)
	hashLevel(src, nodes, stride, n)
	n = width
	dst := make([]byte, ((n+1)/2)*32)
	for n > 1 {
		hashLevel(dst, src, 32, n)
		n = (n + 1) / 2
		src, dst = dst, src
	}
	copy(root[:], src[:32])
	return root
}

// hashLevel writes the (n+1)/2 parents of the n hashes in src into dst.
func hashLevel(dst, src []byte, stride, n int) {
	parents := (n + 1) / 2
	workers := 1
	if parents >= subtreeRootParallelMin {
		workers = min(runtime.GOMAXPROCS(0), subtreeRootMaxWorkers)
	}
	if workers <= 1 {
		hashRange(dst, src, stride, n, 0, parents)
		return
	}
	chunk := (parents + workers - 1) / workers
	var wg sync.WaitGroup
	for lo := 0; lo < parents; lo += chunk {
		hi := min(lo+chunk, parents)
		wg.Add(1)
		go func() {
			defer wg.Done()
			hashRange(dst, src, stride, n, lo, hi)
		}()
	}
	wg.Wait()
}

// hashRange computes parents lo..hi-1 of the n hashes in src.
func hashRange(dst, src []byte, stride, n, lo, hi int) {
	var zero [32]byte
	var pair [64]byte
	for p := lo; p < hi; p++ {
		out := dst[p*32 : p*32+32]
		left := src[2*p*stride : 2*p*stride+32]
		if [32]byte(left) == zero {
			clear(out)
			continue
		}
		copy(pair[:32], left)
		copy(pair[32:], left)
		if r := 2*p + 1; r < n {
			right := src[r*stride : r*stride+32]
			if [32]byte(right) != zero {
				copy(pair[32:], right)
			}
		}
		h := sha256.Sum256(pair[:])
		h = sha256.Sum256(h[:])
		copy(out, h[:])
	}
}
