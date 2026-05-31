package worker

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
)

func TestSetRecvBufBytes(t *testing.T) {
	fwd := makeTestForwarder()
	w := New(0, fwd, nil, nil)
	def := w.recvBufBytes
	w.SetRecvBufBytes(0) // < 1 leaves default in place
	if w.recvBufBytes != def {
		t.Errorf("SetRecvBufBytes(0) changed default: %d", w.recvBufBytes)
	}
	w.SetRecvBufBytes(1 << 20)
	if w.recvBufBytes != 1<<20 {
		t.Errorf("recvBufBytes = %d, want %d", w.recvBufBytes, 1<<20)
	}
}

// buildTCPHeaderFrame builds a 92-byte V4/V5 header frame with the given
// version byte, msgType (byte 7), id byte at offset 8, and payload.
func buildTCPHeaderFrame(ver, msgType, idByte byte, payload []byte) []byte {
	buf := make([]byte, frame.HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = ver
	buf[7] = msgType
	buf[8] = idByte
	binary.BigEndian.PutUint32(buf[88:92], uint32(len(payload)))
	copy(buf[frame.HeaderSize:], payload)
	return buf
}

func TestHandleConnV4BlockFrame(t *testing.T) {
	dialHandleConn(t, func(c net.Conn) {
		_, _ = c.Write(buildTCPHeaderFrame(frame.FrameVerV4, frame.BlockMsgAnnounce, 0x01, []byte("blk")))
	})
}

func TestHandleConnV5SubtreeDataFrame(t *testing.T) {
	dialHandleConn(t, func(c net.Conn) {
		_, _ = c.Write(buildTCPHeaderFrame(frame.FrameVerV5, frame.SubtreeMsgHashesOnly, 0x02, []byte("nodes")))
	})
}

func TestHandleConnSubtreeAnnounceControl(t *testing.T) {
	dialHandleConn(t, func(c net.Conn) {
		// A 64-byte SubtreeAnnounce datagram: magic + MsgTypeSubtreeAnnounce
		// at offset 6, padded out to SubtreeAnnounceSize.
		buf := make([]byte, frame.SubtreeAnnounceSize)
		binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
		binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
		buf[6] = frame.MsgTypeSubtreeAnnounce
		_, _ = c.Write(buf)
	})
}

func TestHandleConnUnsupportedVersion(t *testing.T) {
	dialHandleConn(t, func(c net.Conn) {
		buf := make([]byte, frame.HeaderSizeLegacy)
		binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
		binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
		buf[6] = 0x7F // not a recognised FrameVer → connection closed
		_, _ = c.Write(buf)
	})
}
