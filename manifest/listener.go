// Package manifest implements the proxy-side BRC-137 manifest consumer.
// Unlike the listener (which already had a beacon socket for BRC-126
// ADVERTs), the proxy has no prior reason to join the beacon group; this
// package owns a dedicated socket that reads only manifests (MsgType
// 0x40) and feeds them into a shared shard-common/manifest registry.
//
// The socket is opened posture-aware via netjoin: ASM
// (IPV6_JOIN_GROUP) when sources is empty, SSM (one
// MCAST_JOIN_SOURCE_GROUP per source) otherwise. ADVERT datagrams that
// share the beacon group are counted-and-dropped: the proxy has no
// downstream consumer for them.
package manifest

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	commanifest "github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/netjoin"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// Listener owns the proxy's manifest-receive socket.
type Listener struct {
	Group    *net.UDPAddr   // FFxx::B:FFFD on the chosen scope
	Iface    *net.Interface // multicast-egress iface (must support recv)
	Sources  []netip.Addr   // empty ⇒ ASM; non-empty ⇒ SSM bootstrap.beacon ∪ bootstrap.manifest
	Registry *commanifest.Registry
	Rec      *metrics.Recorder
	Log      *slog.Logger
	Debug    bool
}

// Start opens the socket, joins the group, and reads until ctx is
// cancelled. Returns the first non-temporary error.
func (l *Listener) Start(ctx context.Context) error {
	log := l.Log
	if log == nil {
		log = slog.Default().With("component", "manifest-listener")
	}
	conn, err := l.openGroupConn()
	if err != nil {
		return fmt.Errorf("manifest-listener: open: %w", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadBuffer(1 << 16)

	log.Info("manifest listener started",
		"group", l.Group.IP.String(),
		"port", l.Group.Port,
		"posture", posture(l.Sources),
		"sources", len(l.Sources))

	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			select {
			case <-ctx.Done():
				return nil
			default:
				log.Warn("read error", "err", err)
				continue
			}
		}
		if n < 7 {
			continue
		}
		// Demux: only MsgType 0x40 is interesting for the proxy.
		if buf[6] != frame.MsgTypeShardManifest {
			if l.Debug {
				log.Debug("ignoring non-manifest MsgType", "msg_type", buf[6], "src", src.IP.String())
			}
			continue
		}
		m, err := frame.DecodeShardManifest(buf[:n])
		if err != nil {
			if l.Debug {
				log.Debug("decode error", "src", src.IP.String(), "err", err)
			}
			continue
		}
		srcAddr, ok := netip.AddrFromSlice(src.IP.To16())
		if !ok {
			continue
		}
		l.Registry.Upsert(srcAddr, m)
		l.Rec.ManifestReceived()
		if l.Debug {
			log.Debug("manifest upserted",
				"src", src.IP.String(),
				"instance", m.InstanceID,
				"shard_bits", m.ShardBits,
				"flags", m.Flags)
		}
	}
}

// openGroupConn opens the receive socket. ASM uses
// net.ListenMulticastUDP; SSM uses ListenPacket + netjoin.Join so the
// (S,G) filter list goes through one shared helper.
func (l *Listener) openGroupConn() (*net.UDPConn, error) {
	if len(l.Sources) == 0 {
		return net.ListenMulticastUDP("udp6", l.Iface, l.Group)
	}
	pc, err := net.ListenPacket("udp6", fmt.Sprintf("[::]:%d", l.Group.Port))
	if err != nil {
		return nil, fmt.Errorf("ssm listen %d: %w", l.Group.Port, err)
	}
	uc, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("ssm listen: unexpected conn type %T", pc)
	}
	ga, ok := netip.AddrFromSlice(l.Group.IP.To16())
	if !ok {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: bad group address %s", l.Group.IP)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: SyscallConn: %w", err)
	}
	var joinErr error
	if cerr := raw.Control(func(fd uintptr) {
		joinErr = netjoin.Join(int(fd), l.Iface.Index, ga, l.Sources)
	}); cerr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm listen: Control: %w", cerr)
	}
	if joinErr != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("ssm join (%d sources): %w", len(l.Sources), joinErr)
	}
	return uc, nil
}

func posture(sources []netip.Addr) string {
	if len(sources) == 0 {
		return "asm"
	}
	return "ssm"
}
