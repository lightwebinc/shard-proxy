// Command recv-test-frames joins one or more IPv6 multicast groups and prints
// every BSV-over-UDP frame it receives. Use alongside send-test-frames to
// verify that bitcoin-shard-proxy is forwarding to the correct groups.
//
// Usage:
//
//	recv-test-frames -iface lo0 -port 9001 -groups ff02::0,ff02::1
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
)

func main() {
	iface := flag.String("iface", "lo0", "interface to join multicast groups on (lo on Linux)")
	port := flag.Int("port", 9001, "UDP port to listen on")
	groupsFlag := flag.String("groups", "ff02::0,ff02::1", "comma-separated multicast group addresses to join")
	count := flag.Int("count", 0, "exit after receiving this many frames (0 = run forever)")
	timeout := flag.Duration("timeout", 0, "exit with failure after this duration if count not reached (0 = no timeout)")
	flag.Parse()

	ifi, err := net.InterfaceByName(*iface)
	if err != nil {
		log.Fatalf("interface %q: %v", *iface, err)
	}

	// Open a single IPv6 UDP socket with SO_REUSEPORT + SO_REUSEADDR so it
	// can share port 9001 with other processes, then join every requested
	// multicast group on the socket individually via IPV6_JOIN_GROUP.
	// Using one socket for all groups avoids the EADDRINUSE error that occurs
	// when net.ListenMulticastUDP tries to bind a second socket to the same port.
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, unix.IPPROTO_UDP)
	if err != nil {
		log.Fatalf("socket: %v", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Fatalf("SO_REUSEADDR: %v", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		log.Fatalf("SO_REUSEPORT: %v", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrInet6{Port: *port}); err != nil {
		log.Fatalf("bind :%d: %v", *port, err)
	}

	groups := strings.Split(*groupsFlag, ",")
	for _, g := range groups {
		g = strings.TrimSpace(g)
		ip := net.ParseIP(g)
		if ip == nil {
			log.Printf("invalid group address %q, skipping", g)
			continue
		}
		ip16 := ip.To16()
		mreq := &unix.IPv6Mreq{Interface: uint32(ifi.Index)}
		copy(mreq.Multiaddr[:], ip16)
		if err := unix.SetsockoptIPv6Mreq(fd, unix.IPPROTO_IPV6, unix.IPV6_JOIN_GROUP, mreq); err != nil {
			log.Fatalf("IPV6_JOIN_GROUP %s on %s: %v", g, ifi.Name, err)
		}
		log.Printf("joined %-20s on %s", g, ifi.Name)
	}

	var received atomic.Int64
	done := make(chan struct{})

	go recvLoop(fd, *count, &received, done)

	if *timeout > 0 {
		go func() {
			timer := time.NewTimer(*timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				log.Printf("timeout after %s: only received %d/%d frames", *timeout, received.Load(), *count)
				_ = unix.Close(fd)
				os.Exit(1)
			case <-done:
			}
		}()
	}

	<-done
	_ = unix.Close(fd)
}

func recvLoop(fd int, limit int, received *atomic.Int64, done chan struct{}) {
	buf := make([]byte, 65536)
	for {
		n, from, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			// fd was closed (shutdown)
			return
		}
		sa, ok := from.(*unix.SockaddrInet6)
		if !ok {
			continue
		}
		src := fmt.Sprintf("[%s]:%d", net.IP(sa.Addr[:]).String(), sa.Port)
		f, err := frame.Decode(buf[:n])
		if err != nil {
			log.Printf("decode error from %s: %v", src, err)
			continue
		}
		fmt.Printf("recv  src=%-26s  txid[0:4]=%08X  payload_len=%d\n",
			src, binary.BigEndian.Uint32(f.TxID[0:4]), len(f.Payload))
		if limit > 0 {
			if received.Add(1) >= int64(limit) {
				log.Printf("received %d frames, exiting", limit)
				close(done)
				return
			}
		}
	}
}
