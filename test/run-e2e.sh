#!/bin/sh
set -e

SHARD_BITS=${SHARD_BITS:-2}
RECV_COUNT=${RECV_COUNT:-4}
UDP_LISTEN_PORT=${UDP_LISTEN_PORT:-8725}
EGRESS_PORT=${EGRESS_PORT:-9001}
METRICS_PORT=${METRICS_PORT:-9100}

# Detect OS and loopback interface name
if [ "$(uname)" = "Darwin" ]; then
    LOOPBACK="lo0"
    USE_METRICS=0
else
    LOOPBACK="lo"
    USE_METRICS=1
    # Loopback needs the MULTICAST flag and an ff00::/8 route in table local, or
    # every IPv6 multicast send out lo fails ENETUNREACH ("network is unreachable").
    #
    # The kernel does NOT provide that route for loopback: addrconf sends
    # ARPHRD_LOOPBACK down init_loopback() (which adds only ::1/128), while
    # addrconf_add_mroute() — the thing that installs ff00::/8 — is reached only
    # for non-loopback devices. So it must be provisioned once, out of band.
    #
    # This used to attempt the mutation here as `... 2>/dev/null || true`. Run as
    # an ordinary user both commands silently no-op, so the harness reported a
    # confusing mid-run send failure instead of the real precondition. CHECK and
    # REFUSE instead: say exactly what to run, once, and stop.
    if ! ip -6 route show table local type multicast | grep -qw 'dev lo'; then
        cat >&2 <<'MSG'
FATAL: loopback is missing its IPv6 multicast route, so every multicast send
       out lo would fail with "network is unreachable".

       The kernel never creates this route for loopback; it must be installed:

         sudo ip link set lo multicast on
         sudo ip -6 route replace table local multicast ff00::/8 dev lo \
           proto kernel metric 256 pref medium

       ('replace' is idempotent; 'append' returns EEXIST on a second run.)
       On a lab host, prefer a boot-time guard so it survives reboots and any
       `ip link set lo down`: a oneshot unit ordered before the services, or
       an rc.d script, that re-runs the two commands above.
MSG
        exit 1
    fi
    ip link set lo multicast on 2>/dev/null || true
fi

# Compute multicast group list: ff02::0 through ff02::<N-1>
num_groups=$(( 1 << SHARD_BITS ))
groups=""
i=0
while [ $i -lt $num_groups ]; do
    if [ -z "$groups" ]; then
        groups="ff02::$i"
    else
        groups="$groups,ff02::$i"
    fi
    i=$(( i + 1 ))
done

echo "=== E2E test: shard_bits=$SHARD_BITS iface=$LOOPBACK groups=$groups ==="

# Start proxy
shard-proxy \
    -iface "$LOOPBACK" \
    -scope link \
    -shard-bits "$SHARD_BITS" \
    -udp-listen-port "$UDP_LISTEN_PORT" \
    -egress-port "$EGRESS_PORT" \
    -metrics-addr ":$METRICS_PORT" \
    -debug &
PROXY_PID=$!

if [ "$USE_METRICS" = "0" ]; then
    # macOS: use multicast receiver (loopback multicast works on lo0)
    recv-test-frames \
        -iface "$LOOPBACK" \
        -port "$EGRESS_PORT" \
        -groups "$groups" \
        -count "$RECV_COUNT" \
        -timeout "${RECV_TIMEOUT:-30s}" &
    RECV_PID=$!
fi

# Wait for proxy to be ready
sleep 1

# Send exactly one frame per shard group
send-test-frames \
    -addr "[::1]:$UDP_LISTEN_PORT" \
    -shard-bits "$SHARD_BITS" \
    -spread \
    -interval 100

if [ "$USE_METRICS" = "0" ]; then
    # macOS: wait for receiver to report all frames received
    wait $RECV_PID
    RECV_EXIT=$?
    kill $PROXY_PID 2>/dev/null || true
    wait $PROXY_PID 2>/dev/null || true
    if [ $RECV_EXIT -eq 0 ]; then
        echo "=== PASS: received $RECV_COUNT frames ==="
        exit 0
    else
        echo "=== FAIL: receiver exited with code $RECV_EXIT ==="
        exit 1
    fi
else
    # Linux: assert via Prometheus metrics — check bsp_packets_forwarded_total >= RECV_COUNT
    # Give OTel a moment to flush to the Prometheus scrape endpoint
    sleep 1
    FORWARDED=$(curl -sf "http://127.0.0.1:$METRICS_PORT/metrics" \
        | grep '^bsp_packets_forwarded_total{' \
        | awk '{sum += $2} END {print int(sum)}')
    kill $PROXY_PID 2>/dev/null || true
    wait $PROXY_PID 2>/dev/null || true
    echo "bsp_packets_forwarded_total=$FORWARDED (expected>=$RECV_COUNT)"
    if [ "${FORWARDED:-0}" -ge "$RECV_COUNT" ]; then
        echo "=== PASS: proxy forwarded $FORWARDED frames ==="
        exit 0
    else
        echo "=== FAIL: proxy only forwarded ${FORWARDED:-0} frames (expected>=$RECV_COUNT) ==="
        exit 1
    fi
fi
