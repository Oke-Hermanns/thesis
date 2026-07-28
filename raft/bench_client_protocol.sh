#!/usr/bin/env bash
# Measures the cost of the client<->leader protocol itself (TCP round trip +
# framing + encode/decode + one goroutine hop through ClientServer.handleConn
# in client.go), with as little raft machinery mixed in as possible.
#
# Uses GET, not put/delete, and that's the whole point: a get is answered
# straight out of handleClientRequest via a local sync.Map lookup
# (kvFsm.Get), with no replication, no quorum wait, no applyLoop involved
#
# Usage: ./bench_client_protocol.sh <leader-client-addr:port> <duration-seconds> [key] [timeout-seconds]
#   leader-client-addr:port  e.g. 128.110.219.132:9001
#   duration-seconds         how long to run the timed get loop
#   key                      key to seed and repeatedly get (default: "protocolbench")
#   timeout-seconds          per-request response timeout (default 2)

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <leader-client-addr:port> <duration-seconds> [key] [timeout-seconds]" >&2
    exit 1
fi

ADDR="$1"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"
DURATION="$2"
KEY="${3:-protocolbench}"
TIMEOUT="${4:-2}"

python3 - "$HOST" "$PORT" "$DURATION" "$KEY" "$TIMEOUT" <<'PYEOF'
import socket
import statistics
import struct
import sys
import time

host, port, duration, key_str, timeout = (
    sys.argv[1],
    int(sys.argv[2]),
    float(sys.argv[3]),
    sys.argv[4],
    float(sys.argv[5]),
)

# Must match `const ( Put operation = iota + 1; Get; Del )` in raft.go
PUT, GET = 1, 2
key = key_str.encode()
value = b"protocolbenchvalue"


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed by leader")
        buf += chunk
    return buf


def encode_request(op, key: bytes, value: bytes) -> bytes:
    body = struct.pack(">B H", op, len(key)) + key + struct.pack(">I", len(value)) + value
    return struct.pack(">I", len(body)) + body


def decode_response(body: bytes):
    success = body[0] == 1
    offset = 1
    (hint_len,) = struct.unpack(">H", body[offset : offset + 2])
    offset += 2
    leader_hint = body[offset : offset + hint_len].decode()
    offset += hint_len
    (err_len,) = struct.unpack(">H", body[offset : offset + 2])
    offset += 2
    error = body[offset : offset + err_len].decode()
    offset += err_len
    (value_len,) = struct.unpack(">I", body[offset : offset + 4])
    offset += 4
    val = body[offset : offset + value_len]
    return success, leader_hint, error, val


def connect():
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(timeout)
    return s


def roundtrip(sock, op, k, v):
    sock.sendall(encode_request(op, k, v))
    (resp_len,) = struct.unpack(">I", read_exact(sock, 4))
    return decode_response(read_exact(sock, resp_len))


sock = connect()

# Untimed setup: seed the key so the timed loop measures real hits.
success, leader_hint, error, _ = roundtrip(sock, PUT, key, value)
if not success:
    print(f"setup put failed: {error} (leader hint: {leader_hint})", file=sys.stderr)
    sys.exit(1)

ok = 0
fail = 0
lost = 0
latencies = []

bench_start = time.monotonic()

while time.monotonic() - bench_start < duration:
    start = time.monotonic()
    try:
        success, leader_hint, error, _ = roundtrip(sock, GET, key, b"")
        elapsed = time.monotonic() - start
        if success:
            ok += 1
            latencies.append(elapsed)
        else:
            fail += 1
    except (OSError, EOFError):
        fail += 1
        lost += 1
        sock.close()
        try:
            sock = connect()
        except OSError:
            break

bench_elapsed = time.monotonic() - bench_start
sock.close()

attempted = ok + fail
print(f"\n{ok}/{attempted} succeeded, {fail} failed ({lost} lost/timed out)")
print(f"throughput: {ok / bench_elapsed:.1f} gets/sec over {bench_elapsed:.3f}s (single connection, sequential)")

if latencies:
    lat_us = sorted(l * 1e6 for l in latencies)
    n = len(lat_us)
    avg = statistics.mean(lat_us)
    stdev = statistics.stdev(lat_us) if n > 1 else 0.0

    def percentile(p):
        idx = int(n * p / 100)
        return lat_us[min(idx, n - 1)]

    print(f"\n====> avg-latency={avg:.3f}us (std-dev={stdev:.3f})")
    print(f"Total {n} observations (client<->leader protocol round trip only, no replication)")
    print(f"---> <MAX> observation = {lat_us[-1]:>8.3f}")
    for p in (99.999, 99.990, 99.900, 99.000, 90.000, 75.000, 50.000, 25.000):
        print(f"---> percentile {p:>6.3f} = {percentile(p):>8.3f}")
    print(f"---> <MIN> observation = {lat_us[0]:>8.3f}")
PYEOF
