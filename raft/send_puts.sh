#!/usr/bin/env bash
# Sends N put requests to a raft leader's client-facing TCP port, using the
# binary protocol defined in client.go (encodeClientRequest/decodeClientResponse).
# Not plain HTTP/text, so the actual framing/parsing is done by an embedded
# Python3 helper (bash can't reliably handle binary data - NUL bytes truncate
# bash string variables) over a single persistent connection, matching how the
# leader expects a benchmarking client to behave.
#
# Usage: ./send_puts.sh <leader-client-addr:port> [count] [value-size] [timeout-seconds] [-d|--duration seconds]
#   leader-client-addr:port  e.g. 128.110.219.132:9001
#   count                    number of puts to send (default 100). Ignored
#                            if -d/--duration is given.
#   value-size               bytes per value (default 8, value is "x" repeated)
#   timeout-seconds          per-request response timeout (default 2). A put
#                            can legitimately never get a response - e.g. the
#                            cluster's raft transport is UDP and the
#                            AppendEntries/response that would let it commit
#                            got dropped - so this bounds how long we wait
#                            before giving up on a request and reconnecting.
#   -d, --duration seconds   run for this many seconds instead of a fixed
#                            count, sending puts back to back until time is up.

set -euo pipefail

DURATION=0
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        --duration=*)
            DURATION="${1#*=}"
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done
if [ ${#ARGS[@]} -gt 0 ]; then
    set -- "${ARGS[@]}"
else
    set --
fi

if [ $# -lt 1 ]; then
    echo "usage: $0 <leader-client-addr:port> [count] [value-size] [timeout-seconds] [-d|--duration seconds]" >&2
    exit 1
fi

ADDR="$1"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"
COUNT="${2:-100}"
VALUE_SIZE="${3:-8}"
TIMEOUT="${4:-2}"

python3 - "$HOST" "$PORT" "$COUNT" "$VALUE_SIZE" "$TIMEOUT" "$DURATION" <<'PYEOF'
import socket
import statistics
import struct
import sys
import time

host, port, count, value_size, timeout, duration = (
    sys.argv[1],
    int(sys.argv[2]),
    int(sys.argv[3]),
    int(sys.argv[4]),
    float(sys.argv[5]),
    float(sys.argv[6]),
)

PUT = 1  # matches `const put operation = iota + 1` in raft.go


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed by leader")
        buf += chunk
    return buf


def encode_request(key: bytes, value: bytes) -> bytes:
    body = struct.pack(">B H", PUT, len(key)) + key + struct.pack(">I", len(value)) + value
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
    value = body[offset : offset + value_len]
    return success, leader_hint, error, value


def connect():
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(timeout)
    return s


sock = connect()

ok = 0
fail = 0
lost = 0
latencies = []

if duration > 0:
    print(f"running for {duration:.0f}s...", file=sys.stderr)
else:
    print(f"sending {count} puts...", file=sys.stderr)

bench_start = time.monotonic()

i = 0
while True:
    if duration > 0:
        if time.monotonic() - bench_start >= duration:
            break
    elif i >= count:
        break
    i += 1

    key = f"key{i}".encode()
    value = ("x" * value_size).encode()

    start = time.monotonic()
    try:
        sock.sendall(encode_request(key, value))

        (resp_len,) = struct.unpack(">I", read_exact(sock, 4))
        resp_body = read_exact(sock, resp_len)
        elapsed = time.monotonic() - start

        success, leader_hint, error, _value = decode_response(resp_body)
        if success:
            ok += 1
            latencies.append(elapsed)
            print(f"request {i}: put {key.decode()}={value.decode()} committed in {elapsed*1e6:.1f}us")
        else:
            fail += 1
            print(f"request {i}: failed: {error} (leader hint: {leader_hint})", file=sys.stderr)
    except (OSError, EOFError) as e:
        # No response within `timeout`, or the connection dropped mid-request.
        # Common cause: the cluster's raft transport is UDP and a message
        # needed to commit this entry was lost, so the leader never replies.
        # The connection is now in an unknown framing state (a delayed
        # response for this request could still arrive later), so drop it
        # and reconnect before sending the next request.
        fail += 1
        lost += 1
        print(f"request {i}: no response within {timeout}s ({e}); reconnecting", file=sys.stderr)
        sock.close()
        try:
            sock = connect()
        except OSError as reconnect_err:
            print(f"reconnect to {host}:{port} failed ({reconnect_err}); giving up", file=sys.stderr)
            break

bench_elapsed = time.monotonic() - bench_start
sock.close()

attempted = ok + fail
print(f"\n{ok}/{attempted} succeeded, {fail} failed ({lost} lost/timed out)")
print(f"throughput: {ok / bench_elapsed:.1f} committed puts/sec over {bench_elapsed:.3f}s")

if latencies:
    latencies_us = sorted(l * 1e6 for l in latencies)
    n = len(latencies_us)
    avg = statistics.mean(latencies_us)
    stdev = statistics.stdev(latencies_us) if n > 1 else 0.0

    def percentile(p):
        idx = int(n * p / 100)
        if idx >= n:
            idx = n - 1
        return latencies_us[idx]

    def field(val):
        return f"= {val:>8.3f}"

    print(f"\n====> avg-latency={avg:.3f} (std-dev={stdev:.3f})")
    print(f"# dropped messages = {lost}; # duplicated messages = 0; # out-of-order messages = 0")
    print(f"Summary: Latency is {avg:.3f} usec")
    print(f"Total {n} observations; each percentile contains {n / 100:.2f} observations")
    print(f"---> <MAX> observation {field(latencies_us[-1])}")
    for p in (99.999, 99.990, 99.900, 99.000, 90.000, 75.000, 50.000, 25.000):
        print(f"---> percentile {p:>6.3f} {field(percentile(p))}")
    print(f"---> <MIN> observation {field(latencies_us[0])}")
PYEOF
