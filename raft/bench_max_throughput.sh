#!/usr/bin/env bash
# Maximizes throughput against a raft leader's client-facing TCP port, using
# the binary protocol from client.go (same helpers as send_puts.sh).
#
# Usage: ./bench_max_throughput.sh <leader-client-addr:port> <duration-seconds> [workers] [value-size] [timeout-seconds] [op]
#   leader-client-addr:port  e.g. 128.110.219.132:9001
#   duration-seconds         how long to run
#   workers                  concurrent connections (default 8)
#   value-size               bytes per value (default 8, value is "x" repeated)
#   timeout-seconds          per-request response timeout (default 2)
#   op                       put or get (default put); get reads keys this
#                            same run put first, so keep workers modest or
#                            values small enough that puts land before gets
#                            start outrunning them

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <leader-client-addr:port> <duration-seconds> [workers] [value-size] [timeout-seconds] [op]" >&2
    exit 1
fi

ADDR="$1"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"
DURATION="$2"
WORKERS="${3:-8}"
VALUE_SIZE="${4:-8}"
TIMEOUT="${5:-2}"
OP="${6:-put}"

case "$OP" in
    put|get) ;;
    *)
        echo "op must be 'put' or 'get', got: $OP" >&2
        exit 1
        ;;
esac

python3 - "$HOST" "$PORT" "$DURATION" "$WORKERS" "$VALUE_SIZE" "$TIMEOUT" "$OP" <<'PYEOF'
import socket
import statistics
import struct
import sys
import threading
import time

host, port, duration, workers, value_size, timeout, op_name = (
    sys.argv[1],
    int(sys.argv[2]),
    float(sys.argv[3]),
    int(sys.argv[4]),
    int(sys.argv[5]),
    float(sys.argv[6]),
    sys.argv[7],
)

# Must match `const ( put operation = iota + 1; get; del )` in raft.go
OP = {"put": 1, "get": 2}[op_name]
VALUE = ("x" * value_size).encode()


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed by leader")
        buf += chunk
    return buf


def encode_request(key: bytes, value: bytes) -> bytes:
    body = struct.pack(">B H", OP, len(key)) + key + struct.pack(">I", len(value)) + value
    return struct.pack(">I", len(body)) + body


def decode_response(body: bytes):
    success = body[0] == 1
    offset = 1
    (hint_len,) = struct.unpack(">H", body[offset : offset + 2])
    offset += 2
    offset += hint_len
    (err_len,) = struct.unpack(">H", body[offset : offset + 2])
    offset += 2
    offset += err_len
    return success


def connect():
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(timeout)
    return s


results_lock = threading.Lock()
results = []  # list of (ok_count, fail_count, latencies) per worker
stop_flag = threading.Event()


def worker(worker_id):
    try:
        sock = connect()
    except OSError:
        with results_lock:
            results.append((0, 0, []))
        return

    ok = 0
    fail = 0
    latencies = []
    i = 0
    value = VALUE

    while not stop_flag.is_set():
        i += 1
        # Per-worker key prefix so concurrent puts never collide.
        key = f"w{worker_id}-key{i}".encode()

        start = time.monotonic()
        try:
            sock.sendall(encode_request(key, value))
            (resp_len,) = struct.unpack(">I", read_exact(sock, 4))
            resp_body = read_exact(sock, resp_len)
            elapsed = time.monotonic() - start
            if decode_response(resp_body):
                ok += 1
                latencies.append(elapsed)
            else:
                fail += 1
        except (OSError, EOFError):
            fail += 1
            try:
                sock.close()
            except OSError:
                pass
            try:
                sock = connect()
            except OSError:
                break

    with results_lock:
        results.append((ok, fail, latencies))


threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(workers)]
bench_start = time.monotonic()
for t in threads:
    t.start()

time.sleep(duration)

stop_flag.set()
for t in threads:
    t.join(timeout=timeout + 1)

bench_elapsed = time.monotonic() - bench_start

total_ok = sum(r[0] for r in results)
total_fail = sum(r[1] for r in results)
all_latencies = [l for r in results for l in r[2]]

print(f"\n{total_ok}/{total_ok + total_fail} succeeded, {total_fail} failed, over {bench_elapsed:.3f}s with {workers} workers")
print(f"throughput: {total_ok / bench_elapsed:.1f} committed {op_name}s/sec")

print("\nper-worker:")
for w, (ok, fail, lat) in enumerate(results):
    print(f"  worker {w}: {ok} ok, {fail} failed, {ok/bench_elapsed:.1f}/sec")

if all_latencies:
    lat_us = sorted(l * 1e6 for l in all_latencies)
    n = len(lat_us)
    avg = statistics.mean(lat_us)
    stdev = statistics.stdev(lat_us) if n > 1 else 0.0

    def percentile(p):
        idx = int(n * p / 100)
        return lat_us[min(idx, n - 1)]

    print(f"\n====> avg-latency={avg:.3f} (std-dev={stdev:.3f})")
    print(f"Total {n} observations")
    print(f"---> <MAX> observation = {lat_us[-1]:>8.3f}")
    for p in (99.999, 99.990, 99.900, 99.000, 90.000, 75.000, 50.000, 25.000):
        print(f"---> percentile {p:>6.3f} = {percentile(p):>8.3f}")
    print(f"---> <MIN> observation = {lat_us[0]:>8.3f}")
PYEOF
