#!/usr/bin/env bash
# Gets or deletes a single key from a raft leader's client-facing TCP port,
# using the binary protocol defined in client.go
# (encodeClientRequest/decodeClientResponse). See send_puts.sh for why this
# is a Python helper instead of plain bash/HTTP.
#
# Usage: ./send_get_delete.sh <leader-client-addr:port> <get|delete> <key>
#   leader-client-addr:port  e.g. 128.110.219.132:9001
#   key                      a bare number (e.g. 17 -> "key17", matching the
#                            keys send_puts.sh writes) or an arbitrary string

set -euo pipefail

if [ $# -ne 3 ]; then
    echo "usage: $0 <leader-client-addr:port> <get|delete> <key>" >&2
    exit 1
fi

ADDR="$1"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"
OP="$2"
KEY="$3"

case "$OP" in
    get|delete) ;;
    *)
        echo "operation must be 'get' or 'delete', got: $OP" >&2
        exit 1
        ;;
esac

if [[ "$KEY" =~ ^[0-9]+$ ]]; then
    KEY="key${KEY}"
fi

python3 - "$HOST" "$PORT" "$OP" "$KEY" <<'PYEOF'
import socket
import struct
import sys

host, port, op_name, key = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4].encode()

# Must match `const ( put operation = iota + 1; get; del )` in raft.go
OP = {"get": 2, "delete": 3}[op_name]


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed by leader")
        buf += chunk
    return buf


def encode_request(key: bytes) -> bytes:
    # get/delete requests carry no value.
    body = struct.pack(">B H", OP, len(key)) + key + struct.pack(">I", 0)
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


try:
    sock = socket.create_connection((host, port), timeout=5)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.sendall(encode_request(key))
    (resp_len,) = struct.unpack(">I", read_exact(sock, 4))
    resp_body = read_exact(sock, resp_len)
    sock.close()
except (OSError, EOFError) as e:
    print(f"error: no response from {host}:{port} ({e})", file=sys.stderr)
    sys.exit(1)

success, leader_hint, error, value = decode_response(resp_body)

if success:
    if op_name == "get":
        print(value.decode(errors="replace"))
    else:
        print(f"deleted {key.decode()}, previous value: {value.decode(errors='replace')!r}")
else:
    if error == "key not found":
        print(f"{key.decode()}: not found", file=sys.stderr)
    else:
        print(f"error: {error} (leader hint: {leader_hint})", file=sys.stderr)
    sys.exit(1)
PYEOF
