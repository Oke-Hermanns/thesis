package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// Client-facing protocol: a lean, length-prefixed binary framing over a
// persistent TCP connection. Deliberately not UDP (client writes must not be
// silently dropped) and not HTTP (avoids per-request parsing/header overhead
// that would skew latency benchmarks). TCP already gives reliable, in-order
// delivery, so this protocol only needs to add message framing on top.

const maxClientMessageSize = 1 << 20 // 1 MiB cap on a request/response body; unrelated to the raft/UDP 9000-byte limit

type clientRequest struct {
	Op    operation
	Key   string
	Value []byte
}

type clientResponse struct {
	Success bool
	// LeaderHint is set when Success is false because this node isn't the
	// leader, so the caller knows who to retry against.
	LeaderHint string
	Error      string
	// Value carries the result of a successful get. Empty for put/delete.
	Value []byte
}

// framed prepends a 4-byte length prefix. Needed because TCP is a byte
// stream with no built-in message boundaries (unlike the UDP raft transport,
// where one read already equals one datagram equals one message).
func framed(body []byte) []byte {
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf
}

// readFrame reads one length-prefixed message body from r.
func readFrame(r io.Reader) ([]byte, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])
	if length > maxClientMessageSize {
		return nil, fmt.Errorf("client message too large: %d bytes", length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

func encodeClientRequest(req *clientRequest) []byte {
	body := make([]byte, 1+2+len(req.Key)+4+len(req.Value))
	offset := 0
	body[offset] = byte(req.Op)
	offset += 1
	binary.BigEndian.PutUint16(body[offset:], uint16(len(req.Key)))
	offset += 2
	copy(body[offset:], req.Key)
	offset += len(req.Key)
	binary.BigEndian.PutUint32(body[offset:], uint32(len(req.Value)))
	offset += 4
	copy(body[offset:], req.Value)

	return framed(body)
}

func decodeClientRequest(body []byte) (*clientRequest, error) {
	if len(body) < 3 {
		return nil, io.ErrUnexpectedEOF
	}
	op := operation(body[0])
	keyLen := int(binary.BigEndian.Uint16(body[1:3]))
	offset := 3
	if len(body) < offset+keyLen+4 {
		return nil, io.ErrUnexpectedEOF
	}
	key := string(body[offset : offset+keyLen])
	offset += keyLen
	valueLen := int(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if len(body) < offset+valueLen {
		return nil, io.ErrUnexpectedEOF
	}
	value := body[offset : offset+valueLen]

	return &clientRequest{Op: op, Key: key, Value: value}, nil
}

func encodeClientResponse(resp *clientResponse) []byte {
	success := byte(0)
	if resp.Success {
		success = 1
	}
	body := make([]byte, 1+2+len(resp.LeaderHint)+2+len(resp.Error)+4+len(resp.Value))
	offset := 0
	body[offset] = success
	offset += 1
	binary.BigEndian.PutUint16(body[offset:], uint16(len(resp.LeaderHint)))
	offset += 2
	copy(body[offset:], resp.LeaderHint)
	offset += len(resp.LeaderHint)
	binary.BigEndian.PutUint16(body[offset:], uint16(len(resp.Error)))
	offset += 2
	copy(body[offset:], resp.Error)
	offset += len(resp.Error)
	binary.BigEndian.PutUint32(body[offset:], uint32(len(resp.Value)))
	offset += 4
	copy(body[offset:], resp.Value)

	return framed(body)
}

func decodeClientResponse(body []byte) (*clientResponse, error) {
	if len(body) < 3 {
		return nil, io.ErrUnexpectedEOF
	}
	success := body[0] == 1
	hintLen := int(binary.BigEndian.Uint16(body[1:3]))
	offset := 3
	if len(body) < offset+hintLen+2 {
		return nil, io.ErrUnexpectedEOF
	}
	leaderHint := string(body[offset : offset+hintLen])
	offset += hintLen
	errLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if len(body) < offset+errLen {
		return nil, io.ErrUnexpectedEOF
	}
	errStr := string(body[offset : offset+errLen])
	offset += errLen
	if len(body) < offset+4 {
		return nil, io.ErrUnexpectedEOF
	}
	valueLen := int(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if len(body) < offset+valueLen {
		return nil, io.ErrUnexpectedEOF
	}
	value := body[offset : offset+valueLen]

	return &clientResponse{Success: success, LeaderHint: leaderHint, Error: errStr, Value: value}, nil
}

// ClientServer is the TCP listener a raft node runs to accept requests from
// a loadgen. It runs on only on the leader.
type ClientServer struct {
	listener net.Listener
	node     *RaftNode
}

func setupClientServer(address string, node *RaftNode) (*ClientServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	cs := &ClientServer{listener: listener, node: node}
	go cs.acceptLoop()
	return cs, nil
}

func (cs *ClientServer) Close() error {
	return cs.listener.Close()
}

func (cs *ClientServer) acceptLoop() {
	for {
		conn, err := cs.listener.Accept()
		if err != nil {
			return
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			// This is a small synchronous request/response protocol, so
			// Nagle's algorithm would only add latency waiting to coalesce
			// writes, never a throughput win.
			tcpConn.SetNoDelay(true)
		}
		go cs.handleConn(conn)
	}
}

func (cs *ClientServer) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		body, err := readFrame(conn)
		if err != nil {
			return
		}
		req, err := decodeClientRequest(body)
		if err != nil {
			return
		}

		resp := cs.node.handleClientRequest(req)

		if _, err := conn.Write(encodeClientResponse(resp)); err != nil {
			return
		}
	}
}
