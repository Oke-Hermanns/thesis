package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// Client-facing server speaking the exact same length-prefixed binary protocol
// as the own implementation (raft/client.go), so the same loadgen
// (send_puts.sh) can benchmark both systems without changes.

const maxClientMessageSize = 1 << 20

const applyTimeout = 10 * time.Second

type clientRequest struct {
	Op    operation
	Key   string
	Value []byte
}

type clientResponse struct {
	Success    bool
	LeaderHint string
	Error      string
	Value      []byte
}

func framed(body []byte) []byte {
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf
}

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

type ClientServer struct {
	listener     net.Listener
	raftNode     *raft.Raft
	stateMachine *kvFsm
}

func setupClientServer(address string, raftNode *raft.Raft, stateMachine *kvFsm) (*ClientServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	cs := &ClientServer{listener: listener, raftNode: raftNode, stateMachine: stateMachine}
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

		resp := cs.handleClientRequest(req)

		if _, err := conn.Write(encodeClientResponse(resp)); err != nil {
			return
		}
	}
}

func (cs *ClientServer) handleClientRequest(req *clientRequest) *clientResponse {
	if cs.raftNode.State() != raft.Leader {
		return &clientResponse{Success: false, LeaderHint: string(cs.raftNode.Leader()), Error: "not leader"}
	}

	switch req.Op {
	case get:
		value, found := cs.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		return &clientResponse{Success: true, Value: value.([]byte)}
	case put:
		future := cs.raftNode.Apply(encodeCommand(put, req.Key, req.Value), applyTimeout)
		if err := future.Error(); err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		if applyErr, ok := future.Response().(error); ok && applyErr != nil {
			return &clientResponse{Success: false, Error: applyErr.Error()}
		}
		return &clientResponse{Success: true}
	case del:
		value, found := cs.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		future := cs.raftNode.Apply(encodeCommand(del, req.Key, nil), applyTimeout)
		if err := future.Error(); err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		if applyErr, ok := future.Response().(error); ok && applyErr != nil {
			return &clientResponse{Success: false, Error: applyErr.Error()}
		}
		return &clientResponse{Success: true, Value: value.([]byte)}
	default:
		return &clientResponse{Success: false, Error: "currently unsupported operation"}
	}
}
