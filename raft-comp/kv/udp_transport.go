package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
)

// UDPTransport implements the raft.Transport interface over plain UDP
// datagrams, so the unmodified hashicorp/raft library can be benchmarked over
// the same wire protocol family as the own implementation. The library's
// built-in NetworkTransport cannot be reused for this: it requires a
// StreamLayer (reliable, ordered, connection-oriented like TCP), and faking
// that over UDP would mean reimplementing TCP's reliability, which would make
// the comparison meaningless.
//
// Datagram layout: [1B kind (request/response)][1B rpcType][8B seq][msgpack body].
// A request's response is matched by seq. A lost datagram simply times out the
// RPC; hashicorp/raft treats failed RPCs as normal events and retries at the
// replication/election layer, the same way the own implementation relies on
// raft-level retries instead of transport reliability.
//
// Encoding stays msgpack (the same codec NetworkTransport uses) so the
// TCP-vs-UDP comparison within hashicorp/raft isolates the transport protocol,
// not the serialization format.

const (
	udpKindRequest uint8 = iota
	udpKindResponse
)

const (
	udpAppendEntries uint8 = iota
	udpRequestVote
	udpTimeoutNow
	udpRequestPreVote
)

const (
	udpHeaderSize      = 1 + 1 + 8
	maxUDPDatagramSize = 65507
)

var errUDPSnapshotNotSupported = errors.New("InstallSnapshot is not supported over the UDP transport")

type udpPendingRPC struct {
	respCh chan []byte
}

type UDPTransport struct {
	localAddr raft.ServerAddress
	conn      *net.UDPConn
	consumeCh chan raft.RPC
	timeout   time.Duration

	heartbeatFnLock sync.Mutex
	heartbeatFn     func(raft.RPC)

	seq         uint64
	pendingLock sync.Mutex
	pending     map[uint64]*udpPendingRPC

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

func NewUDPTransport(bindAddr string, timeout time.Duration) (*UDPTransport, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return nil, err
	}
	if udpAddr.IP == nil || udpAddr.IP.IsUnspecified() {
		return nil, errors.New("local bind address is not advertisable")
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	t := &UDPTransport{
		localAddr:  raft.ServerAddress(bindAddr),
		conn:       conn,
		consumeCh:  make(chan raft.RPC),
		timeout:    timeout,
		pending:    make(map[uint64]*udpPendingRPC),
		shutdownCh: make(chan struct{}),
	}

	go t.listen()

	return t, nil
}

func (t *UDPTransport) Consumer() <-chan raft.RPC {
	return t.consumeCh
}

func (t *UDPTransport) LocalAddr() raft.ServerAddress {
	return t.localAddr
}

func (t *UDPTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	// The library falls back to serial replication when pipelining is not
	// supported (see replication.go), which matches the own implementation's
	// one-request-at-a-time replication more closely anyway.
	return nil, raft.ErrPipelineReplicationNotSupported
}

func (t *UDPTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return t.genericRPC(target, udpAppendEntries, args, resp)
}

func (t *UDPTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.genericRPC(target, udpRequestVote, args, resp)
}

func (t *UDPTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.genericRPC(target, udpTimeoutNow, args, resp)
}

// RequestPreVote implements raft.WithPreVote.
func (t *UDPTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return t.genericRPC(target, udpRequestPreVote, args, resp)
}

// InstallSnapshot streams arbitrarily large state and inherently needs a
// reliable stream; it is never triggered in the benchmark setup (snapshotting
// is disabled via DiscardSnapshotStore + a huge SnapshotThreshold).
func (t *UDPTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return errUDPSnapshotNotSupported
}

func (t *UDPTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (t *UDPTransport) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

func (t *UDPTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	t.heartbeatFnLock.Lock()
	t.heartbeatFn = cb
	t.heartbeatFnLock.Unlock()
}

// Close implements raft.WithClose.
func (t *UDPTransport) Close() error {
	t.shutdownOnce.Do(func() {
		close(t.shutdownCh)
		t.conn.Close()
	})
	return nil
}

func (t *UDPTransport) genericRPC(target raft.ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	seq := atomic.AddUint64(&t.seq, 1)
	pending := &udpPendingRPC{respCh: make(chan []byte, 1)}

	t.pendingLock.Lock()
	t.pending[seq] = pending
	t.pendingLock.Unlock()
	defer func() {
		t.pendingLock.Lock()
		delete(t.pending, seq)
		t.pendingLock.Unlock()
	}()

	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.WriteByte(udpKindRequest)
	buf.WriteByte(rpcType)
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	buf.Write(seqBytes[:])
	if err := codec.NewEncoder(buf, &codec.MsgpackHandle{}).Encode(args); err != nil {
		return err
	}
	if buf.Len() > maxUDPDatagramSize {
		return fmt.Errorf("encoded rpc (%d bytes) exceeds max udp datagram size %d", buf.Len(), maxUDPDatagramSize)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", string(target))
	if err != nil {
		return err
	}
	if _, err := t.conn.WriteToUDP(buf.Bytes(), udpAddr); err != nil {
		return err
	}

	select {
	case body := <-pending.respCh:
		dec := codec.NewDecoder(bytes.NewReader(body), &codec.MsgpackHandle{})
		var rpcError string
		if err := dec.Decode(&rpcError); err != nil {
			return err
		}
		if err := dec.Decode(resp); err != nil {
			return err
		}
		if rpcError != "" {
			return errors.New(rpcError)
		}
		return nil
	case <-time.After(t.timeout):
		return fmt.Errorf("udp rpc timeout after %s to %s", t.timeout, target)
	case <-t.shutdownCh:
		return raft.ErrTransportShutdown
	}
}

func (t *UDPTransport) listen() {
	buf := make([]byte, maxUDPDatagramSize)
	for {
		n, addr, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-t.shutdownCh:
			default:
				t.Close()
			}
			return
		}
		if n < udpHeaderSize {
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])

		kind := data[0]
		rpcType := data[1]
		seq := binary.BigEndian.Uint64(data[2:10])
		body := data[10:]

		switch kind {
		case udpKindResponse:
			t.pendingLock.Lock()
			pending, ok := t.pending[seq]
			t.pendingLock.Unlock()
			if ok {
				select {
				case pending.respCh <- body:
				default:
				}
			}
		case udpKindRequest:
			t.handleRequest(rpcType, seq, body, addr)
		}
	}
}

func (t *UDPTransport) handleRequest(rpcType uint8, seq uint64, body []byte, from *net.UDPAddr) {
	dec := codec.NewDecoder(bytes.NewReader(body), &codec.MsgpackHandle{})

	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{RespChan: respCh}

	isHeartbeat := false
	switch rpcType {
	case udpAppendEntries:
		var req raft.AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		rpc.Command = &req

		// Same heartbeat detection as the library's NetworkTransport
		// (net_transport.go), so the heartbeat fast-path behaves identically
		// across the TCP and UDP variants.
		leaderAddr := req.Addr
		if len(leaderAddr) == 0 {
			leaderAddr = req.Leader
		}
		if req.Term != 0 && leaderAddr != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}
	case udpRequestVote:
		var req raft.RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		rpc.Command = &req
	case udpTimeoutNow:
		var req raft.TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		rpc.Command = &req
	case udpRequestPreVote:
		var req raft.RequestPreVoteRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		rpc.Command = &req
	default:
		return
	}

	go t.awaitResponse(rpcType, seq, respCh, from)

	if isHeartbeat {
		t.heartbeatFnLock.Lock()
		fn := t.heartbeatFn
		t.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			return
		}
	}

	select {
	case t.consumeCh <- rpc:
	case <-t.shutdownCh:
	}
}

func (t *UDPTransport) awaitResponse(rpcType uint8, seq uint64, respCh <-chan raft.RPCResponse, to *net.UDPAddr) {
	var resp raft.RPCResponse
	select {
	case resp = <-respCh:
	case <-t.shutdownCh:
		return
	}

	buf := bytes.NewBuffer(make([]byte, 0, 256))
	buf.WriteByte(udpKindResponse)
	buf.WriteByte(rpcType)
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	buf.Write(seqBytes[:])

	enc := codec.NewEncoder(buf, &codec.MsgpackHandle{})
	respErr := ""
	if resp.Error != nil {
		respErr = resp.Error.Error()
	}
	if err := enc.Encode(respErr); err != nil {
		return
	}
	if err := enc.Encode(resp.Response); err != nil {
		return
	}
	if buf.Len() > maxUDPDatagramSize {
		return
	}

	t.conn.WriteToUDP(buf.Bytes(), to)
}
