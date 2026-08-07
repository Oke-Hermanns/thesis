package main

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
)

type MessageType uint8

const (
	AppendEntries MessageType = iota + 1
	AppendEntriesResponse
	Join
	JoinResponse
)

type LogEntry struct {
	Term    uint32
	Index   uint64
	Payload []byte
}

type AppendEntriesMsg struct {
	Term uint32
	// This would normally be LeaderID, but because Followers use AppendEntriesMsg for joining the cluster, LeaderID did not make sense anymore
	IDofSender   uint16
	PrevLogIndex uint64 // Follower needs to check this (papers consistency check)
	PrevLogTerm  uint64 // Follower needs to check this (papers consistency check)
	Entries      []LogEntry
	LeaderCommit uint64

	// Fields send in the response message
	Success     bool
	CurrentTerm uint32

	SenderAddress string

	Sequence       uint64
	LinkReceived   uint64
	LinkLost       uint64
	LinkOutOfOrder uint64
}

type Message struct {
	Type          MessageType
	Body          *AppendEntriesMsg
	RemoteAddress string
}

type LinkStats struct {
	Received   uint64
	Lost       uint64
	OutOfOrder uint64
}

type linkSendSeq struct {
	mutex sync.Mutex
	seq   map[string]uint64
}

func newLinkSendSeq() *linkSendSeq {
	return &linkSendSeq{seq: make(map[string]uint64)}
}

func (s *linkSendSeq) next(address string) uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.seq[address]++
	return s.seq[address]
}

type linkRecvTracker struct {
	mutex sync.Mutex
	stats map[string]*linkRecvState
}

type linkRecvState struct {
	lastSeq    uint64
	received   uint64
	lost       uint64
	outOfOrder uint64
}

func newLinkRecvTracker() *linkRecvTracker {
	return &linkRecvTracker{stats: make(map[string]*linkRecvState)}
}

/*
If a packet arrives out of order, it gets deducted from the lost count.
However, there is a risk of counting duplicates, e.g. through Equal-Cost Multi-Path routing.
But keeping track of recently seen sequence numbers or counting out of order arrivals as lost aren't perfect either.
*/
func (tracker *linkRecvTracker) record(address string, seq uint64) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	state, ok := tracker.stats[address]
	if !ok {
		state = &linkRecvState{}
		tracker.stats[address] = state
	}
	if ok {
		if seq > state.lastSeq+1 {
			state.lost += seq - state.lastSeq - 1
		}
		if seq <= state.lastSeq {
			// High number of out of order packets and zero loss indicates duplicate packets
			state.outOfOrder++
			if state.lost > 0 {
				state.lost--
			}
		}
		if seq > state.lastSeq {
			state.lastSeq = seq
		}
	} else {
		state.lastSeq = seq
	}
	state.received++
}

func (tracker *linkRecvTracker) snapshot() map[string]LinkStats {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	out := make(map[string]LinkStats, len(tracker.stats))
	for addr, s := range tracker.stats {
		out[addr] = LinkStats{Received: s.received, Lost: s.lost, OutOfOrder: s.outOfOrder}
	}
	return out
}

func encode(msgType MessageType, msg *AppendEntriesMsg) ([]byte, error) {
	// 1 + 8 + 4 + 2 + 8 + 8 + 8 + 1 + 4 + 8 + 8 + 8 + 2 for message type, Sequence, Term, LeaderID, PrevLogIndex, PrevLogTerm, LeaderCommit, Success, CurrentTerm, LinkReceived, LinkLost, LinkOutOfOrder, number of entries
	// + 2 + len(SenderAddress) for the senders listening address (came later)
	bodySize := 70 + 2 + len(msg.SenderAddress)

	for entry := range msg.Entries {
		bodySize += 2 + 4 + 8 + len(msg.Entries[entry].Payload)
	}

	buffer := make([]byte, 4+bodySize)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(bodySize))
	offset := 4

	buffer[offset] = byte(msgType)
	offset += 1

	binary.BigEndian.PutUint64(buffer[offset:], msg.Sequence)
	offset += 8
	binary.BigEndian.PutUint32(buffer[offset:], msg.Term)
	offset += 4
	binary.BigEndian.PutUint16(buffer[offset:], msg.IDofSender)
	offset += 2
	binary.BigEndian.PutUint64(buffer[offset:], msg.PrevLogIndex)
	offset += 8
	binary.BigEndian.PutUint64(buffer[offset:], msg.PrevLogTerm)
	offset += 8
	binary.BigEndian.PutUint64(buffer[offset:], msg.LeaderCommit)
	offset += 8

	if msg.Success {
		buffer[offset] = 1
	} else {
		buffer[offset] = 0
	}
	offset += 1

	binary.BigEndian.PutUint32(buffer[offset:], msg.CurrentTerm)
	offset += 4
	binary.BigEndian.PutUint64(buffer[offset:], msg.LinkReceived)
	offset += 8
	binary.BigEndian.PutUint64(buffer[offset:], msg.LinkLost)
	offset += 8
	binary.BigEndian.PutUint64(buffer[offset:], msg.LinkOutOfOrder)
	offset += 8
	binary.BigEndian.PutUint16(buffer[offset:], uint16(len(msg.Entries)))
	offset += 2

	binary.BigEndian.PutUint16(buffer[offset:], uint16(len(msg.SenderAddress)))
	offset += 2
	copy(buffer[offset:], msg.SenderAddress)
	offset += len(msg.SenderAddress)

	for entry := range msg.Entries {
		binary.BigEndian.PutUint16(buffer[offset:], uint16(14+len(msg.Entries[entry].Payload)))
		offset += 2
		binary.BigEndian.PutUint32(buffer[offset:], msg.Entries[entry].Term)
		offset += 4
		binary.BigEndian.PutUint64(buffer[offset:], msg.Entries[entry].Index)
		offset += 8
		copy(buffer[offset:], msg.Entries[entry].Payload)
		offset += len(msg.Entries[entry].Payload)
	}

	return buffer, nil
}

func decode(data []byte) (MessageType, *AppendEntriesMsg, error) {
	if len(data) < 4 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	messageSize := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(4+messageSize) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	data = data[4 : 4+messageSize]
	if len(data) < 72 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	message := &AppendEntriesMsg{}

	msgType := MessageType(data[0])
	sequence := binary.BigEndian.Uint64(data[1:9])
	term := binary.BigEndian.Uint32(data[9:13])
	leaderID := binary.BigEndian.Uint16(data[13:15])
	prevLogIndex := binary.BigEndian.Uint64(data[15:23])
	prevLogTerm := binary.BigEndian.Uint64(data[23:31])
	leaderCommit := binary.BigEndian.Uint64(data[31:39])
	success := data[39]
	currentTerm := binary.BigEndian.Uint32(data[40:44])
	linkReceived := binary.BigEndian.Uint64(data[44:52])
	linkLost := binary.BigEndian.Uint64(data[52:60])
	linkOutOfOrder := binary.BigEndian.Uint64(data[60:68])
	numEntries := binary.BigEndian.Uint16(data[68:70])

	addressLen := int(binary.BigEndian.Uint16(data[70:72]))
	if 72+addressLen > len(data) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	senderAddress := string(data[72 : 72+addressLen])

	entryOffset := 72 + addressLen

	for i := 0; i < int(numEntries); i++ {
		if entryOffset+2 > len(data) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		entrySize := binary.BigEndian.Uint16(data[entryOffset : entryOffset+2])
		if entryOffset+int(entrySize) > len(data) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		if entrySize < 14 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		entryTerm := binary.BigEndian.Uint32(data[entryOffset+2 : entryOffset+6])
		entryIndex := binary.BigEndian.Uint64(data[entryOffset+6 : entryOffset+14])

		payloadSize := int(entrySize) - 14
		payload := data[entryOffset+14 : entryOffset+14+payloadSize]
		message.Entries = append(message.Entries, LogEntry{
			Term:    entryTerm,
			Index:   entryIndex,
			Payload: payload,
		})

		entryOffset += int(entrySize)
	}

	message = &AppendEntriesMsg{
		Term:           term,
		IDofSender:     leaderID,
		PrevLogIndex:   prevLogIndex,
		Entries:        message.Entries,
		PrevLogTerm:    prevLogTerm,
		LeaderCommit:   leaderCommit,
		Success:        success == 1,
		CurrentTerm:    currentTerm,
		SenderAddress:  senderAddress,
		Sequence:       sequence,
		LinkReceived:   linkReceived,
		LinkLost:       linkLost,
		LinkOutOfOrder: linkOutOfOrder,
	}
	return MessageType(msgType), message, nil
}

type Transport interface {
	Send(address string, msg *Message) error
	Receive() <-chan *Message
	getAddress() string
	Close() error
	LossStats() map[string]LinkStats
	MaxMessageBytes() int
}

// UDP transport
type UDPTransport struct {
	localAddress string
	recvCh       chan *Message
	conn         *net.UDPConn

	addrMutex sync.Mutex
	addrCache map[string]*net.UDPAddr

	sendSeq     *linkSendSeq
	recvTracker *linkRecvTracker
}

func setupUDPTransport(localAddress string, receiveBuffer int) (*UDPTransport, error) {
	udpAddress, err := net.ResolveUDPAddr("udp", localAddress)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}

	transport := &UDPTransport{
		localAddress: localAddress,
		recvCh:       make(chan *Message, receiveBuffer),
		conn:         conn,
		addrCache:    make(map[string]*net.UDPAddr),
		sendSeq:      newLinkSendSeq(),
		recvTracker:  newLinkRecvTracker(),
	}

	go transport.busyReceiver()

	return transport, nil
}

func (transport *UDPTransport) getAddress() string {
	return transport.localAddress
}

func (udpTransport *UDPTransport) resolveCached(address string) (*net.UDPAddr, error) {
	udpTransport.addrMutex.Lock()
	cached, ok := udpTransport.addrCache[address]
	udpTransport.addrMutex.Unlock()
	if ok {
		return cached, nil
	}

	resolved, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	udpTransport.addrMutex.Lock()
	udpTransport.addrCache[address] = resolved
	udpTransport.addrMutex.Unlock()

	return resolved, nil
}

func (udpTransport *UDPTransport) Send(address string, msg *Message) error {
	msg.Body.SenderAddress = udpTransport.localAddress
	msg.Body.Sequence = udpTransport.sendSeq.next(address)
	data, err := encode(msg.Type, msg.Body)
	if err != nil {
		return err
	}

	udpAddress, err := udpTransport.resolveCached(address)
	if err != nil {
		return err
	}

	_, err = udpTransport.conn.WriteToUDP(data, udpAddress)
	if err != nil {
		return err
	}

	return nil
}

func (udpTransport *UDPTransport) Receive() <-chan *Message {
	return udpTransport.recvCh
}

func (udpTransport *UDPTransport) Close() error {
	return udpTransport.conn.Close()
}

func (udpTransport *UDPTransport) LossStats() map[string]LinkStats {
	return udpTransport.recvTracker.snapshot()
}

// Over 10000, so that its possible to exceed 9000 byte jumbo frames
func (udpTransport *UDPTransport) MaxMessageBytes() int {
	return 10200
}

func (udpTransport *UDPTransport) busyReceiver() {
	buffer := make([]byte, 11000)
	for {
		length, _, err := udpTransport.conn.ReadFromUDP(buffer)
		if err != nil {
			close(udpTransport.recvCh)
			return
		}

		data := make([]byte, length)
		copy(data, buffer[:length])
		msgType, msg, err := decode(data)
		if err != nil {
			continue
		}

		select {
		case udpTransport.recvCh <- &Message{Type: msgType, Body: msg, RemoteAddress: msg.SenderAddress}:
			udpTransport.recvTracker.record(msg.SenderAddress, msg.Sequence)
		default:
			continue
		}
	}
}

// TCP transport
type pooledConn struct {
	conn  net.Conn
	mutex sync.Mutex
}

type TCPTransport struct {
	localAddress string
	recvCh       chan *Message
	listener     net.Listener

	connsMutex sync.Mutex
	conns      map[string]*pooledConn

	acceptedMutex sync.Mutex
	accepted      []net.Conn

	sendSeq     *linkSendSeq
	recvTracker *linkRecvTracker
}

func setupTCPTransport(localAddress string, receiveBuffer int) (*TCPTransport, error) {
	listener, err := net.Listen("tcp", localAddress)
	if err != nil {
		return nil, err
	}

	transport := &TCPTransport{
		localAddress: localAddress,
		recvCh:       make(chan *Message, receiveBuffer),
		listener:     listener,
		conns:        make(map[string]*pooledConn),
		sendSeq:      newLinkSendSeq(),
		recvTracker:  newLinkRecvTracker(),
	}
	go transport.acceptLoop()

	return transport, nil
}

func (transport *TCPTransport) getAddress() string {
	return transport.localAddress
}

func (transport *TCPTransport) acceptLoop() {
	for {
		conn, err := transport.listener.Accept()
		if err != nil {
			return
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
		}
		transport.acceptedMutex.Lock()
		transport.accepted = append(transport.accepted, conn)
		transport.acceptedMutex.Unlock()
		go transport.readLoop(conn)
	}
}

func (transport *TCPTransport) readLoop(conn net.Conn) {
	for {
		var lengthBuffer [4]byte
		if _, err := io.ReadFull(conn, lengthBuffer[:]); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lengthBuffer[:])
		buf := make([]byte, 4+length)
		copy(buf[0:4], lengthBuffer[:])
		if _, err := io.ReadFull(conn, buf[4:]); err != nil {
			return
		}
		msgType, msg, err := decode(buf)
		if err != nil {
			continue
		}
		transport.recvTracker.record(msg.SenderAddress, msg.Sequence)

		transport.recvCh <- &Message{Type: msgType, Body: msg, RemoteAddress: msg.SenderAddress}
	}
}

func (transport *TCPTransport) getConnection(address string) (*pooledConn, error) {
	transport.connsMutex.Lock()
	entry, ok := transport.conns[address]
	transport.connsMutex.Unlock()
	if ok {
		return entry, nil
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	newEntry := &pooledConn{conn: conn}

	transport.connsMutex.Lock()
	if existing, ok := transport.conns[address]; ok {
		transport.connsMutex.Unlock()
		conn.Close()
		return existing, nil
	}
	transport.conns[address] = newEntry
	transport.connsMutex.Unlock()

	return newEntry, nil
}

func (transport *TCPTransport) dropConn(address string, entry *pooledConn) {
	transport.connsMutex.Lock()
	if transport.conns[address] == entry {
		delete(transport.conns, address)
	}
	transport.connsMutex.Unlock()
	entry.conn.Close()
}

func (transport *TCPTransport) Send(address string, msg *Message) error {
	msg.Body.SenderAddress = transport.localAddress
	msg.Body.Sequence = transport.sendSeq.next(address)
	data, err := encode(msg.Type, msg.Body)
	if err != nil {
		return err
	}

	entry, err := transport.getConnection(address)
	if err != nil {
		return err
	}

	entry.mutex.Lock()
	_, err = entry.conn.Write(data)
	entry.mutex.Unlock()

	if err != nil {
		transport.dropConn(address, entry)
	}

	return err
}

func (transport *TCPTransport) Receive() <-chan *Message {
	return transport.recvCh
}

func (transport *TCPTransport) Close() error {
	err := transport.listener.Close()

	transport.connsMutex.Lock()
	for _, entry := range transport.conns {
		entry.conn.Close()
	}
	transport.conns = make(map[string]*pooledConn)
	transport.connsMutex.Unlock()

	transport.acceptedMutex.Lock()
	for _, conn := range transport.accepted {
		conn.Close()
	}
	transport.accepted = nil
	transport.acceptedMutex.Unlock()

	return err
}

func (transport *TCPTransport) LossStats() map[string]LinkStats {
	return transport.recvTracker.snapshot()
}

func (transport *TCPTransport) MaxMessageBytes() int {
	return 1 << 20 // 1 MiB
}
