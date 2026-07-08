package main

import (
	"encoding/binary"
	"io"
	"net"
)

type MessageType uint8

const (
	AppendEntries MessageType = iota + 1
	AppendEntriesResponse
)

type LogEntry struct {
	Size    uint16
	Term    uint32
	Index   uint64
	Payload []byte
}

type AppendEntriesMsg struct {
	Term         uint32
	LeaderID     uint16
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64

	// Fields send in the response message
	Success     bool
	CurrentTerm uint32 // TODO: Not sure if this is needed, but specified in the Raft paper
}

type Message struct {
	Type       MessageType
	Body       *AppendEntriesMsg
	RemoteAddr string
}

func encode(msgType MessageType, msg *AppendEntriesMsg) ([]byte, error) {
	// 1 + 4 + 2 + 8 + 8 + 8 + 1 + 4 + 2 for message type, Term, LeaderID, PrevLogIndex, PrevLogTerm, LeaderCommit, Success, CurrentTerm, number of entries
	bodySize := 1 + 4 + 2 + 8 + 8 + 8 + 1 + 4 + 2

	for entry := range msg.Entries {
		bodySize += 2 + 4 + 8 + len(msg.Entries[entry].Payload)
	}

	buffer := make([]byte, bodySize)
	offset := 0

	buffer[offset] = byte(msgType)
	offset += 1

	binary.BigEndian.PutUint32(buffer[offset:], msg.Term)
	offset += 4
	binary.BigEndian.PutUint16(buffer[offset:], msg.LeaderID)
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
	binary.BigEndian.PutUint16(buffer[offset:], uint16(len(msg.Entries)))
	offset += 2

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

	// Add body length at the start
	// TODO: This certainly can be done more efficiently
	messageSize := len(buffer) + 4
	bufferWithLength := make([]byte, messageSize)
	binary.BigEndian.PutUint32(bufferWithLength[0:4], uint32(len(buffer)))
	copy(bufferWithLength[4:], buffer)

	return bufferWithLength, nil
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
	if len(data) < 38 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	message := &AppendEntriesMsg{}

	msgType := MessageType(data[0])
	term := binary.BigEndian.Uint32(data[1:5])
	leaderID := binary.BigEndian.Uint16(data[5:7])
	prevLogIndex := binary.BigEndian.Uint64(data[7:15])
	prevLogTerm := binary.BigEndian.Uint64(data[15:23])
	leaderCommit := binary.BigEndian.Uint64(data[23:31])
	success := data[31]
	currentTerm := binary.BigEndian.Uint32(data[32:36])
	numEntries := binary.BigEndian.Uint16(data[36:38])

	entryOffset := 38

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
			Size:    entrySize,
			Term:    entryTerm,
			Index:   entryIndex,
			Payload: payload,
		})

		entryOffset += int(entrySize)
	}

	message = &AppendEntriesMsg{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		Entries:      message.Entries,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: leaderCommit,
		Success:      success == 1,
		CurrentTerm:  currentTerm,
	}
	return MessageType(msgType), message, nil
}

type Transport interface {
	Send(address string, msg *Message) error
	Receive() <-chan *Message
	Close() error
}

// UDP
// TODO: Sequence numbers might be worth a try, but for now unpacking and checking index and term should be fine, check later again when raft is working

type UDPTransport struct {
	localAddress string
	recvCh       chan *Message
	conn         *net.UDPConn
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
	}

	go transport.busyReceiver()

	return transport, nil
}

func (udpTransport *UDPTransport) Send(address string, msg *Message) error {
	data, err := encode(msg.Type, msg.Body)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	_, err = udpTransport.conn.WriteToUDP(data, udpAddr)
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

func (udpTransport *UDPTransport) busyReceiver() {
	// Reminder to check in raft protocol that messages > 9000 bytes (-headers) are never sent
	buffer := make([]byte, 9000)
	for {
		length, addr, err := udpTransport.conn.ReadFromUDP(buffer)
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
		case udpTransport.recvCh <- &Message{Type: msgType, Body: msg, RemoteAddr: addr.String()}:
		default:
			continue
		}
	}
}

// TODO: TCP communication
