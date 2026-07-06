package main

type MessageType uint8

const (
	AppendEntries MessageType = iota + 1
	AppendEntriesResponse
)

type LogEntry struct {
	Term    uint32
	Index   uint64
	Payload []byte
}

type AppendEntriesMsg struct {
	Term uint64
	LeaderID string
	PrevLogIndex uint64
	PrevLogTerm uint64
	Entries []LogEntry
	LeaderCommit uint64

	// Fields send in the response message
	Success bool
	CurrentTerm uint64 // TODO: Not sure if this is needed, but specified in the Raft paper
}

type Message struct {
	Type MessageType
	Body *AppendEntriesMsg
	RemoteAddr string
}