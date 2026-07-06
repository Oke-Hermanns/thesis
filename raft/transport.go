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

	Success bool
	//MatchIndex uint64
	//FollowerID string

}

type Message struct {
	Type MessageType
	Body *AppendEntriesMsg
	RemoteAddr string
}