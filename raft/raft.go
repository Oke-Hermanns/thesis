package main

type RaftEntry struct {
	// Combination of term and index uniquely identifies a log entry
	Term    uint32
	Index   uint64
	Command []byte
}

type role uint8

const (
	Leader role = iota + 1
	Follower
)

type RaftNode struct {
	Role        role
	Id          uint8
	CurrentTerm uint32
	Log         []RaftEntry
	CommitIndex uint64
	LastApplied uint64

	// For leader only
	NextIndex  map[uint8]uint64
	MatchIndex map[uint8]uint64
	// For follower only
	LeaderAddress string
}

func createNode() {}

func makeLeader() {}

func makeFollower() {}

func applyCommandToLeader() {}

func heartbeatSender() {}

func entryBroadcast() {}

func entryToFollower() {}

func followerHandleAppendEntries() {}

func leaderHandleAppendEntriesResponse() {}

func advanceCommitIndex() {}

func applyCommitedEntries() {}

func followerSendAppendEntriesResponse() {}

func receiveLoop() {}
