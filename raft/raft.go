package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type role uint8
type operation uint8

const (
	Leader role = iota + 1
	Follower
)

const (
	Put operation = iota + 1
	Get
	Del
)

type RaftNode struct {
	Role        role
	Id          uint8
	CurrentTerm uint32
	Log         []LogEntry
	CommitIndex uint64
	LastApplied uint64
	Transport   Transport
	Config      config

	// For leader only
	NextIndex   map[uint8]uint64
	MatchIndex  map[uint8]uint64
	Peers       []peer
	Replicators map[uint8]chan struct{}
	// For follower only
	LeaderAddress string

	Mutex        sync.Mutex
	NewCommit    *sync.Cond
	stateMachine *kvFsm

	// For follower only, closed when leader acknowledges join
	Joined   chan struct{}
	JoinOnce sync.Once
}

func createNode(id uint8, transport Transport, config config, stateMachine *kvFsm) *RaftNode {
	node := &RaftNode{
		Id:           id,
		Transport:    transport,
		Config:       config,
		Log:          []LogEntry{},
		stateMachine: stateMachine,
		NextIndex:    make(map[uint8]uint64),
		MatchIndex:   make(map[uint8]uint64),
		Replicators:  make(map[uint8]chan struct{}),
		Joined:       make(chan struct{}),
	}
	node.NewCommit = sync.NewCond(&node.Mutex)
	return node
}

func (node *RaftNode) makeLeader() {
	node.Role = Leader
	node.CurrentTerm = 1

	go node.heartbeatSender()
	go node.receiveLoop()
	go node.applyLoop()

}

func (node *RaftNode) makeFollower(address string) {
	node.Role = Follower
	node.LeaderAddress = address
	node.CurrentTerm = 1

	go node.receiveLoop()
	go node.applyLoop()
	go node.sendJoinToLeader()
}

// Sends periodic heartbeats
func (node *RaftNode) heartbeatSender() {
	ticker := time.NewTicker(node.Config.HeartbeatTimeout)
	defer ticker.Stop()

	for range ticker.C {
		node.broadcast()
	}
}

// Initiates send to all followers (peers)
func (node *RaftNode) broadcast() {
	node.Mutex.Lock()
	ids := make([]uint8, len(node.Peers))
	for i, p := range node.Peers {
		ids[i] = p.Id
	}
	node.Mutex.Unlock()

	for _, id := range ids {
		node.notifyReplicator(id)
	}
}

// Replicator idea from hashicorp implementation
// Called whenever the Leader needs to send something (Heartbeat/AppendEntries) to a Follower
func (node *RaftNode) notifyReplicator(id uint8) {
	node.Mutex.Lock()
	ch := node.Replicators[id]
	node.Mutex.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Started by the Leader when a Follower joins, handles replication to followers asynchronously
func (node *RaftNode) startReplicator(id uint8) {
	ch := make(chan struct{}, 1)
	node.Replicators[id] = ch
	go func() {
		for range ch {
			node.sendAppendEntriesToFollower(id)
		}
	}()
}

// Sends to a specific follower, checks based on Raft paper conditions if entries have to be send
// If no entries have to be send to the follower, the payload is empty, which is the definition of a heartbeat message
func (node *RaftNode) sendAppendEntriesToFollower(id uint8) {
	// 8000 should work for all cloud environments with jumbo frames enabled
	const maxEntriesBytes = 8000
	node.Mutex.Lock()
	address := ""
	for _, p := range node.Peers {
		if p.Id == id {
			address = p.Address
			break
		}
	}
	if address == "" {
		node.Mutex.Unlock()
		return
	}
	nextIndex := node.NextIndex[id]
	var prevLogTerm uint64
	logLength := uint64(len(node.Log))
	if nextIndex > 0 && nextIndex <= logLength {
		prevLogTerm = uint64(node.Log[nextIndex-1].Term)
	}
	var entries []LogEntry
	entriesSize := 0
	for i := nextIndex; i < logLength; i++ {
		entrySize := 14 + len(node.Log[i].Payload)
		if entrySize+entriesSize > maxEntriesBytes {
			break
		}
		entries = append(entries, node.Log[i])
		entriesSize += entrySize
	}

	appendEntry := &AppendEntriesMsg{
		Term:         node.CurrentTerm,
		IDofSender:   uint16(node.Id),
		PrevLogIndex: nextIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: node.CommitIndex,
	}
	node.Mutex.Unlock()

	node.Transport.Send(address, &Message{AppendEntries, appendEntry, address})
}

// Check if request is valid and if that is the case, apply the command to the leader
func (node *RaftNode) handleClientRequest(req *clientRequest) *clientResponse {
	node.Mutex.Lock()
	isLeader := node.Role == Leader
	leaderHint := node.LeaderAddress
	node.Mutex.Unlock()

	if !isLeader {
		return &clientResponse{Success: false, LeaderHint: leaderHint, Error: "not leader"}
	}

	switch req.Op {
	case Get:
		value, found := node.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		return &clientResponse{Success: true, Value: value.([]byte)}
	case Put:
		index, err := node.applyCommandToLeader(Put, encodeCommand(Put, req.Key, req.Value))
		if err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		node.awaitAppliedToStateMachine(index)
		return &clientResponse{Success: true}
	case Del:
		value, found := node.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		index, err := node.applyCommandToLeader(Del, encodeCommand(Del, req.Key, nil))
		if err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		node.awaitAppliedToStateMachine(index)
		return &clientResponse{Success: true, Value: value.([]byte)}
	default:
		return &clientResponse{Success: false, Error: "currently unsupported operation"}
	}
}

// Append the command to the leaders log and start the replication process
func (node *RaftNode) applyCommandToLeader(op operation, data any) (uint64, error) {
	node.Mutex.Lock()
	if node.Role != Leader {
		node.Mutex.Unlock()
		return 0, fmt.Errorf("not leader")
	}
	if op != Put && op != Del {
		node.Mutex.Unlock()
		return 0, fmt.Errorf("invalid operation")
	}
	transformedData, ok := data.([]byte)
	if !ok {
		node.Mutex.Unlock()
		return 0, fmt.Errorf("failed to transform data to bytes")
	}
	entry := LogEntry{
		Term:    node.CurrentTerm,
		Index:   uint64(len(node.Log) + 1),
		Payload: transformedData,
	}

	node.Log = append(node.Log, entry)
	// For a cluster with only one node, leaderHandleAppendEntriesResponse is never called
	if len(node.Peers) == 0 {
		node.tryAdvanceCommitIndex(entry.Index)
		node.Mutex.Unlock()
	} else {
		ids := make([]uint8, len(node.Peers))
		for i, p := range node.Peers {
			ids[i] = p.Id
		}
		node.Mutex.Unlock()

		for _, id := range ids {
			node.notifyReplicator(id)
		}
	}
	return entry.Index, nil
}

// Block until index is applied to state machine, avoids get after put issues
func (node *RaftNode) awaitAppliedToStateMachine(index uint64) {
	node.Mutex.Lock()
	defer node.Mutex.Unlock()
	for node.LastApplied < index {
		node.NewCommit.Wait()
	}
}

// Append Entries to Follower if term and previous log index allow it
func (node *RaftNode) followerHandleAppendEntries(msg *Message) {
	if msg.Type != AppendEntries {
		log.Println("A follower can only handle AppendEntries, no AppendEntriesResponse")
		return
	}
	appendEntry := msg.Body

	node.Mutex.Lock()

	// There is no leader election, so the term will never change, but treated as if it was possible
	if appendEntry.Term < node.CurrentTerm {
		logLength, nodeTerm := uint64(len(node.Log)), node.CurrentTerm
		node.Mutex.Unlock()
		node.followerSendAppendEntriesResponse(false, logLength, nodeTerm)
		return
	}

	if appendEntry.PrevLogIndex > uint64(len(node.Log)) ||
		(appendEntry.PrevLogIndex > 0 && node.Log[appendEntry.PrevLogIndex-1].Term != uint32(appendEntry.PrevLogTerm)) {
		logLength, nodeTerm := uint64(len(node.Log)), node.CurrentTerm
		node.Mutex.Unlock()
		node.followerSendAppendEntriesResponse(false, logLength, nodeTerm)
		return
	}

	for _, entry := range appendEntry.Entries {
		if entry.Index <= uint64(len(node.Log)) {
			if node.Log[entry.Index-1].Term == entry.Term {
				continue
			}
			node.Log = node.Log[:entry.Index-1]
		}
		node.Log = append(node.Log, entry)
	}

	if appendEntry.LeaderCommit > node.CommitIndex {
		newCommitIndex := appendEntry.LeaderCommit
		if logLen := uint64(len(node.Log)); newCommitIndex > logLen {
			newCommitIndex = logLen
		}
		node.CommitIndex = newCommitIndex
		node.NewCommit.Broadcast()
	}
	logLength, nodeTerm := uint64(len(node.Log)), node.CurrentTerm
	node.Mutex.Unlock()

	node.followerSendAppendEntriesResponse(true, logLength, nodeTerm)
}

// Initiates new send by notifying replicator if msg is unsuccessful appendentries
// Does nothing for a heartbeat response
// Trys to advance commit indiex for successful append
func (node *RaftNode) leaderHandleAppendEntriesResponse(msg *Message) {
	if node.Role != Leader {
		log.Println("Follower is not allowed to call leaderHandleAppendEntriesResponse")
		return
	}
	if msg.Type != AppendEntriesResponse {
		log.Println("Leader currently can only handle AppendEntriesResponse")
		return
	}
	response := msg.Body
	followerAddress := msg.RemoteAddress

	node.Mutex.Lock()
	var followerIndex uint8
	found := false
	for _, p := range node.Peers {
		if p.Address == followerAddress {
			followerIndex = p.Id
			found = true
			break
		}
	}
	if !found {
		node.Mutex.Unlock()
		log.Printf("The node with address %s is currently not known", followerAddress)
		return
	}
	if response.Success {
		matchIndex := response.PrevLogIndex
		if matchIndex > node.MatchIndex[followerIndex] {
			node.MatchIndex[followerIndex] = matchIndex
			node.NextIndex[followerIndex] = matchIndex
			node.tryAdvanceCommitIndex(matchIndex)
		}
		node.Mutex.Unlock()
	} else {
		node.NextIndex[followerIndex] = response.PrevLogIndex
		node.Mutex.Unlock()
		node.notifyReplicator(followerIndex)
	}
}

// Notify leader about status of AppendEntries request
func (node *RaftNode) followerSendAppendEntriesResponse(success bool, logLength uint64, term uint32) {
	resp := &Message{
		Type: AppendEntriesResponse,
		Body: &AppendEntriesMsg{
			CurrentTerm:  term,
			Success:      success,
			PrevLogIndex: logLength,
		},
		RemoteAddress: node.Transport.getAddress(),
	}

	node.Transport.Send(node.LeaderAddress, resp)
}

// Advances the commit index if more than half of the nodes have a higher index in their log
func (node *RaftNode) tryAdvanceCommitIndex(indexToTry uint64) {
	if logLen := uint64(len(node.Log)); indexToTry > logLen {
		indexToTry = logLen
	}

	total := len(node.Peers) + 1
	quorum := total/2 + 1
	count := 0

	for _, i := range node.MatchIndex {
		if i >= indexToTry {
			count++
		}
	}

	if count+1 >= quorum && indexToTry > node.CommitIndex {
		node.CommitIndex = indexToTry
		node.NewCommit.Broadcast()
	}
}

// Applies entries to the key value state machine (kvFsm)
func (node *RaftNode) applyCommittedEntries() {
	node.Mutex.Lock()
	defer node.Mutex.Unlock()
	for node.LastApplied < node.CommitIndex {
		entry := node.Log[node.LastApplied]
		node.Mutex.Unlock()
		err := node.stateMachine.Apply(entry)
		if err != nil {
			log.Panicf("Failed to apply entry %d: %v", entry.Index, err)
		}
		node.Mutex.Lock()
		node.LastApplied++
		node.NewCommit.Broadcast()
	}
}

// Calls the correct function based on the message type received
func (node *RaftNode) receiveLoop() {
	// Runs as long as channel is open
	for msg := range node.Transport.Receive() {
		switch msg.Type {
		case AppendEntries:
			node.followerHandleAppendEntries(msg)
		case AppendEntriesResponse:
			node.leaderHandleAppendEntriesResponse(msg)
		case Join:
			node.leaderHandleJoin(msg)
		case JoinResponse:
			node.followerHandleJoinResponse()
		default:
			log.Printf("Something went wrong receiving message with this type: %d", msg.Type)
		}
	}
}

// Gets waken up when the commit index advances
func (node *RaftNode) applyLoop() {
	for {
		node.Mutex.Lock()
		for node.LastApplied >= node.CommitIndex {
			node.NewCommit.Wait()
		}
		node.Mutex.Unlock()
		node.applyCommittedEntries()
	}
}

/*
All functions below are not a part of the Raft algorithm, only a simple way to connect nodes to the leader
*/

func (node *RaftNode) sendJoinToLeader() {
	ticker := time.NewTicker(node.Config.JoinRetryTimeout)
	defer ticker.Stop()

	node.sendJoin()
	for {
		select {
		case <-ticker.C:
			node.sendJoin()
		case <-node.Joined:
			return
		}
	}
}

func (node *RaftNode) sendJoin() {
	msg := &Message{
		Type:          Join,
		Body:          &AppendEntriesMsg{IDofSender: uint16(node.Id)},
		RemoteAddress: node.Transport.getAddress(),
	}
	node.Transport.Send(node.LeaderAddress, msg)
}

func (node *RaftNode) leaderHandleJoin(msg *Message) {
	id := uint8(msg.Body.IDofSender)
	address := msg.RemoteAddress

	node.Mutex.Lock()
	known := false
	for i, p := range node.Peers {
		if p.Id == id {
			known = true
			if p.Address != address {
				node.Peers[i].Address = address
			}
			break
		}
	}
	if !known {
		node.Peers = append(node.Peers, peer{Id: id, Address: address})
		node.NextIndex[id] = 0
		node.MatchIndex[id] = 0
		node.startReplicator(id)
	}
	node.Mutex.Unlock()

	node.leaderSendJoinResponse(address)
}

func (node *RaftNode) leaderSendJoinResponse(followerAddress string) {
	msg := &Message{
		Type:          JoinResponse,
		Body:          &AppendEntriesMsg{IDofSender: uint16(node.Id)},
		RemoteAddress: node.Transport.getAddress(),
	}
	node.Transport.Send(followerAddress, msg)
}

func (node *RaftNode) followerHandleJoinResponse() {
	node.JoinOnce.Do(func() { close(node.Joined) })
}
