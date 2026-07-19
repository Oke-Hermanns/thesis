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
	put operation = iota + 1
	get
	del
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
	NextIndex  map[uint8]uint64
	MatchIndex map[uint8]uint64
	Peers      []peer
	// For follower only
	LeaderAddress string

	mutex        sync.Mutex
	newCommit    *sync.Cond
	stateMachine *kvFsm

	// For follower only, closed when leader acknowledges join
	joined   chan struct{}
	joinOnce sync.Once
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
		joined:       make(chan struct{}),
	}
	node.newCommit = sync.NewCond(&node.mutex)
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

// Currently only way to shutdown is to kill the node, add shutdown function later
func (node *RaftNode) heartbeatSender() {
	ticker := time.NewTicker(node.Config.HeartbeatTimeout)
	defer ticker.Stop()

	for range ticker.C {
		node.broadcast()
	}
}

func (node *RaftNode) broadcast() {
	node.mutex.Lock()
	// Copy peers
	peers := append([]peer(nil), node.Peers...)
	node.mutex.Unlock()

	for _, peer := range peers {
		node.sendAppendEntriesToFollower(peer)
	}
}

func (node *RaftNode) sendAppendEntriesToFollower(peer peer) {
	node.mutex.Lock()
	nextIndex := node.NextIndex[peer.Id]
	var prevLogTerm uint64
	if nextIndex > 0 && nextIndex <= uint64(len(node.Log)) {
		prevLogTerm = uint64(node.Log[nextIndex-1].Term)
	}
	var entries []LogEntry
	if nextIndex < uint64(len(node.Log)) {
		entries = []LogEntry{node.Log[nextIndex]}
	}

	appendEntry := &AppendEntriesMsg{
		Term:         node.CurrentTerm,
		IDofSender:   uint16(node.Id),
		PrevLogIndex: nextIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: node.CommitIndex,
	}
	node.mutex.Unlock()

	node.Transport.Send(peer.Address, &Message{AppendEntries, appendEntry, peer.Address})
}
func (node *RaftNode) applyCommandToLeader(op operation, data any) (uint64, error) {
	node.mutex.Lock()
	if node.Role != Leader {
		node.mutex.Unlock()
		return 0, fmt.Errorf("not leader")
	}
	if op != put && op != del {
		node.mutex.Unlock()
		return 0, fmt.Errorf("invalid operation")
	}
	transformedData, ok := data.([]byte)
	if !ok {
		node.mutex.Unlock()
		return 0, fmt.Errorf("failed to transform data to bytes")
	}
	entry := LogEntry{
		Term:    node.CurrentTerm,
		Index:   uint64(len(node.Log) + 1),
		Payload: transformedData,
	}

	node.Log = append(node.Log, entry)
	peers := append([]peer(nil), node.Peers...)
	node.mutex.Unlock()

	for _, peer := range peers {
		node.sendAppendEntriesToFollower(peer)
	}
	return entry.Index, nil
}

func (node *RaftNode) awaitCommit(index uint64) {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	for node.CommitIndex < index {
		node.newCommit.Wait()
	}
}

func (node *RaftNode) handleClientRequest(req *clientRequest) *clientResponse {
	node.mutex.Lock()
	isLeader := node.Role == Leader
	leaderHint := node.LeaderAddress
	node.mutex.Unlock()

	if !isLeader {
		return &clientResponse{Success: false, LeaderHint: leaderHint, Error: "not leader"}
	}

	switch req.Op {
	case get:
		value, found := node.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		return &clientResponse{Success: true, Value: value.([]byte)}
	case put:
		index, err := node.applyCommandToLeader(put, encodeCommand(put, req.Key, req.Value))
		if err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		node.awaitCommit(index)
		return &clientResponse{Success: true}
	case del:
		value, found := node.stateMachine.Get(req.Key)
		if !found {
			return &clientResponse{Success: false, Error: "key not found"}
		}
		index, err := node.applyCommandToLeader(del, encodeCommand(del, req.Key, nil))
		if err != nil {
			return &clientResponse{Success: false, Error: err.Error()}
		}
		node.awaitCommit(index)
		return &clientResponse{Success: true, Value: value.([]byte)}
	default:
		return &clientResponse{Success: false, Error: "currently unsupported operation"}
	}
}

func (node *RaftNode) followerHandleAppendEntries(msg *Message) {
	if msg.Type != AppendEntries {
		log.Println("A follower can only handle AppendEntries, no AppendEntriesResponse")
		return
	}
	appendEntry := msg.Body

	node.mutex.Lock()

	if appendEntry.Term < node.CurrentTerm {
		node.mutex.Unlock()
		node.followerSendAppendEntriesResponse(false)
		return
	}

	if appendEntry.PrevLogIndex > uint64(len(node.Log)) ||
		(appendEntry.PrevLogIndex > 0 && node.Log[appendEntry.PrevLogIndex-1].Term != uint32(appendEntry.PrevLogTerm)) {
		node.mutex.Unlock()
		node.followerSendAppendEntriesResponse(false)
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
		node.newCommit.Broadcast()
	}
	node.mutex.Unlock()

	node.followerSendAppendEntriesResponse(true)
}

func (node *RaftNode) leaderHandleAppendEntriesResponse(msg *Message) {
	// The current way how the follower is identified is really bad and definietly has to be changed
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

	node.mutex.Lock()
	var followerIndex uint8
	var retryPeer peer
	found := false
	for _, p := range node.Peers {
		if p.Address == followerAddress {
			followerIndex = p.Id
			retryPeer = p
			found = true
			break
		}
	}
	if !found {
		node.mutex.Unlock()
		return
	}
	if response.Success {
		matchIndex := response.PrevLogIndex
		if matchIndex > node.MatchIndex[followerIndex] {
			node.MatchIndex[followerIndex] = matchIndex
			node.NextIndex[followerIndex] = matchIndex
			// TODO: Not sure if this is a smart way to do this
			node.tryAdvanceCommitIndex(matchIndex)
		}
		node.mutex.Unlock()
	} else {
		node.NextIndex[followerIndex] = node.NextIndex[followerIndex] - 1
		node.mutex.Unlock()
		node.sendAppendEntriesToFollower(retryPeer)
	}
}

func (node *RaftNode) followerSendAppendEntriesResponse(success bool) {
	resp := &Message{
		Type: AppendEntriesResponse,
		Body: &AppendEntriesMsg{
			CurrentTerm:  node.CurrentTerm,
			Success:      success,
			PrevLogIndex: uint64(len(node.Log)),
		},
		RemoteAddress: node.Transport.getAddress(),
	}

	node.Transport.Send(node.LeaderAddress, resp)
}

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
		node.newCommit.Broadcast()
	}
}

func (node *RaftNode) applyCommittedEntries() {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	for node.LastApplied < node.CommitIndex {
		node.LastApplied++
		entry := node.Log[node.LastApplied-1]
		node.mutex.Unlock()
		err := node.stateMachine.Apply(entry)
		if err != nil {
			log.Printf("Failed to apply entry %d: %v", entry.Index, err)
		}
		node.mutex.Lock()
	}
}

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

func (node *RaftNode) applyLoop() {
	for {
		node.mutex.Lock()
		for node.LastApplied >= node.CommitIndex {
			node.newCommit.Wait()
		}
		node.mutex.Unlock()
		node.applyCommittedEntries()
	}
}

// Functions to connect nodes in cluster
func (node *RaftNode) sendJoinToLeader() {
	ticker := time.NewTicker(node.Config.JoinRetryTimeout)
	defer ticker.Stop()

	node.sendJoin()
	for {
		select {
		case <-ticker.C:
			node.sendJoin()
		case <-node.joined:
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

	node.mutex.Lock()
	known := false
	for _, p := range node.Peers {
		if p.Id == id {
			known = true
			break
		}
	}
	if !known {
		node.Peers = append(node.Peers, peer{Id: id, Address: address})
		node.NextIndex[id] = 0
		node.MatchIndex[id] = 0
	}
	node.mutex.Unlock()

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
	node.joinOnce.Do(func() { close(node.joined) })
}
