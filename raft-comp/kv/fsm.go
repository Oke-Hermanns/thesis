package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type operation uint8

// Must match the operation values in the own raft implementation (raft/raft.go)
// so both systems use the identical client protocol and command payload format.
const (
	put operation = iota + 1
	get
	del
)

// kvFsm mirrors the own implementation's kvFsm (raft/db.go): the same
// sync.Map store and the same command payload encoding, so both systems do
// identical state-machine work per committed entry.
type kvFsm struct {
	dbMutex sync.RWMutex
	db      *sync.Map
}

// encodeCommand packs op/key/value the same way as the own implementation:
// [1 byte op][2 bytes keyLen][key][value]. value is empty for delete.
func encodeCommand(op operation, key string, value []byte) []byte {
	buf := make([]byte, 1+2+len(key)+len(value))
	buf[0] = byte(op)
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(key)))
	copy(buf[3:3+len(key)], key)
	copy(buf[3+len(key):], value)
	return buf
}

func decodeCommand(payload []byte) (op operation, key string, value []byte, err error) {
	if len(payload) < 3 {
		return 0, "", nil, io.ErrUnexpectedEOF
	}
	op = operation(payload[0])
	keyLen := int(binary.BigEndian.Uint16(payload[1:3]))
	if len(payload) < 3+keyLen {
		return 0, "", nil, io.ErrUnexpectedEOF
	}
	key = string(payload[3 : 3+keyLen])
	value = payload[3+keyLen:]
	return op, key, value, nil
}

// Apply implements raft.FSM. The returned value surfaces through
// ApplyFuture.Response() on the leader.
func (kf *kvFsm) Apply(entry *raft.Log) interface{} {
	op, key, value, err := decodeCommand(entry.Data)
	if err != nil {
		return err
	}
	kf.dbMutex.Lock()
	defer kf.dbMutex.Unlock()
	switch op {
	case put:
		kf.db.Store(key, value)
	case del:
		kf.db.Delete(key)
	default:
		return fmt.Errorf("unsupported operation in log entry: %d", op)
	}
	return nil
}

func (kf *kvFsm) Get(key string) (any, bool) {
	kf.dbMutex.RLock()
	defer kf.dbMutex.RUnlock()
	val, b := kf.db.Load(key)
	return val, b
}

// Snapshot/Restore are required by raft.FSM but never exercised in the
// benchmark setup: the host uses DiscardSnapshotStore and a snapshot
// threshold high enough that snapshotting never triggers.
func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	return &noopSnapshot{}, nil
}

func (kf *kvFsm) Restore(snapshot io.ReadCloser) error {
	snapshot.Close()
	kf.db = &sync.Map{}
	return nil
}

type noopSnapshot struct{}

func (s *noopSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *noopSnapshot) Release() {}
