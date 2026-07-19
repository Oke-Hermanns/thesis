package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type kvFsm struct {
	dbMutex sync.RWMutex
	db      *sync.Map
}

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

func (kf *kvFsm) Apply(entry LogEntry) error {
	op, key, value, err := decodeCommand(entry.Payload)
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
		return fmt.Errorf("Only put, delete and get are supported, %d does not exist currently", op)
	}
	return nil
}

func (kf *kvFsm) Get(key string) (any, bool) {
	kf.dbMutex.RLock()
	defer kf.dbMutex.RUnlock()
	val, b := kf.db.Load(key)
	return val, b
}
