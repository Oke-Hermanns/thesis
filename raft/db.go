package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type kvFsm struct {
	db *sync.Map
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
	if op != Put {
		return fmt.Errorf("Only put is currently supported")
	}
	kf.db.Store(key, value)
	return nil
}

func (kf *kvFsm) Get(key string) (any, bool) {
	val, b := kf.db.Load(key)
	return val, b
}
