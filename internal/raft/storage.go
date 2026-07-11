package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"

	"aegian/proto"
)

type storage struct {
	db *bolt.DB
}

var raftBucket = []byte("raft")

func newStorage(nodeID int32) (*storage, error) {
	if err := os.MkdirAll("data", 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join("data", "node-"+itoa(int(nodeID))+".db")

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(raftBucket)
		return e
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &storage{db: db}, nil
}

func (s *storage) save(currentTerm int32, votedFor int32, log []*proto.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(raftBucket)

		if err := b.Put([]byte("currentTerm"), int32ToBytes(currentTerm)); err != nil {
			return err
		}
		if err := b.Put([]byte("votedFor"), int32ToBytes(votedFor)); err != nil {
			return err
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(log); err != nil {
			return err
		}
		return b.Put([]byte("log"), buf.Bytes())
	})
}

func (s *storage) load() (currentTerm int32, votedFor int32, log []*proto.LogEntry, ok bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(raftBucket)

		termBytes := b.Get([]byte("currentTerm"))
		if termBytes == nil {
			return nil
		}
		ok = true

		currentTerm = bytesToInt32(termBytes)
		votedFor = bytesToInt32(b.Get([]byte("votedFor")))

		logBytes := b.Get([]byte("log"))
		if logBytes != nil {
			return gob.NewDecoder(bytes.NewReader(logBytes)).Decode(&log)
		}
		return nil
	})
	return currentTerm, votedFor, log, ok, err
}

func (s *storage) close() error {
	return s.db.Close()
}

func int32ToBytes(v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf
}

func bytesToInt32(buf []byte) int32 {
	if len(buf) < 4 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(buf))
}
