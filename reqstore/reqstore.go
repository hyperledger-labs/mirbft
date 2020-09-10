/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package reqstore is an implementation of the RequestStore utilized by the samples.
// Depending on your application, it may or may not be appropriate.  In particular, if your
// application wants to retain the requests rather than simply apply and discard them, you may
// wish to write your own or adapt this one.
package reqstore

import (
	"fmt"
	pb "github.com/IBM/mirbft/mirbftpb"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
)

func key(ack *pb.RequestAck) []byte {
	return []byte(fmt.Sprintf("%d.%d.%x", ack.ClientId, ack.ReqNo, ack.Digest))
}

type Store struct {
	db *badger.DB
}

func Open(dirPath string) (*Store, error) {
	var badgerOpts badger.Options
	if dirPath == "" {
		badgerOpts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		badgerOpts = badger.DefaultOptions(dirPath).WithSyncWrites(false).WithTruncate(true)
		// TODO, maybe WithDetectConflicts as false?
	}
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, errors.WithMessage(err, "could not open backing db")
	}

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Store(requestAck *pb.RequestAck, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(requestAck), data)
	})
}
func (s *Store) Get(requestAck *pb.RequestAck) ([]byte, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key(requestAck))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		return err
	})

	return valCopy, err
}

func (s *Store) Sync() error {
	return s.db.Sync()
}

func (s *Store) Close() {
	s.db.Close()
}
