/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: This is the original old code with very few modifications.
//       Go through all of it, comment what is to be kept and delete what is not needed.

// Package reqstore is an implementation of the RequestStore utilized by the samples.
// Depending on your application, it may or may not be appropriate.  In particular, if your
// application wants to retain the requests rather than simply apply and discard them, you may
// wish to write your own or adapt this one.
package reqstore

import (
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"github.com/pkg/errors"
)

func reqKey(ack *requestpb.RequestRef) []byte {
	return []byte(fmt.Sprintf("req-%d.%d.%x", ack.ClientId, ack.ReqNo, ack.Digest))
}

func allocKey(clientID t.ClientID, reqNo t.ReqNo) []byte {
	return []byte(fmt.Sprintf("alloc-%d.%d", clientID, reqNo))
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

func (s *Store) PutAllocation(clientID t.ClientID, reqNo t.ReqNo, digest []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(allocKey(clientID, reqNo), digest)
	})
}

func (s *Store) GetAllocation(clientID t.ClientID, reqNo t.ReqNo) ([]byte, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(allocKey(clientID, reqNo))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return valCopy, err
}

func (s *Store) PutRequest(requestRef *requestpb.RequestRef, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(reqKey(requestRef), data)
	})
}

func (s *Store) GetRequest(requestRef *requestpb.RequestRef) ([]byte, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(reqKey(requestRef))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return valCopy, err
}

func (s *Store) Commit(ack *requestpb.RequestRef) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(reqKey(ack))
	})
}

func (s *Store) Sync() error {
	return s.db.Sync()
}

func (s *Store) Close() {
	s.db.Close()
}
