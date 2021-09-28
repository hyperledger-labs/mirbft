/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: This is the original old code with very few modifications.
//       Go through all of it, comment what is to be kept and delete what is not needed.

// TODO: Decide whether to keep an explicit index or only use the Append function,
//       truncating using a more abstract concept (e.g. increasing non-unique numbers attached to entries).

// Package simplewal is a basic WAL implementation meant to be the first 'real' WAL
// option for mirbft. More sophisticated WALs with checksums, byte alignments, etc.
// may be produced in the future, but this is just a simple place to start.
package simplewal

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"sync"

	"github.com/pkg/errors"
	"github.com/tidwall/wal"
	"google.golang.org/protobuf/proto"
)

type WAL struct {
	mutex sync.Mutex
	log   *wal.Log
	idx   uint64
}

func Open(path string) (*WAL, error) {

	// Create underlying log
	log, err := wal.Open(path, &wal.Options{
		NoSync: true,
		NoCopy: true,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "could not open WAL")
	}

	// Initialize index.
	// The LastIndex obtained form the tidwall implementation happens to be the next index
	// (in terms of our WAL abstraction), as the underlying implementation starts counting at 1 and we start at 0.
	idx, err := log.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed obtaining last WAL index: %w", err)
	}

	// Return new object implementing the WAL abstraction.
	return &WAL{
		log: log,
		idx: idx,
	}, nil
}

func (w *WAL) IsEmpty() (bool, error) {
	firstIndex, err := w.log.FirstIndex()
	if err != nil {
		return false, errors.WithMessage(err, "could not read first index")
	}

	return firstIndex == 0, nil
}

func (w *WAL) LoadAll(forEach func(index uint64, p *eventpb.Event)) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	firstIndex, err := w.log.FirstIndex()
	if err != nil {
		return errors.WithMessage(err, "could not read first index")
	}

	if firstIndex == 0 {
		// WAL is empty
		return nil
	}

	lastIndex, err := w.log.LastIndex()
	if err != nil {
		return errors.WithMessage(err, "could not read first index")
	}

	for i := firstIndex; i <= lastIndex; i++ {
		data, err := w.log.Read(i)
		if err != nil {
			return errors.WithMessagef(err, "could not read index %d", i)
		}

		result := &eventpb.Event{}
		err = proto.Unmarshal(data, result)
		if err != nil {
			return errors.WithMessage(err, "error decoding to proto, is the WAL corrupt?")
		}

		forEach(i, result)
	}

	return nil
}

func (w *WAL) Write(index uint64, p *eventpb.Event) error {

	// Check whether the index corresponds to the next index
	if w.idx != index {
		return fmt.Errorf("invalid wal index: expected %d, got %d", w.idx, index)
	}

	data, err := proto.Marshal(p)
	if err != nil {
		return errors.WithMessage(err, "could not marshal")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.idx = index + 1
	return w.log.Write(index+1, data) // The log implementation seems to be indexing starting with 1.
}

func (w *WAL) Append(event *eventpb.Event) error {
	return w.Write(w.idx, event)
}

func (w *WAL) Truncate(index uint64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.log.TruncateFront(index)
}

func (w *WAL) Sync() error {
	return w.log.Sync()
}

func (w *WAL) Close() error {
	return w.log.Close()
}
