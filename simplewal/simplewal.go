/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package simplewal is a basic WAL implementation meant to be the first 'real' WAL
// option for mirbft.  More sophisticated WALs with checksums, byte alignments, etc.
// may be produced in the future, but this is just a simple place to start.
package simplewal

import (
	"io"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/tidwall/wal"
)

type WAL struct {
	nextIndex         uint64
	checkpointIndices []uint64
	log               *wal.Log
}

type Iterator struct {
	currentIndex uint64
	stopIndex    uint64
	log          *wal.Log
}

func (i *Iterator) LoadNext() (*pb.Persistent, error) {
	if i.currentIndex > i.stopIndex {
		return nil, io.EOF
	}

	data, err := i.log.Read(i.currentIndex)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not read index %d", i.currentIndex)
	}

	result := &pb.Persistent{}
	err = proto.Unmarshal(data, result)
	if err != nil {
		return nil, errors.WithMessage(err, "error decoding to proto, is the WAL corrput?")
	}

	i.currentIndex++

	return result, nil
}

// New creates a new WAL, populated with an initial network state and application value.
func New(path string, initialState *pb.NetworkState, initialValue []byte) (*WAL, error) {
	log, err := wal.Open(path, &wal.Options{
		NoSync: true,
		NoCopy: true,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "could not open WAL")
	}

	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, errors.WithMessage(err, "could not read last index")
	}

	if lastIndex != 0 {
		log.Close()
		return nil, errors.Errorf("new WAL already has data in it")
	}

	initialCheckpoint := &pb.Persistent{
		Type: &pb.Persistent_CEntry{
			CEntry: &pb.CEntry{
				SeqNo:           0,
				CheckpointValue: initialValue,
				NetworkState:    initialState,
				EpochConfig: &pb.EpochConfig{
					Number:            0,
					Leaders:           initialState.Config.Nodes,
					PlannedExpiration: 0,
				},
			},
		},
	}

	initialEpochChange := &pb.Persistent{
		Type: &pb.Persistent_EpochChange{
			EpochChange: &pb.EpochChange{
				NewEpoch: 1,
				Checkpoints: []*pb.Checkpoint{
					{
						SeqNo: 0,
						Value: initialValue,
					},
				},
			},
		},
	}

	result := &WAL{
		log:       log,
		nextIndex: 1,
	}

	if err := result.Append(initialCheckpoint); err != nil {
		log.Close()
		return nil, errors.WithMessage(err, "could not append initial checkpoint")
	}

	if err := result.Append(initialEpochChange); err != nil {
		log.Close()
		return nil, errors.WithMessage(err, "could not append initial epoch change")
	}

	if err := log.Sync(); err != nil {
		log.Close()
		return nil, errors.WithMessage(err, "could not sync log to filesystem")
	}

	return result, nil
}

func (w *WAL) Open(path string) (*WAL, error) {
	log, err := wal.Open(path, &wal.Options{
		NoSync: true,
		NoCopy: true,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "could not open WAL")
	}

	result := &WAL{
		log: log,
	}

	i, err := result.Iterator()
	if err != nil {
		log.Close()
		return nil, err
	}

	if i.currentIndex == 0 || i.stopIndex == 0 {
		log.Close()
		return nil, errors.Errorf("WAL is empty, was it deleted?")
	}

	result.nextIndex = i.stopIndex + 1

	var checkpointIndices []uint64

	for {
		currentIndex := i.currentIndex
		next, err := i.LoadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Close()
			return nil, errors.WithMessage(err, "could not read next entry")
		}

		if _, ok := next.Type.(*pb.Persistent_CEntry); ok {
			checkpointIndices = append(checkpointIndices, currentIndex)
		}
	}

	result.checkpointIndices = checkpointIndices

	return result, nil
}

func (w *WAL) Iterator() (*Iterator, error) {
	firstIndex, err := w.log.FirstIndex()
	if err != nil {
		return nil, errors.WithMessage(err, "could not read first index")
	}

	lastIndex, err := w.log.LastIndex()
	if err != nil {
		return nil, errors.WithMessage(err, "could not read first index")
	}

	return &Iterator{
		currentIndex: firstIndex,
		stopIndex:    lastIndex,
		log:          w.log,
	}, nil
}

func (w *WAL) Append(p *pb.Persistent) error {
	data, err := proto.Marshal(p)
	if err != nil {
		return errors.WithMessage(err, "could not marshal")
	}

	w.log.Write(w.nextIndex, data)

	if _, ok := p.Type.(*pb.Persistent_CEntry); ok {
		w.checkpointIndices = append(w.checkpointIndices, w.nextIndex)
		if len(w.checkpointIndices) > 3 {
			pruneIndex := w.checkpointIndices[len(w.checkpointIndices)-3]
			w.checkpointIndices = w.checkpointIndices[len(w.checkpointIndices)-3:]
			err = w.log.TruncateFront(pruneIndex)
			if err != nil {
				return errors.WithMessage(err, "could not truncate log")
			}
		}
	}

	w.nextIndex++

	return nil
}

func (w *WAL) Sync() error {
	return w.log.Sync()
}
