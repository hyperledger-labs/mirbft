/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package simplewal is a basic WAL implementation meant to be the first 'real' WAL
// option for mirbft.  More sophisticated WALs with checksums, byte alignments, etc.
// may be produced in the future, but this is just a simple place to start.
package simplewal

import (
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

func Open(path string) (*WAL, error) {
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

	err = result.LoadAll(func(i uint64, p *pb.Persistent) {
		if _, ok := p.Type.(*pb.Persistent_CEntry); ok {
			result.checkpointIndices = append(result.checkpointIndices, i)
		}
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (w *WAL) IsEmpty() (bool, error) {
	firstIndex, err := w.log.FirstIndex()
	if err != nil {
		return false, errors.WithMessage(err, "could not read first index")
	}

	return firstIndex == 0, nil
}

func (w *WAL) LoadAll(forEach func(index uint64, p *pb.Persistent)) error {
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

		result := &pb.Persistent{}
		err = proto.Unmarshal(data, result)
		if err != nil {
			return errors.WithMessage(err, "error decoding to proto, is the WAL corrput?")
		}

		forEach(i, result)
	}

	return nil
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

func (w *WAL) Close() error {
	return w.log.Close()
}
