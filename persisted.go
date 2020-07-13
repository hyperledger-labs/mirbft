/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"
	"io"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mock/storage.go -fake-name Storage . Storage

type Storage interface {
	Load(index uint64) (*pb.Persisted, error)
}

type logEntry struct {
	entry *pb.Persisted
	next  *logEntry
}

type persisted struct {
	offset  uint64
	logHead *logEntry
	logTail *logEntry

	checkpoints   []*pb.CEntry
	lastCommitted uint64 // Seq

	myConfig *Config
}

func newPersisted(myConfig *Config) *persisted {
	return &persisted{
		myConfig:    myConfig,
		checkpoints: make([]*pb.CEntry, 3),
	}
}

func (p *persisted) appendLogEntry(entry *pb.Persisted) {
	p.offset++
	if p.logHead == nil {
		p.logHead = &logEntry{
			entry: entry,
		}
		p.logTail = p.logHead
	} else {
		p.logTail.next = &logEntry{
			entry: entry,
		}
		p.logTail = p.logTail.next
	}
}

func loadPersisted(config *Config, storage Storage) (*persisted, error) {
	persisted := newPersisted(config)

	var data *pb.Persisted
	var err error
	var index uint64

	for {
		data, err = storage.Load(index)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Errorf("failed to load persisted from Storage: %s", err)
		}

		switch d := data.Type.(type) {
		case *pb.Persisted_PEntry:
			persisted.addPEntry(d.PEntry)
		case *pb.Persisted_QEntry:
			persisted.addQEntry(d.QEntry)
		case *pb.Persisted_CEntry:
			persisted.lastCommitted = d.CEntry.SeqNo
			persisted.addCEntry(d.CEntry)
		default:
			panic("unrecognized data type")
		}
		index++
	}

	if persisted.checkpoints[0] == nil {
		panic("no checkpoints in log")
	}

	persisted.truncate(persisted.checkpoints[0].SeqNo)

	return persisted, nil
}

func (p *persisted) addPEntry(pEntry *pb.PEntry) *Actions {
	d := &pb.Persisted{
		Type: &pb.Persisted_PEntry{
			PEntry: pEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persisted: []*pb.Persisted{d},
	}

}

func (p *persisted) addQEntry(qEntry *pb.QEntry) *Actions {
	d := &pb.Persisted{
		Type: &pb.Persisted_QEntry{
			QEntry: qEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persisted: []*pb.Persisted{d},
	}
}

func (p *persisted) addCEntry(cEntry *pb.CEntry) *Actions {
	if cEntry.NetworkConfig == nil {
		panic("network config must be set")
	}

	switch {
	case p.checkpoints[0] == nil:
		p.checkpoints[0] = cEntry
	case p.checkpoints[1] == nil:
		p.checkpoints[1] = cEntry
	case p.checkpoints[2] == nil:
		p.checkpoints[2] = cEntry
	default:
		p.checkpoints[0] = p.checkpoints[1]
		p.checkpoints[1] = p.checkpoints[2]
		p.checkpoints[2] = cEntry
	}

	d := &pb.Persisted{
		Type: &pb.Persisted_CEntry{
			CEntry: cEntry,
		},
	}

	p.appendLogEntry(d)

	return &Actions{
		Persisted: []*pb.Persisted{d},
	}
}

func (p *persisted) setLastCommitted(seqNo uint64) {
	if p.lastCommitted+1 != seqNo {
		panic(fmt.Sprintf("dev sanity test, remove me: lastCommitted=%d >= seqNo=%d", p.lastCommitted, seqNo))
	}

	p.lastCommitted = seqNo
}

func (p *persisted) truncate(lowWatermark uint64) {
	for head := p.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persisted_PEntry:
			if d.PEntry.SeqNo > lowWatermark {
				return
			}
		case *pb.Persisted_QEntry:
			if d.QEntry.SeqNo > lowWatermark {
				return
			}
		case *pb.Persisted_CEntry:
			p.logHead = head
			if d.CEntry.SeqNo >= lowWatermark {
				return
			}
		default:
			panic("unrecognized data type")
		}
	}
}

func (p *persisted) sets() (pSet map[uint64]*pb.PEntry, qSet map[uint64]map[uint64]*pb.QEntry, cSet map[uint64]*pb.CEntry) {
	pSet = map[uint64]*pb.PEntry{}            // Seq -> PEntry
	qSet = map[uint64]map[uint64]*pb.QEntry{} // Seq -> Epoch -> QEntry
	cSet = map[uint64]*pb.CEntry{}            // Seq -> CEntry

	for head := p.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persisted_PEntry:
			pSet[d.PEntry.SeqNo] = d.PEntry
		case *pb.Persisted_QEntry:
			qSeqMap, ok := qSet[d.QEntry.SeqNo]
			if !ok {
				qSeqMap = map[uint64]*pb.QEntry{}
				qSet[d.QEntry.SeqNo] = qSeqMap
			}
			qSeqMap[d.QEntry.Epoch] = d.QEntry
		case *pb.Persisted_CEntry:
			cSet[d.CEntry.SeqNo] = d.CEntry
		default:
			panic("unrecognized data type")
		}
	}

	return pSet, qSet, cSet
}

func (p *persisted) constructEpochChange(newEpoch uint64, ct *checkpointTracker) *pb.EpochChange {
	pSet, qSet, cSet := p.sets()

	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	var highestStableCheckpoint *pb.Checkpoint
	var checkpoints []*pb.Checkpoint
	var networkConfig *pb.NetworkConfig
	for seqNo, cEntry := range cSet {
		pcp := ct.checkpoint(seqNo)
		cp := &pb.Checkpoint{
			SeqNo: seqNo,
			Value: cEntry.CheckpointValue,
		}
		if pcp.stable && (highestStableCheckpoint == nil || highestStableCheckpoint.SeqNo < seqNo) {
			highestStableCheckpoint = cp
			networkConfig = cEntry.NetworkConfig
		} else {
			checkpoints = append(checkpoints, cp)
		}
	}
	checkpoints = append(checkpoints, highestStableCheckpoint)

	if highestStableCheckpoint == nil {
		panic("this should never happen")
	}
	if highestStableCheckpoint != nil && networkConfig == nil {
		panic("this should really never happen")
	}

	// Note, this is so that our order is deterministic, across restarts
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].SeqNo < checkpoints[j].SeqNo
	})

	epochChange.Checkpoints = checkpoints

	for seqNo := highestStableCheckpoint.SeqNo; seqNo < highestStableCheckpoint.SeqNo+uint64(networkConfig.CheckpointInterval)*3; seqNo++ {
		qSubSet, ok := qSet[seqNo]
		if !ok {
			continue
		}

		qEntries := make([]*pb.QEntry, len(qSubSet))
		i := 0
		for _, qEntry := range qSubSet {
			qEntries[i] = qEntry
			i++
		}
		// Note, this is so that our order is deterministic, across restarts
		sort.Slice(qEntries, func(i, j int) bool {
			return qEntries[i].Epoch < qEntries[j].Epoch
		})

		for _, qEntry := range qEntries {
			epochChange.QSet = append(epochChange.QSet, &pb.EpochChange_SetEntry{
				SeqNo:  qEntry.SeqNo,
				Epoch:  qEntry.Epoch,
				Digest: qEntry.Digest,
			})
		}

		pEntry, ok := pSet[seqNo]
		if !ok {
			continue
		}

		epochChange.PSet = append(epochChange.PSet, &pb.EpochChange_SetEntry{
			SeqNo:  pEntry.SeqNo,
			Epoch:  pEntry.Epoch,
			Digest: pEntry.Digest,
		})

	}

	return epochChange
}
