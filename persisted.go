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

	pSet          map[uint64]*pb.PEntry            // Seq -> PEntry
	qSet          map[uint64]map[uint64]*pb.QEntry // Seq -> Epoch -> QEntry
	cSet          map[uint64]*pb.CEntry            // Seq -> CEntry
	lastCommitted uint64                           // Seq

	myConfig *Config
}

func newPersisted(myConfig *Config) *persisted {
	return &persisted{
		pSet:     map[uint64]*pb.PEntry{},
		qSet:     map[uint64]map[uint64]*pb.QEntry{},
		cSet:     map[uint64]*pb.CEntry{},
		myConfig: myConfig,
	}
}

func (p *persisted) appendLogEntry(entry *pb.Persisted) {
	if p.logHead == nil {
		p.logHead = &logEntry{
			entry: entry,
		}
		p.logTail = p.logHead
	} else {
		p.logHead.next = &logEntry{
			entry: entry,
		}
		p.logTail = p.logHead.next
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
			persisted.addCEntry(d.CEntry)
		default:
			panic("unrecognized data type")
		}
		index++
	}

	checkpoints := make([]*pb.CEntry, len(persisted.cSet))
	i := 0
	for _, cEntry := range persisted.cSet {
		checkpoints[i] = cEntry
		i++
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].SeqNo < checkpoints[j].SeqNo
	})

	if len(checkpoints) >= 3 {
		persisted.truncate(checkpoints[len(checkpoints)-3].SeqNo)
	} else {
		persisted.truncate(checkpoints[0].SeqNo)
	}

	persisted.lastCommitted = checkpoints[len(checkpoints)-1].SeqNo

	return persisted, nil
}

func (p *persisted) addPEntry(pEntry *pb.PEntry) *Actions {
	if oldEntry, ok := p.pSet[pEntry.SeqNo]; ok && oldEntry.Epoch >= pEntry.Epoch {
		panic("dev sanity test, remove me")
	}

	p.pSet[pEntry.SeqNo] = pEntry

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
	qSeqMap, ok := p.qSet[qEntry.SeqNo]
	if !ok {
		qSeqMap = map[uint64]*pb.QEntry{}
		p.qSet[qEntry.SeqNo] = qSeqMap
	}

	qSeqMap[qEntry.Epoch] = qEntry

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

	p.cSet[cEntry.SeqNo] = cEntry

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
	for seqNo := range p.pSet {
		if seqNo <= lowWatermark {
			delete(p.pSet, seqNo)
			delete(p.qSet, seqNo)
		}
	}

	for seqNo := range p.qSet {
		if seqNo <= lowWatermark {
			delete(p.qSet, seqNo)
		}
	}

	for seqNo := range p.cSet {
		if seqNo < lowWatermark {
			delete(p.cSet, seqNo)
		}
	}

	lowest := p.logHead

outer:
	for head := p.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persisted_PEntry:
			if d.PEntry.SeqNo > lowWatermark {
				break outer
			}
		case *pb.Persisted_QEntry:
			if d.QEntry.SeqNo > lowWatermark {
				break outer
			}
		case *pb.Persisted_CEntry:
			lowest = head
		default:
			panic("unrecognized data type")
		}
	}

	p.logHead = lowest
}

func (p *persisted) constructEpochChange(newEpoch uint64, ct *checkpointTracker) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	var highestStableCheckpoint *pb.Checkpoint
	var checkpoints []*pb.Checkpoint
	var networkConfig *pb.NetworkConfig
	for seqNo, cEntry := range p.cSet {
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
		qSubSet, ok := p.qSet[seqNo]
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

		pEntry, ok := p.pSet[seqNo]
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
