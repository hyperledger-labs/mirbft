/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type persisted struct {
	pSet          map[uint64]*pb.PEntry            // Seq -> PEntry
	qSet          map[uint64]map[uint64]*pb.QEntry // Seq -> Epoch -> QEntry
	checkpoints   map[uint64]*pb.Checkpoint        // Seq -> Checkpoint
	lastCommitted uint64                           // Seq

	networkConfig *pb.NetworkConfig
}

func (p *persisted) addPEntry(pEntry *pb.PEntry) {
	if p.pSet == nil {
		p.pSet = map[uint64]*pb.PEntry{}
	}

	if oldEntry, ok := p.pSet[pEntry.SeqNo]; ok && oldEntry.Epoch >= pEntry.Epoch {
		panic("dev sanity test, remove me")
	}

	p.pSet[pEntry.SeqNo] = pEntry
}

func (p *persisted) addQEntry(qEntry *pb.QEntry) {
	if p.qSet == nil {
		p.qSet = map[uint64]map[uint64]*pb.QEntry{}
	}

	qSeqMap, ok := p.qSet[qEntry.SeqNo]
	if !ok {
		qSeqMap = map[uint64]*pb.QEntry{}
		p.qSet[qEntry.SeqNo] = qSeqMap
	}

	qSeqMap[qEntry.Epoch] = qEntry
}

func (p *persisted) addCheckpoint(cp *pb.Checkpoint) {
	if p.checkpoints == nil {
		p.checkpoints = map[uint64]*pb.Checkpoint{}
	}

	p.checkpoints[cp.SeqNo] = cp
}

func (p *persisted) setLastCommitted(seqNo uint64) {
	if p.lastCommitted >= seqNo {
		panic("dev sanity test, remove me")
	}

	p.lastCommitted = seqNo
}

func (p *persisted) truncate(lowWatermark uint64) {
	for seqNo := range p.pSet {
		if seqNo < lowWatermark {
			delete(p.pSet, seqNo)
			delete(p.qSet, seqNo)
		}
	}

	for seqNo := range p.qSet {
		if seqNo < lowWatermark {
			delete(p.qSet, seqNo)
		}
	}

	for seqNo := range p.checkpoints {
		if seqNo < lowWatermark {
			delete(p.checkpoints, seqNo)
		}
	}
}

func (p *persisted) constructEpochChange(newEpoch uint64, ct *checkpointTracker) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
	}

	var highestStableCheckpoint *pb.Checkpoint
	var checkpoints []*pb.Checkpoint
	for seqNo, cp := range p.checkpoints {
		pcp := ct.checkpoint(seqNo)
		if pcp.stable && (highestStableCheckpoint == nil || highestStableCheckpoint.SeqNo < seqNo) {
			highestStableCheckpoint = cp
		} else {
			checkpoints = append(checkpoints, cp)
		}
	}
	checkpoints = append(checkpoints, highestStableCheckpoint)

	if highestStableCheckpoint == nil {
		panic("this should never happen")
	}

	// Note, this is so that our order is deterministic, across restarts
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].SeqNo < checkpoints[j].SeqNo
	})

	epochChange.Checkpoints = checkpoints

	for seqNo := highestStableCheckpoint.SeqNo; seqNo < highestStableCheckpoint.SeqNo+uint64(p.networkConfig.CheckpointInterval)*2; seqNo++ {
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
