/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

type persisted struct {
	pSet          map[uint64]*pb.PEntry            // Seq -> PEntry
	qSet          map[uint64]map[uint64]*pb.QEntry // Seq -> Epoch -> QEntry
	checkpoints   map[uint64]*pb.Checkpoint        // Seq -> Checkpoint
	lastCommitted uint64                           // Seq
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
