/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

type proposer struct {
	myConfig      *Config
	epochConfig   *epochConfig
	maxAssignable uint64

	nextAssigned    uint64
	ownedBuckets    []BucketID
	nextBucketIndex int

	queue     [][]byte
	sizeBytes int
	pending   [][][]byte
}

func newProposer(config *epochConfig, myConfig *Config) *proposer {
	ownedBuckets := []BucketID{}
	for bucketID, nodeID := range config.buckets {
		if nodeID == NodeID(myConfig.ID) {
			ownedBuckets = append(ownedBuckets, bucketID)
		}
	}

	// XXX we don't handle owning more than one bucket (or zero)

	return &proposer{
		myConfig:     myConfig,
		epochConfig:  config,
		ownedBuckets: ownedBuckets,
		// nextAssigned: config.lowWatermark + 1,
		// XXX initialize this properly, sort of like the above
		nextAssigned: 1 + uint64(ownedBuckets[0]),
	}
}

func (p *proposer) propose(data []byte) *Actions {
	p.queue = append(p.queue, data)
	p.sizeBytes += len(data)
	if p.sizeBytes >= p.myConfig.BatchParameters.CutSizeBytes {
		p.pending = append(p.pending, p.queue)
	}
	p.queue = nil
	p.sizeBytes = 0

	return p.drainQueue()
}

func (p *proposer) noopAdvance() *Actions {
	initialSeq := p.nextAssigned

	actions := p.drainQueue() // XXX this really shouldn't ever be necessary, double check

	// Allocate an op to all buckets, if there is room, so that the seq advances
	for p.roomToAssign() && p.nextAssigned == initialSeq {
		if len(p.queue) > 0 {
			actions.Append(p.advance(p.queue))
			p.queue = nil
			continue
		}

		actions.Append(p.advance(nil))
	}

	return actions
}

func (p *proposer) drainQueue() *Actions {
	actions := &Actions{}

	for p.roomToAssign() && len(p.pending) > 0 {
		actions.Append(p.advance(p.pending[0]))
		p.pending = p.pending[1:]
	}

	return actions
}

func (p *proposer) roomToAssign() bool {
	// TODO, this is a bit of an odd hardcoded check.  And should be removed.
	return p.nextAssigned <= p.maxAssignable
}

func (p *proposer) advance(batch [][]byte) *Actions {
	actions := &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Preprepare{
					Preprepare: &pb.Preprepare{
						Epoch: p.epochConfig.number,
						SeqNo: p.nextAssigned,
						Batch: batch,
					},
				},
			},
		},
	}

	if p.nextBucketIndex == 0 {
		p.nextAssigned = p.nextAssigned + uint64(len(p.epochConfig.buckets))
	}

	return actions
}
