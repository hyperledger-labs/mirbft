/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

type proposer struct {
	epochConfig *epochConfig

	nextAssigned    SeqNo
	ownedBuckets    []BucketID
	nextBucketIndex int

	queue     [][]byte
	sizeBytes int
	pending   [][][]byte
}

func newProposer(config *epochConfig) *proposer {
	ownedBuckets := []BucketID{}
	for bucketID, nodeID := range config.buckets {
		if nodeID == NodeID(config.myConfig.ID) {
			ownedBuckets = append(ownedBuckets, bucketID)
		}
	}

	return &proposer{
		epochConfig:  config,
		ownedBuckets: ownedBuckets,
		nextAssigned: config.lowWatermark + 1,
	}
}

func (p *proposer) propose(data []byte) *Actions {
	p.queue = append(p.queue, data)
	p.sizeBytes += len(data)
	if p.sizeBytes >= p.epochConfig.myConfig.BatchParameters.CutSizeBytes {
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
	// TODO, this is a bit of an odd hardcoded check.  It assumes a total of
	// 4 checkpoint windows in play. An oldest to avoid "out of watermarks" errors on slow
	// nodes, a newest to avoid "out of watermarks" on fast nodes, and two middle ones which
	// are intended to be actually active.
	return p.nextAssigned <= p.epochConfig.highWatermark-2*p.epochConfig.checkpointInterval
}

func (p *proposer) advance(batch [][]byte) *Actions {
	actions := &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Preprepare{
					Preprepare: &pb.Preprepare{
						Epoch:  p.epochConfig.number,
						SeqNo:  uint64(p.nextAssigned),
						Batch:  batch,
						Bucket: uint64(p.ownedBuckets[p.nextBucketIndex]),
					},
				},
			},
		},
	}

	p.nextBucketIndex = (p.nextBucketIndex + 1) % len(p.ownedBuckets)
	if p.nextBucketIndex == 0 {
		p.nextAssigned++
	}

	return actions
}
