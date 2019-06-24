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
	// We leave one empty checkpoint interval within the watermarks to avoid messages being dropped when
	// from the first nodes to move watermarks.
	// XXX, the constant '4' garbage checkpoints in epoch.go is tied to the constant '5' free checkpoints
	// defined here and assumes the network is configured for 10 total checkpoints, but not enforced
	return p.nextAssigned <= p.epochConfig.highWatermark-5*p.epochConfig.checkpointInterval
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
