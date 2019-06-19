/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type Proposer struct {
	EpochConfig *EpochConfig

	NextAssigned    SeqNo
	OwnedBuckets    []BucketID
	NextBucketIndex int

	Queue     [][]byte
	SizeBytes int
	Pending   [][][]byte
}

func NewProposer(config *EpochConfig) *Proposer {
	ownedBuckets := []BucketID{}
	for bucketID, nodeID := range config.Buckets {
		if nodeID == NodeID(config.MyConfig.ID) {
			ownedBuckets = append(ownedBuckets, bucketID)
		}
	}

	return &Proposer{
		EpochConfig:  config,
		OwnedBuckets: ownedBuckets,
		NextAssigned: config.LowWatermark + 1,
	}
}

func (p *Proposer) Propose(data []byte) *consumer.Actions {
	p.Queue = append(p.Queue, data)
	p.SizeBytes += len(data)
	if p.SizeBytes >= p.EpochConfig.MyConfig.BatchParameters.CutSizeBytes {
		p.Pending = append(p.Pending, p.Queue)
	}
	p.Queue = nil
	p.SizeBytes = 0

	return p.DrainQueue()
}

func (p *Proposer) NoopAdvance() *consumer.Actions {
	initialSeq := p.NextAssigned

	actions := p.DrainQueue() // XXX this really shouldn't ever be necessary, double check

	// Allocate an op to all buckets, if there is room, so that the seq advances
	for p.RoomToAssign() && p.NextAssigned == initialSeq {
		if len(p.Queue) > 0 {
			actions.Append(p.Advance(p.Queue))
			p.Queue = nil
			continue
		}

		actions.Append(p.Advance(nil))
	}

	return actions
}

func (p *Proposer) DrainQueue() *consumer.Actions {
	actions := &consumer.Actions{}

	for p.RoomToAssign() && len(p.Pending) > 0 {
		actions.Append(p.Advance(p.Pending[0]))
		p.Pending = p.Pending[1:]
	}

	return actions
}

func (p *Proposer) RoomToAssign() bool {
	// We leave one empty checkpoint interval within the watermarks to avoid messages being dropped when
	// from the first nodes to move watermarks.
	// XXX, the constant '4' garbage checkpoints in epoch.go is tied to the constant '5' free checkpoints
	// defined here and assumes the network is configured for 10 total checkpoints, but not enforced
	return p.NextAssigned <= p.EpochConfig.HighWatermark-5*p.EpochConfig.CheckpointInterval
}

func (p *Proposer) Advance(batch [][]byte) *consumer.Actions {
	actions := &consumer.Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Preprepare{
					Preprepare: &pb.Preprepare{
						Epoch:  p.EpochConfig.Number,
						SeqNo:  uint64(p.NextAssigned),
						Batch:  batch,
						Bucket: uint64(p.OwnedBuckets[p.NextBucketIndex]),
					},
				},
			},
		},
	}

	p.NextBucketIndex = (p.NextBucketIndex + 1) % len(p.OwnedBuckets)
	if p.NextBucketIndex == 0 {
		p.NextAssigned++
	}

	return actions
}
