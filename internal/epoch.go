/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

// EpochConfig is the information required by the various
// state machines whose state is scoped to an epoch
type EpochConfig struct {
	// MyConfig is the configuration specific to this node
	MyConfig *consumer.Config

	// Oddities stores counts of suspicious acitivities and logs them.
	Oddities *Oddities

	// Number is the epoch number this config applies to
	Number uint64

	// HighWatermark is the current maximum seqno that may be processed
	HighWatermark SeqNo

	// LowWatermark is the current minimum seqno for which messages are valid
	LowWatermark SeqNo

	// F is the total number of faults tolerated by the network
	F int

	// CheckpointInterval is the number of sequence numbers to commit before broadcasting a checkpoint
	CheckpointInterval SeqNo

	// Nodes is all the node ids in the network
	Nodes []NodeID

	// Buckets is a map from bucket ID to leader ID
	Buckets map[BucketID]NodeID
}

type Epoch struct {
	EpochConfig *EpochConfig

	Nodes map[NodeID]*Node

	Buckets map[BucketID]*Bucket

	CheckpointWindows map[SeqNo]*CheckpointWindow
}

func NewEpoch(config *EpochConfig) *Epoch {
	nodes := map[NodeID]*Node{}
	for _, id := range config.Nodes {
		nodes[id] = NewNode(id, config)
	}

	buckets := map[BucketID]*Bucket{}
	for bucketID := range config.Buckets {
		buckets[bucketID] = NewBucket(config, bucketID)
	}

	checkpointWindows := map[SeqNo]*CheckpointWindow{}
	for seqNo := config.LowWatermark + config.CheckpointInterval; seqNo <= config.HighWatermark; seqNo += config.CheckpointInterval {
		checkpointWindows[seqNo] = NewCheckpointWindow(seqNo, config)
	}

	return &Epoch{
		EpochConfig:       config,
		Nodes:             nodes,
		Buckets:           buckets,
		CheckpointWindows: checkpointWindows,
	}
}

func (e *Epoch) ValidateMsg(
	source NodeID,
	seqNo SeqNo,
	bucket BucketID,
	msgType string,
	inspect func(node *Node) Applyable,
	apply func(node *Node) *consumer.Actions,
) *consumer.Actions {
	if bucket > BucketID(len(e.EpochConfig.Buckets)) {
		e.EpochConfig.Oddities.BadBucket(e.EpochConfig, msgType, source, seqNo, bucket)
		return &consumer.Actions{}
	}

	if seqNo < e.EpochConfig.LowWatermark {
		e.EpochConfig.Oddities.BelowWatermarks(e.EpochConfig, msgType, source, seqNo, bucket)
		return &consumer.Actions{}
	}

	if seqNo > e.EpochConfig.HighWatermark {
		e.EpochConfig.Oddities.AboveWatermarks(e.EpochConfig, msgType, source, seqNo, bucket)
		return &consumer.Actions{}
	}

	node, ok := e.Nodes[source]
	if !ok {
		e.EpochConfig.MyConfig.Logger.Panic("unknown node")
		// TODO perhaps handle this a bit more gracefully? We should never get a message for a node
		// not defined in this epoch, but at the time of this writing, it's not clear whether
		// a subtle bug could cause this or whether this is an obvious panic situation
	}

	switch inspect(node) {
	case Past:
		e.EpochConfig.Oddities.AlreadyProcessed(e.EpochConfig, msgType, source, seqNo, bucket)
	case Future:
		e.EpochConfig.MyConfig.Logger.Debug("deferring apply as it's from the future", zap.Uint64("NodeID", uint64(source)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		// TODO handle this with some sort of 'unprocessed' cache, but ignoring for now
	case Current:
		e.EpochConfig.MyConfig.Logger.Debug("applying", zap.Uint64("NodeID", uint64(source)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		return apply(node)
	default: // Invalid
		e.EpochConfig.Oddities.InvalidMessage(e.EpochConfig, msgType, source, seqNo, bucket)
	}
	return &consumer.Actions{}
}

func (e *Epoch) Process(preprocessResult consumer.PreprocessResult) *consumer.Actions {
	bucketID := BucketID(preprocessResult.Cup % uint64(len(e.EpochConfig.Buckets)))
	nodeID := e.EpochConfig.Buckets[bucketID]
	if nodeID == NodeID(e.EpochConfig.MyConfig.ID) {
		return e.Buckets[bucketID].Propose(preprocessResult.Proposal.Data)
	}

	if preprocessResult.Proposal.Source == e.EpochConfig.MyConfig.ID {
		// I originated this proposal, but someone else leads this bucket,
		// forward the message to them
		return &consumer.Actions{
			Unicast: []consumer.Unicast{
				{
					Target: uint64(nodeID),
					Msg: &pb.Msg{
						Type: &pb.Msg_Forward{
							Forward: &pb.Forward{
								Epoch:  e.EpochConfig.Number,
								Bucket: uint64(bucketID),
								Data:   preprocessResult.Proposal.Data,
							},
						},
					},
				},
			},
		}
	}

	// Someone forwarded me this proposal, but I'm not responsible for it's bucket
	// TODO, log oddity? Assign it to the wrong bucket? Forward it again?
	return &consumer.Actions{}
}

func (e *Epoch) Preprepare(source NodeID, seqNo SeqNo, bucket BucketID, batch [][]byte) *consumer.Actions {
	return e.ValidateMsg(
		source, seqNo, bucket, "Preprepare",
		func(node *Node) Applyable { return node.InspectPreprepare(seqNo, bucket) },
		func(node *Node) *consumer.Actions {
			node.ApplyPreprepare(seqNo, bucket)
			return e.Buckets[bucket].ApplyPreprepare(seqNo, batch)
		},
	)
}

func (e *Epoch) Prepare(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *consumer.Actions {
	return e.ValidateMsg(
		source, seqNo, bucket, "Prepare",
		func(node *Node) Applyable { return node.InspectPrepare(seqNo, bucket) },
		func(node *Node) *consumer.Actions {
			node.ApplyPrepare(seqNo, bucket)
			return e.Buckets[bucket].ApplyPrepare(source, seqNo, digest)
		},
	)
}

func (e *Epoch) Commit(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *consumer.Actions {
	return e.ValidateMsg(
		source, seqNo, bucket, "Commit",
		func(node *Node) Applyable { return node.InspectCommit(seqNo, bucket) },
		func(node *Node) *consumer.Actions {
			node.ApplyCommit(seqNo, bucket)
			actions := e.Buckets[bucket].ApplyCommit(source, seqNo, digest)
			if len(actions.Commit) > 0 {
				// XXX this is a moderately hacky way to determine if this commit msg triggered
				// a commit, is there a better way?
				if _, ok := e.CheckpointWindows[seqNo]; ok {
					actions.Append(&consumer.Actions{
						Checkpoint: []uint64{uint64(seqNo)},
					})
				}
			}
			return actions
		},
	)
}

func (e *Epoch) Checkpoint(source NodeID, seqNo SeqNo, value, attestation []byte) *consumer.Actions {
	return e.ValidateMsg(
		source, seqNo, 0, "Commit", // XXX using bucket '0' for checkpoints is a bit of a hack, as it has no bucket
		func(node *Node) Applyable { return node.InspectCheckpoint(seqNo) },
		func(node *Node) *consumer.Actions {
			node.ApplyCheckpoint(seqNo)
			return e.CheckpointWindows[seqNo].ApplyCheckpointMsg(source, value, attestation)
		},
	)
}

func (e *Epoch) CheckpointResult(seqNo SeqNo, value, attestation []byte) *consumer.Actions {
	checkpointWindow, ok := e.CheckpointWindows[seqNo]
	if !ok {
		panic("received an unexpected checkpoint result")
	}
	return checkpointWindow.ApplyCheckpointResult(value, attestation)
}

func (e *Epoch) Digest(seqNo SeqNo, bucket BucketID, digest []byte) *consumer.Actions {
	return e.ValidateMsg(
		NodeID(e.EpochConfig.MyConfig.ID), seqNo, bucket, "Digest",
		func(node *Node) Applyable { return Current },
		func(node *Node) *consumer.Actions {
			return e.Buckets[bucket].ApplyDigestResult(seqNo, digest)
		},
	)
}

func (e *Epoch) Validate(seqNo SeqNo, bucket BucketID, valid bool) *consumer.Actions {
	return e.ValidateMsg(
		NodeID(e.EpochConfig.MyConfig.ID), seqNo, bucket, "Validate",
		func(node *Node) Applyable { return Current },
		func(node *Node) *consumer.Actions {
			return e.Buckets[bucket].ApplyValidateResult(seqNo, valid)
		},
	)
}
