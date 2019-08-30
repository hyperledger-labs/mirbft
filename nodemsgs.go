/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type applyable int

const (
	past applyable = iota
	current
	future
	invalid
)

// nodeMsgs buffers incoming messages from a node, and allowing them to be applied
// in order, even though links may be out of order.
type nodeMsgs struct {
	id NodeID

	epochMsgs map[EpochNo]*epochMsgs

	nextCheckpoint SeqNo
}

type epochMsgs struct {
	epochConfig *epochConfig

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[BucketID]*nextMsg

	leadsSomeBucket bool

	largestPreprepare SeqNo
}

type nextMsg struct {
	leader  bool
	prepare SeqNo // Note Prepare is Preprepare if Leader is true
	commit  SeqNo
}

func newNodeMsgs(nodeID NodeID, epochConfig *epochConfig) *nodeMsgs {
	return &nodeMsgs{
		id: nodeID,
		epochMsgs: map[EpochNo]*epochMsgs{
			0: newEpochMsgs(nodeID, epochConfig),
		},
		nextCheckpoint: epochConfig.lowWatermark + epochConfig.checkpointInterval,
	}
}

// processMsg returns a possibly empty list of in-order msgs ready to be processed from this node.
// Note, this list will either include the message passed in, or be empty.
// XXX for now, the list will be at most size 1, but, this will change in the future.
func (n *nodeMsgs) processMsg(outerMsg *pb.Msg) []*pb.Msg {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]

	// XXX this whole function is a poorly structured hack, as part of an incremental approach
	// to refactoring.  Ultimately, inspect/apply should be merged into a single call.

	var status applyable
	var msgType string
	var seqNo SeqNo
	var bucket BucketID
	var apply func()

	switch innerMsg := outerMsg.Type.(type) {
	case *pb.Msg_Preprepare:
		msg := innerMsg.Preprepare
		msgType, seqNo, bucket = "Preprepare", SeqNo(msg.SeqNo), BucketID(msg.Bucket)
		status = epochMsgs.inspectPreprepare(seqNo, bucket)
		apply = func() { epochMsgs.applyPreprepare(seqNo, bucket) }
	case *pb.Msg_Prepare:
		msg := innerMsg.Prepare
		msgType, seqNo, bucket = "Prepare", SeqNo(msg.SeqNo), BucketID(msg.Bucket)
		status = epochMsgs.inspectPrepare(SeqNo(msg.SeqNo), BucketID(msg.Bucket))
		apply = func() { epochMsgs.applyPrepare(seqNo, bucket) }
	case *pb.Msg_Commit:
		msg := innerMsg.Commit
		msgType, seqNo, bucket = "Commit", SeqNo(msg.SeqNo), BucketID(msg.Bucket)
		status = epochMsgs.inspectCommit(SeqNo(msg.SeqNo), BucketID(msg.Bucket))
		apply = func() { epochMsgs.applyCommit(seqNo, bucket) }
	case *pb.Msg_Checkpoint:
		msg := innerMsg.Checkpoint
		msgType, seqNo, bucket = "Checkpoint", SeqNo(msg.SeqNo), BucketID(0) // XXX Bucket 0 is a hack
		status = n.inspectCheckpoint(SeqNo(msg.SeqNo))
		apply = func() { n.applyCheckpoint(seqNo) }
	case *pb.Msg_Forward:
		return []*pb.Msg{outerMsg}
	default:
		// TODO mark oddity
		return nil
	}

	// These three checks belong elsewhere
	if bucket > BucketID(len(epochMsgs.epochConfig.buckets)) {
		epochMsgs.epochConfig.oddities.badBucket(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
		return nil
	}

	if seqNo < epochMsgs.epochConfig.lowWatermark {
		epochMsgs.epochConfig.oddities.belowWatermarks(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
		return nil
	}

	if seqNo > epochMsgs.epochConfig.highWatermark {
		epochMsgs.epochConfig.oddities.aboveWatermarks(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
		return nil
	}

	switch status {
	case past:
		epochMsgs.epochConfig.oddities.AlreadyProcessed(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
	case future:
		epochMsgs.epochConfig.myConfig.Logger.Debug("deferring apply as it's from the future", zap.Uint64("NodeID", uint64(n.id)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		// TODO handle this with some sort of 'unprocessed' cache, but ignoring for now
	case current:
		epochMsgs.epochConfig.myConfig.Logger.Debug("applying", zap.Uint64("NodeID", uint64(n.id)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		apply()
	default: // invalid
		epochMsgs.epochConfig.oddities.InvalidMessage(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
	}

	return []*pb.Msg{outerMsg}
}

func (n *nodeMsgs) inspectCheckpoint(seqNo SeqNo) applyable {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]

	for _, next := range epochMsgs.next {
		if next.commit < seqNo {
			return future
		}
	}

	switch {
	case n.nextCheckpoint > seqNo:
		return past
	case n.nextCheckpoint == seqNo:
		return current
	default:
		return future
	}
}

func (n *nodeMsgs) applyCheckpoint(seqNo SeqNo) {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]
	n.nextCheckpoint = seqNo + epochMsgs.epochConfig.checkpointInterval
}

func (n *nodeMsgs) moveWatermarks() {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]
	for _, next := range epochMsgs.next {
		if next.prepare < epochMsgs.epochConfig.lowWatermark {
			// TODO log warning
			next.prepare = epochMsgs.epochConfig.lowWatermark
		}

		if next.commit < epochMsgs.epochConfig.lowWatermark {
			// TODO log warning
			next.commit = epochMsgs.epochConfig.lowWatermark
		}
	}

	if n.nextCheckpoint < epochMsgs.epochConfig.lowWatermark {
		// TODO log warning
		n.nextCheckpoint = epochMsgs.epochConfig.lowWatermark
	}
}

func newEpochMsgs(nodeID NodeID, epochConfig *epochConfig) *epochMsgs {
	next := map[BucketID]*nextMsg{}
	leadsSomeBucket := false
	for bucketID, leaderID := range epochConfig.buckets {
		leadsSomeBucket = true
		next[bucketID] = &nextMsg{
			leader:  nodeID == leaderID,
			prepare: epochConfig.lowWatermark + 1,
			commit:  epochConfig.lowWatermark + 1,
		}
	}
	return &epochMsgs{
		epochConfig:     epochConfig,
		next:            next,
		leadsSomeBucket: leadsSomeBucket,
	}
}

func (n *epochMsgs) inspectPreprepare(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case !next.leader:
		return invalid
	case next.prepare > seqNo:
		return past
	case next.prepare == seqNo:
		return current
	default:
		// next.Prepare > seqNo
		return future
	}
}

func (n *epochMsgs) inspectPrepare(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case next.leader:
		return invalid
	case next.prepare > seqNo:
		return past
	case next.prepare == seqNo:
		return current
	default:
		// next.Prepare > seqNo
		return future
	}
}

func (n *epochMsgs) inspectCommit(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case next.commit > seqNo:
		return past
	case next.commit == seqNo && next.prepare > next.commit:
		return current
	default:
		return future
	}
}

// applyPreprepare returns true if a new largest preprepare was observed
func (n *epochMsgs) applyPreprepare(seqNo SeqNo, bucket BucketID) bool {
	next := n.next[bucket]
	next.prepare = seqNo + 1
	if n.largestPreprepare < seqNo {
		n.largestPreprepare = seqNo
		return true
	}
	return false
}

func (n *epochMsgs) applyPrepare(seqNo SeqNo, bucket BucketID) {
	next := n.next[bucket]
	next.prepare = seqNo + 1
}

func (n *epochMsgs) applyCommit(seqNo SeqNo, bucket BucketID) {
	next := n.next[bucket]
	next.commit = seqNo + 1
}

type NodeStatus struct {
	ID             uint64
	BucketStatuses []NodeBucketStatus
}

type NodeBucketStatus struct {
	BucketID       int
	IsLeader       bool
	LastPrepare    uint64
	LastCommit     uint64
	LastCheckpoint uint64
}

func (n *nodeMsgs) status() *NodeStatus {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]

	bucketStatuses := make([]NodeBucketStatus, len(epochMsgs.next))
	for bucketID := range bucketStatuses {
		nextMsg := epochMsgs.next[BucketID(bucketID)]
		bucketStatuses[bucketID] = NodeBucketStatus{
			BucketID:       bucketID,
			IsLeader:       nextMsg.leader,
			LastCheckpoint: uint64(n.nextCheckpoint - epochMsgs.epochConfig.checkpointInterval),
			LastPrepare:    uint64(nextMsg.prepare - 1), // No underflow is possible, we start at seq 1
			LastCommit:     uint64(nextMsg.commit - 1),
		}
	}

	return &NodeStatus{
		ID:             uint64(n.id),
		BucketStatuses: bucketStatuses,
	}
}
