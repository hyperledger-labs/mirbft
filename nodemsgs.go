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
	id             NodeID
	oddities       *oddities
	buffer         map[*pb.Msg]struct{} // TODO, this could be much better optimized via a ring buffer
	epochConfig    *epochConfig
	epochMsgs      *epochMsgs
	nextCheckpoint uint64
}

type epochMsgs struct {
	myConfig    *Config
	epochConfig *epochConfig

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[BucketID]*nextMsg
}

type nextMsg struct {
	leader  bool
	prepare uint64 // Note Prepare is Preprepare if Leader is true
	commit  uint64
}

func newNodeMsgs(nodeID NodeID, epochConfig *epochConfig, myConfig *Config, oddities *oddities) *nodeMsgs {
	em := newEpochMsgs(nodeID, epochConfig, myConfig)
	return &nodeMsgs{
		id:        nodeID,
		oddities:  oddities,
		epochMsgs: em,

		epochConfig: epochConfig,

		// nextCheckpoint: epochConfig.lowWatermark + epochConfig.checkpointInterval,
		// XXX we should initialize this properly, sort of like the above
		nextCheckpoint: uint64(epochConfig.networkConfig.CheckpointInterval) / uint64(len(epochConfig.buckets)),
		buffer:         map[*pb.Msg]struct{}{},
	}
}

// ingest the message for management by the nodeMsgs.  This message
// may immediately become available to read from next(), or it may be enqueued
// for future consumption
func (n *nodeMsgs) ingest(outerMsg *pb.Msg) {
	n.buffer[outerMsg] = struct{}{}
}

func (n *nodeMsgs) process(outerMsg *pb.Msg) applyable {
	var epoch uint64
	switch innerMsg := outerMsg.Type.(type) {
	case *pb.Msg_Preprepare:
		epoch = innerMsg.Preprepare.Epoch
	case *pb.Msg_Prepare:
		epoch = innerMsg.Prepare.Epoch
	case *pb.Msg_Commit:
		epoch = innerMsg.Commit.Epoch
	case *pb.Msg_Suspect:
		epoch = innerMsg.Suspect.Epoch
	case *pb.Msg_Checkpoint:
		return n.processCheckpoint(innerMsg.Checkpoint)
	case *pb.Msg_Forward:
		epoch = innerMsg.Forward.Epoch
	case *pb.Msg_EpochChange:
		epoch = innerMsg.EpochChange.NewEpoch
	case *pb.Msg_NewEpoch:
		return current // TODO, decide if this is actually current
	case *pb.Msg_NewEpochEcho:
		return current // TODO, decide if this is actually current
	case *pb.Msg_NewEpochReady:
		return current // TODO, decide if this is actually current
	default:
		n.oddities.invalidMessage(n.id, outerMsg)
		// TODO don't panic here, just return, left here for dev
		// as only a byzantine node with custom protos gets us here.
		panic("unknown message type")
	}

	switch {
	case n.epochMsgs.epochConfig.number < epoch:
		return past
	case n.epochMsgs.epochConfig.number > epoch:
		return future
	}

	// current

	return n.epochMsgs.process(outerMsg)
}

func (n *nodeMsgs) next() *pb.Msg {
	for msg := range n.buffer {
		switch n.process(msg) {
		case past:
			n.oddities.alreadyProcessed(n.id, msg)
			delete(n.buffer, msg)
		case current:
			delete(n.buffer, msg)
			return msg
		case future:
			n.epochMsgs.myConfig.Logger.Debug("deferring apply as it's from the future", zap.Uint64("NodeID", uint64(n.id)))
		}
	}

	return nil
}

func (n *nodeMsgs) processCheckpoint(msg *pb.Checkpoint) applyable {
	switch {
	case n.nextCheckpoint > n.epochConfig.seqToColumn(msg.SeqNo):
		return past
	case n.nextCheckpoint == n.epochConfig.seqToColumn(msg.SeqNo):
		for _, next := range n.epochMsgs.next {
			if next.commit < n.epochConfig.seqToColumn(msg.SeqNo) {
				return future
			}
		}

		n.nextCheckpoint = n.epochConfig.seqToColumn(msg.SeqNo) + uint64(n.epochConfig.networkConfig.CheckpointInterval)/uint64(len(n.epochConfig.buckets))
		return current
	default:
		return future
	}
}

func newEpochMsgs(nodeID NodeID, epochConfig *epochConfig, myConfig *Config) *epochMsgs {
	next := map[BucketID]*nextMsg{}
	for bucketID, leaderID := range epochConfig.buckets {
		next[bucketID] = &nextMsg{
			leader: nodeID == leaderID,
			// prepare: epochConfig.lowWatermark + 1,
			// commit:  epochConfig.lowWatermark + 1,
			// XXX initialize these properly, sort of like the above
			prepare: 1,
			commit:  1,
		}
	}
	return &epochMsgs{
		myConfig:    myConfig,
		epochConfig: epochConfig,
		next:        next,
	}
}

func (n *epochMsgs) process(outerMsg *pb.Msg) applyable {
	switch innerMsg := outerMsg.Type.(type) {
	case *pb.Msg_Preprepare:
		return n.processPreprepare(innerMsg.Preprepare)
	case *pb.Msg_Prepare:
		return n.processPrepare(innerMsg.Prepare)
	case *pb.Msg_Commit:
		return n.processCommit(innerMsg.Commit)
	case *pb.Msg_Forward:
		return current
	case *pb.Msg_Suspect:
		// TODO, do we care about duplicates?
		return current
	case *pb.Msg_EpochChange:
		// TODO, filter
		return current
	}

	panic("programming error, unreachable")
}

func (n *epochMsgs) processPreprepare(msg *pb.Preprepare) applyable {
	next, ok := n.next[n.epochConfig.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	switch {
	case !next.leader:
		return invalid
	case next.prepare > n.epochConfig.seqToColumn(msg.SeqNo):
		return past
	case next.prepare == n.epochConfig.seqToColumn(msg.SeqNo):
		next.prepare++
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processPrepare(msg *pb.Prepare) applyable {
	next, ok := n.next[n.epochConfig.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	switch {
	case next.leader:
		return invalid
	case next.prepare > n.epochConfig.seqToColumn(msg.SeqNo):
		return past
	case next.prepare == n.epochConfig.seqToColumn(msg.SeqNo):
		next.prepare++
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processCommit(msg *pb.Commit) applyable {
	next, ok := n.next[n.epochConfig.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	switch {
	case next.commit > n.epochConfig.seqToColumn(msg.SeqNo):
		return past
	case next.commit == n.epochConfig.seqToColumn(msg.SeqNo) && next.prepare > next.commit:
		next.commit++
		return current
	default:
		return future
	}
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
	bucketStatuses := make([]NodeBucketStatus, len(n.epochMsgs.next))
	for bucketID := range bucketStatuses {
		nextMsg := n.epochMsgs.next[BucketID(bucketID)]
		bucketStatuses[bucketID] = NodeBucketStatus{
			BucketID:       bucketID,
			IsLeader:       nextMsg.leader,
			LastCheckpoint: uint64(n.nextCheckpoint) - uint64(n.epochMsgs.epochConfig.networkConfig.CheckpointInterval),
			LastPrepare:    uint64(nextMsg.prepare - 1), // No underflow is possible, we start at seq 1
			LastCommit:     uint64(nextMsg.commit - 1),
		}
	}

	return &NodeStatus{
		ID:             uint64(n.id),
		BucketStatuses: bucketStatuses,
	}
}
