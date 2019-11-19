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

	nextMsg *pb.Msg

	buffer []*pb.Msg

	epochMsgs map[EpochNo]*epochMsgs

	nextCheckpoint SeqNo
}

type epochMsgs struct {
	epochConfig *epochConfig

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[BucketID]*nextMsg
}

type nextMsg struct {
	leader  bool
	prepare SeqNo // Note Prepare is Preprepare if Leader is true
	commit  SeqNo
}

func newNodeMsgs(nodeID NodeID, epochConfig *epochConfig) *nodeMsgs {
	em := newEpochMsgs(nodeID, epochConfig)
	return &nodeMsgs{
		id: nodeID,
		epochMsgs: map[EpochNo]*epochMsgs{
			0:                           em, // TODO remove this dirty hack
			EpochNo(epochConfig.number): em,
		},
		nextCheckpoint: epochConfig.lowWatermark + epochConfig.checkpointInterval,
	}
}

// ingest the message for management by the nodeMsgs.  This message
// may immediately become available to read from next(), or it may be enqueued
// for fufture consumption
func (n *nodeMsgs) ingest(outerMsg *pb.Msg) {
	n.buffer = append(n.buffer, outerMsg)
}

func (n *nodeMsgs) next() *pb.Msg {
	// TODO handle copying the buffer for still pending stuff
	if len(n.buffer) > 1 {
		panic("did not expect more than one message to be ingested before processing")
	}

	defer func() {
		n.buffer = nil
	}()

	for _, outerMsg := range n.buffer {

		var status applyable

		// TODO, prune based on epoch number

		switch innerMsg := outerMsg.Type.(type) {
		case *pb.Msg_Preprepare:
			msg := innerMsg.Preprepare
			em, ok := n.epochMsgs[EpochNo(msg.Epoch)]
			if !ok {
				panic("here")

				continue
			}
			status = em.processPreprepare(msg)
		case *pb.Msg_Prepare:
			msg := innerMsg.Prepare
			em, ok := n.epochMsgs[EpochNo(msg.Epoch)]
			if !ok {
				continue
			}
			status = em.processPrepare(msg)
		case *pb.Msg_Commit:
			msg := innerMsg.Commit
			em, ok := n.epochMsgs[EpochNo(msg.Epoch)]
			if !ok {
				continue
			}
			status = em.processCommit(msg)
		case *pb.Msg_Checkpoint:
			status = n.processCheckpoint(innerMsg.Checkpoint)
		case *pb.Msg_Forward:
			status = current
		default:
			// TODO log oddity
			status = invalid
		}

		switch status {
		case past:
			//epochMsgs.epochConfig.oddities.AlreadyProcessed(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
			n.epochMsgs[0].epochConfig.myConfig.Logger.Debug("skipping apply as it's from the past", zap.Uint64("NodeID", uint64(n.id)))
		case future:
			n.epochMsgs[0].epochConfig.myConfig.Logger.Debug("deferring apply as it's from the future", zap.Uint64("NodeID", uint64(n.id)))
		case current:
			return outerMsg
		default: // invalid
			//n.epochMsgs[0].epochConfig.oddities.InvalidMessage(epochMsgs.epochConfig, msgType, n.id, seqNo, bucket)
			n.epochMsgs[0].epochConfig.myConfig.Logger.Debug("skipping apply as it's invalid", zap.Uint64("NodeID", uint64(n.id)))
		}
	}

	return nil
}

func (n *nodeMsgs) processCheckpoint(msg *pb.Checkpoint) applyable {
	// XXX we are completely ignoring epoch for the moment
	epochMsgs := n.epochMsgs[0]

	for _, next := range epochMsgs.next {
		if next.commit < SeqNo(msg.SeqNo) {
			return future
		}
	}

	switch {
	case n.nextCheckpoint > SeqNo(msg.SeqNo):
		return past
	case n.nextCheckpoint == SeqNo(msg.SeqNo):
		n.nextCheckpoint = SeqNo(msg.SeqNo) + epochMsgs.epochConfig.checkpointInterval
		return current
	default:
		return future
	}
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
	for bucketID, leaderID := range epochConfig.buckets {
		next[bucketID] = &nextMsg{
			leader:  nodeID == leaderID,
			prepare: epochConfig.lowWatermark + 1,
			commit:  epochConfig.lowWatermark + 1,
		}
	}
	return &epochMsgs{
		epochConfig: epochConfig,
		next:        next,
	}
}

func (n *epochMsgs) processPreprepare(msg *pb.Preprepare) applyable {
	next, ok := n.next[BucketID(msg.Bucket)]
	if !ok {
		return invalid
	}

	switch {
	case !next.leader:
		return invalid
	case next.prepare > SeqNo(msg.SeqNo):
		return past
	case next.prepare == SeqNo(msg.SeqNo):
		next.prepare = SeqNo(msg.SeqNo) + 1
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processPrepare(msg *pb.Prepare) applyable {
	next, ok := n.next[BucketID(msg.Bucket)]
	if !ok {
		return invalid
	}

	switch {
	case next.leader:
		return invalid
	case next.prepare > SeqNo(msg.SeqNo):
		return past
	case next.prepare == SeqNo(msg.SeqNo):
		next.prepare = SeqNo(msg.SeqNo) + 1
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processCommit(msg *pb.Commit) applyable {
	next, ok := n.next[BucketID(msg.Bucket)]
	if !ok {
		return invalid
	}

	switch {
	case next.commit > SeqNo(msg.SeqNo):
		return past
	case next.commit == SeqNo(msg.SeqNo) && next.prepare > next.commit:
		next.commit = SeqNo(msg.SeqNo) + 1
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
