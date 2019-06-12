/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

type Applyable int

const (
	Invalid Applyable = iota
	Past
	Current
	Future
)

// Node tracks the expect next message to apply on a per bucket basis
type Node struct {
	EpochConfig *EpochConfig

	// Next maintains the info about the next expected messages for
	// a particular bucket.
	Next map[BucketID]*NextMsg

	NextCheckpoint SeqNo
}

type NextMsg struct {
	Leader  bool
	Prepare SeqNo // Note Prepare is Preprepare if Leader is true
	Commit  SeqNo
}

func NewNode(nodeID NodeID, epochConfig *EpochConfig) *Node {
	next := map[BucketID]*NextMsg{}
	for bucketID, leaderID := range epochConfig.Buckets {
		next[bucketID] = &NextMsg{
			Leader:  nodeID == leaderID,
			Prepare: epochConfig.LowWatermark,
			Commit:  epochConfig.LowWatermark,
		}
	}
	return &Node{
		EpochConfig:    epochConfig,
		Next:           next,
		NextCheckpoint: epochConfig.LowWatermark + epochConfig.CheckpointInterval,
	}
}

func (n *Node) InspectPreprepare(seqNo SeqNo, bucket BucketID) Applyable {
	next := n.Next[bucket]
	switch {
	case !next.Leader:
		return Invalid
	case next.Prepare > seqNo:
		return Past
	case next.Prepare == seqNo:
		return Current
	default:
		// next.Prepare > seqNo
		return Future
	}
}

func (n *Node) InspectPrepare(seqNo SeqNo, bucket BucketID) Applyable {
	next := n.Next[bucket]
	switch {
	case next.Leader:
		return Invalid
	case next.Prepare > seqNo:
		return Past
	case next.Prepare == seqNo:
		return Current
	default:
		// next.Prepare > seqNo
		return Future
	}
}

func (n *Node) InspectCommit(seqNo SeqNo, bucket BucketID) Applyable {
	next := n.Next[bucket]
	switch {
	case next.Commit > seqNo:
		return Past
	case next.Commit == seqNo && next.Prepare > next.Commit:
		return Current
	default:
		return Future
	}
}

func (n *Node) InspectCheckpoint(seqNo SeqNo) Applyable {
	for _, next := range n.Next {
		if next.Commit < seqNo {
			return Future
		}
	}

	switch {
	case n.NextCheckpoint > seqNo:
		return Past
	case n.NextCheckpoint == seqNo:
		return Current
	default:
		return Future
	}
}

func (n *Node) ApplyPreprepare(seqNo SeqNo, bucket BucketID) {
	next := n.Next[bucket]
	next.Prepare = seqNo + 1
}

func (n *Node) ApplyPrepare(seqNo SeqNo, bucket BucketID) {
	next := n.Next[bucket]
	next.Prepare = seqNo + 1
}

func (n *Node) ApplyCommit(seqNo SeqNo, bucket BucketID) {
	next := n.Next[bucket]
	next.Commit = seqNo + 1
}

func (n *Node) ApplyCheckpoint(seqNo SeqNo) {
	n.NextCheckpoint = seqNo + n.EpochConfig.CheckpointInterval
}

func (n *Node) MoveWatermarks() {
	for _, next := range n.Next {
		if next.Prepare < n.EpochConfig.LowWatermark {
			// TODO log warning
			next.Prepare = n.EpochConfig.LowWatermark
		}

		if next.Commit < n.EpochConfig.LowWatermark {
			// TODO log warning
			next.Commit = n.EpochConfig.LowWatermark
		}
	}

	if n.NextCheckpoint < n.EpochConfig.LowWatermark {
		// TODO log warning
		n.NextCheckpoint = n.EpochConfig.LowWatermark
	}
}

type NodeStatus struct {
	LastCheckpoint SeqNo
	Messages       map[BucketID]NextMsg
}

func (n *Node) Status() *NodeStatus {
	messages := map[BucketID]NextMsg{}
	for bucketID, nextMsg := range n.Next {
		messages[bucketID] = *nextMsg
	}

	return &NodeStatus{
		LastCheckpoint: n.NextCheckpoint - n.EpochConfig.CheckpointInterval,
		Messages:       messages,
	}
}
