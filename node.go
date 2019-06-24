/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type Applyable int

const (
	Invalid Applyable = iota
	Past
	Current
	Future
)

// Node tracks the expect next message to apply on a per bucket basis
type NodeMsgs struct {
	ID NodeID

	EpochConfig *EpochConfig

	// Next maintains the info about the next expected messages for
	// a particular bucket.
	Next map[BucketID]*NextMsg

	NextCheckpoint SeqNo

	LeadsSomeBucket bool

	LargestPreprepare SeqNo
}

type NextMsg struct {
	Leader  bool
	Prepare SeqNo // Note Prepare is Preprepare if Leader is true
	Commit  SeqNo
}

func NewNodeMsgs(nodeID NodeID, epochConfig *EpochConfig) *NodeMsgs {
	next := map[BucketID]*NextMsg{}
	leadsSomeBucket := false
	for bucketID, leaderID := range epochConfig.Buckets {
		leadsSomeBucket = true
		next[bucketID] = &NextMsg{
			Leader:  nodeID == leaderID,
			Prepare: epochConfig.LowWatermark + 1,
			Commit:  epochConfig.LowWatermark + 1,
		}
	}
	return &NodeMsgs{
		ID:              nodeID,
		EpochConfig:     epochConfig,
		Next:            next,
		NextCheckpoint:  epochConfig.LowWatermark + epochConfig.CheckpointInterval,
		LeadsSomeBucket: leadsSomeBucket,
	}
}

func (n *NodeMsgs) InspectPreprepare(seqNo SeqNo, bucket BucketID) Applyable {
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

func (n *NodeMsgs) InspectPrepare(seqNo SeqNo, bucket BucketID) Applyable {
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

func (n *NodeMsgs) InspectCommit(seqNo SeqNo, bucket BucketID) Applyable {
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

func (n *NodeMsgs) InspectCheckpoint(seqNo SeqNo) Applyable {
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

// ApplyPreprepare returns true if a new largest preprepare was observed
func (n *NodeMsgs) ApplyPreprepare(seqNo SeqNo, bucket BucketID) bool {
	next := n.Next[bucket]
	next.Prepare = seqNo + 1
	if n.LargestPreprepare < seqNo {
		n.LargestPreprepare = seqNo
		return true
	}
	return false
}

func (n *NodeMsgs) ApplyPrepare(seqNo SeqNo, bucket BucketID) {
	next := n.Next[bucket]
	next.Prepare = seqNo + 1
}

func (n *NodeMsgs) ApplyCommit(seqNo SeqNo, bucket BucketID) {
	next := n.Next[bucket]
	next.Commit = seqNo + 1
}

func (n *NodeMsgs) ApplyCheckpoint(seqNo SeqNo) {
	n.NextCheckpoint = seqNo + n.EpochConfig.CheckpointInterval
}

func (n *NodeMsgs) MoveWatermarks() {
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

func (n *NodeMsgs) Status() *NodeStatus {
	bucketStatuses := make([]NodeBucketStatus, len(n.Next))
	for bucketID := range bucketStatuses {
		nextMsg := n.Next[BucketID(bucketID)]
		bucketStatuses[bucketID] = NodeBucketStatus{
			BucketID:       bucketID,
			IsLeader:       nextMsg.Leader,
			LastCheckpoint: uint64(n.NextCheckpoint - n.EpochConfig.CheckpointInterval),
			LastPrepare:    uint64(nextMsg.Prepare - 1), // XXX ignoring the underflow, need to re-index at 1 anyway
			LastCommit:     uint64(nextMsg.Commit - 1),
		}
	}

	return &NodeStatus{
		ID:             uint64(n.ID),
		BucketStatuses: bucketStatuses,
	}
}
