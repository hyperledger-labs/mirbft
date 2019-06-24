/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type applyable int

const (
	Invalid applyable = iota
	Past
	Current
	Future
)

// Node tracks the expect next message to apply on a per bucket basis
type nodeMsgs struct {
	id NodeID

	epochConfig *epochConfig

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[BucketID]*nextMsg

	nextCheckpoint SeqNo

	leadsSomeBucket bool

	largestPreprepare SeqNo
}

type nextMsg struct {
	leader  bool
	prepare SeqNo // Note Prepare is Preprepare if Leader is true
	commit  SeqNo
}

func newNodeMsgs(nodeID NodeID, epochConfig *epochConfig) *nodeMsgs {
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
	return &nodeMsgs{
		id:              nodeID,
		epochConfig:     epochConfig,
		next:            next,
		nextCheckpoint:  epochConfig.lowWatermark + epochConfig.checkpointInterval,
		leadsSomeBucket: leadsSomeBucket,
	}
}

func (n *nodeMsgs) inspectPreprepare(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case !next.leader:
		return Invalid
	case next.prepare > seqNo:
		return Past
	case next.prepare == seqNo:
		return Current
	default:
		// next.Prepare > seqNo
		return Future
	}
}

func (n *nodeMsgs) inspectPrepare(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case next.leader:
		return Invalid
	case next.prepare > seqNo:
		return Past
	case next.prepare == seqNo:
		return Current
	default:
		// next.Prepare > seqNo
		return Future
	}
}

func (n *nodeMsgs) inspectCommit(seqNo SeqNo, bucket BucketID) applyable {
	next := n.next[bucket]
	switch {
	case next.commit > seqNo:
		return Past
	case next.commit == seqNo && next.prepare > next.commit:
		return Current
	default:
		return Future
	}
}

func (n *nodeMsgs) inspectCheckpoint(seqNo SeqNo) applyable {
	for _, next := range n.next {
		if next.commit < seqNo {
			return Future
		}
	}

	switch {
	case n.nextCheckpoint > seqNo:
		return Past
	case n.nextCheckpoint == seqNo:
		return Current
	default:
		return Future
	}
}

// applyPreprepare returns true if a new largest preprepare was observed
func (n *nodeMsgs) applyPreprepare(seqNo SeqNo, bucket BucketID) bool {
	next := n.next[bucket]
	next.prepare = seqNo + 1
	if n.largestPreprepare < seqNo {
		n.largestPreprepare = seqNo
		return true
	}
	return false
}

func (n *nodeMsgs) applyPrepare(seqNo SeqNo, bucket BucketID) {
	next := n.next[bucket]
	next.prepare = seqNo + 1
}

func (n *nodeMsgs) applyCommit(seqNo SeqNo, bucket BucketID) {
	next := n.next[bucket]
	next.commit = seqNo + 1
}

func (n *nodeMsgs) applyCheckpoint(seqNo SeqNo) {
	n.nextCheckpoint = seqNo + n.epochConfig.checkpointInterval
}

func (n *nodeMsgs) moveWatermarks() {
	for _, next := range n.next {
		if next.prepare < n.epochConfig.lowWatermark {
			// TODO log warning
			next.prepare = n.epochConfig.lowWatermark
		}

		if next.commit < n.epochConfig.lowWatermark {
			// TODO log warning
			next.commit = n.epochConfig.lowWatermark
		}
	}

	if n.nextCheckpoint < n.epochConfig.lowWatermark {
		// TODO log warning
		n.nextCheckpoint = n.epochConfig.lowWatermark
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
	bucketStatuses := make([]NodeBucketStatus, len(n.next))
	for bucketID := range bucketStatuses {
		nextMsg := n.next[BucketID(bucketID)]
		bucketStatuses[bucketID] = NodeBucketStatus{
			BucketID:       bucketID,
			IsLeader:       nextMsg.leader,
			LastCheckpoint: uint64(n.nextCheckpoint - n.epochConfig.checkpointInterval),
			LastPrepare:    uint64(nextMsg.prepare - 1), // No underflow is possible, we start at seq 1
			LastCommit:     uint64(nextMsg.commit - 1),
		}
	}

	return &NodeStatus{
		ID:             uint64(n.id),
		BucketStatuses: bucketStatuses,
	}
}
