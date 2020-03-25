/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
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
	epochMsgs      *epochMsgs
	myConfig       *Config
	networkConfig  *pb.NetworkConfig
	clientWindows  *clientWindows
	nextCheckpoint uint64
}

type epochMsgs struct {
	myConfig      *Config
	epochConfig   *epochConfig
	epoch         *epoch
	clientWindows *clientWindows

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[BucketID]*nextMsg
}

type nextMsg struct {
	leader  bool
	prepare uint64 // Note Prepare is Preprepare if Leader is true
	commit  uint64
}

func newNodeMsgs(nodeID NodeID, networkConfig *pb.NetworkConfig, myConfig *Config, clientWindows *clientWindows, oddities *oddities) *nodeMsgs {

	return &nodeMsgs{
		id:       nodeID,
		oddities: oddities,

		// nextCheckpoint: epochConfig.lowWatermark + epochConfig.checkpointInterval,
		// XXX we should initialize this properly, sort of like the above
		nextCheckpoint: uint64(networkConfig.CheckpointInterval),
		clientWindows:  clientWindows,
		buffer:         map[*pb.Msg]struct{}{},
		myConfig:       myConfig,
		networkConfig:  networkConfig,
	}
}

func (n *nodeMsgs) setActiveEpoch(epoch *epoch) {
	if epoch == nil {
		n.epochMsgs = nil
		return
	}
	n.epochMsgs = newEpochMsgs(n.id, n.clientWindows, epoch, n.myConfig)
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
		return current // TODO, at least detect past
	case *pb.Msg_Checkpoint:
		return n.processCheckpoint(innerMsg.Checkpoint)
	case *pb.Msg_Forward:
		requestData := innerMsg.Forward.RequestData
		clientWindow, ok := n.clientWindows.clientWindow(requestData.ClientId)
		if !ok {
			if requestData.ReqNo == 1 {
				return current
			} else {
				return future
			}
		}
		switch {
		case clientWindow.lowWatermark > requestData.ReqNo:
			return past
		case clientWindow.highWatermark < requestData.ReqNo:
			return future
		default:
			return current
		}
	case *pb.Msg_EpochChange:
		return current // TODO, decide if this is actually current
	case *pb.Msg_NewEpoch:
		if innerMsg.NewEpoch.Config.Number%uint64(len(n.networkConfig.Nodes)) != uint64(n.id) {
			return invalid
		}
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

	if n.epochMsgs == nil {
		return future
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
			// TODO, this is too aggressive, but useful for debugging
			n.myConfig.Logger.Debug("deferring apply as it's from the future", logBasics(n.id, msg)...)
		}
	}

	return nil
}

func (n *nodeMsgs) processCheckpoint(msg *pb.Checkpoint) applyable {
	switch {
	case n.nextCheckpoint > msg.SeqNo:
		return past
	case n.nextCheckpoint == msg.SeqNo:
		if n.epochMsgs == nil {
			return future
		}
		for _, next := range n.epochMsgs.next {
			if next.commit < n.epochMsgs.epochConfig.seqToColumn(msg.SeqNo) {
				return future
			}
		}

		n.nextCheckpoint = msg.SeqNo + uint64(n.networkConfig.CheckpointInterval)
		return current
	default:
		return future
	}
}

func newEpochMsgs(nodeID NodeID, clientWindows *clientWindows, epoch *epoch, myConfig *Config) *epochMsgs {
	next := map[BucketID]*nextMsg{}
	for bucketID, leaderID := range epoch.config.buckets {
		nm := &nextMsg{
			leader:  nodeID == leaderID,
			prepare: 1,
			commit:  1,
		}

		ec := epoch.config

		for i := int(bucketID); i < len(epoch.sequences); i += len(ec.buckets) {
			if epoch.sequences[i].state >= Prepared {
				nm.prepare++
			}
		}

		next[bucketID] = nm
	}

	return &epochMsgs{
		clientWindows: clientWindows,
		myConfig:      myConfig,
		epochConfig:   epoch.config,
		epoch:         epoch,
		next:          next,
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

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
	}

	for _, batchEntry := range msg.Batch {
		clientWindow, ok := n.clientWindows.clientWindow(batchEntry.ClientId)
		if !ok {
			return future
		}

		if batchEntry.ReqNo < clientWindow.lowWatermark {
			return past
		}

		if batchEntry.ReqNo > clientWindow.highWatermark {
			return future
		}

		request := clientWindow.request(batchEntry.ReqNo)
		if request == nil {
			// XXX, this is a dirty hack which assumes eventually,
			// all requests arrive, it does not handle byzantine behavior.
			return future
		}
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

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
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

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
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

func (n *nodeMsgs) status() *NodeStatus {
	if n.epochMsgs == nil {
		return &NodeStatus{
			ID:             uint64(n.id),
			LastCheckpoint: uint64(n.nextCheckpoint) - uint64(n.networkConfig.CheckpointInterval),
		}
	}

	bucketStatuses := make([]NodeBucketStatus, len(n.epochMsgs.next))
	for bucketID := range bucketStatuses {
		nextMsg := n.epochMsgs.next[BucketID(bucketID)]
		bucketStatuses[bucketID] = NodeBucketStatus{
			BucketID:    bucketID,
			IsLeader:    nextMsg.leader,
			LastPrepare: uint64(nextMsg.prepare - 1), // No underflow is possible, we start at seq 1
			LastCommit:  uint64(nextMsg.commit - 1),
		}
	}

	return &NodeStatus{
		ID:             uint64(n.id),
		BucketStatuses: bucketStatuses,
	}
}
