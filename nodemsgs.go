/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type applyable int

const (
	past applyable = iota
	current
	future
	invalid
)

// nodeMsgs is responsible for two primary tasks
//
//   1) Acting as a buffer to handle times of network turbulence.  For instance, when the network
//      is moving its checkpoint watermarks, or adopting a new view, some nodes will receive the
//      intersection quorum certificate before others.  This means that those nodes may begin sending
//      messages for the new watermarks, or new epoch before the other nodes are ready to receive them.
//      The nodeMsgs instance will hold a bounded number of these messages in buffer to be ready to be
//      played when it adopts the network parameters the rest of the network has already performed.
//      This prevents the case of discarding out-of-bounds messages, then needing to refetch them, or
//      trigger a view-change.
//
//   2) Acting as a first-pass filter on byzantine behavior.  The msgfilters.go acts as a pre-filter,
//      excluding messages that are obviously malformed, but there are many other ways messages can
//      be malformed which are state dependent.  For instance, replicas may only ACK a single epoch
//      change message, and only the leader of an epoch may send a NewEpoch message.  By catching and
//      rejecting these malformed requests before passing them into the rest of the state machine,
//      it reduces the error handling requirements and prevents a DoS style attack from triggering
//      more expensive paths of the state machine.
//
// The general path of interaction with nodeMsgs is very simple.  When the state machine receives a
// message it gives the message to the nodeMsgs for a particular node via the 'ingest' method.  Then,
// it reads from the 'next()' method until no more messages are available.  When important pieces of
// state change (like the active epoch, or the watermarks), the state machine calls into nodeMsgs to
// trigger any re-evaluation of buffered messages.  The state machine will typically immediately drain
// the message queues via 'next'()' after such a reconfiguration.
//
// Expected behavior:
//
// Messages are divided into:
//   1) Active epoch dependent messages.  These messages are the normal three-phase commit in the green
//      path of Preprepare, Prepare, Commit.  For sequence numbers below the low watermark, these messages
//      should be discarded as in the past.  For epoch numbers below the last active epoch, these messages
//      should be discarded as in the past.  For sequence numbers above the high watermark, or above
//      the current epoch, they should be buffered as in the future.  For messages in the currently active
//      epoch within the watermarks, today, we buffer them to arrive in bucket order (e.g. in a 4 bucket
//      network we require that sequences arrive as (1, 5, 9, 13, ...) or (2, 6, 10, 14, ...), or (3, 7, ...)
//      etc.).  We may want to revist the decision to buffer these, but it is a relatively lightweight way
//      to ensure that we do not allow duplication of these messages from a node.
//   2) Checkpoint messages.  This is only the Checkpoint message.  Checkpoint messages below the low watermark
//      should be marked as in the past.
//   3) Client request related messages
//      (TODO, this needs work at the moment, but this would include the 'Forward' message as of today)
//   4) Epoch changing messages.  These messages are those relating to epoch change.  They include
//      Suspect, EpochChange, EpochChangeAck, NewEpoch, NewEpochEcho, and NewEpochReady.  Epoch related
//      messages for epochs older than the current epoch should be marked as in the past.  Depending on
//      the state of the active epoch, some messages should be marked as in the past (for instance, EpochChange
//      is only valid for an Epoch which is not prepending yet.  TODO, this area is a bit under development at
//      the moment, and needs to implement epoch-change fetching, so this will likely change a small bit.
type nodeMsgs struct {
	id             NodeID
	oddities       *oddities
	buffer         *list.List
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
		buffer:         list.New(),
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
	n.buffer.PushBack(outerMsg)
	if n.buffer.Len() > n.myConfig.BufferSize {
		e := n.buffer.Front()
		n.buffer.Remove(e)
	}
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
		requestData := innerMsg.Forward
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
	case *pb.Msg_EpochChangeAck:
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
	e := n.buffer.Front()
	if e == nil {
		return nil
	}

	for e != nil {
		msg := e.Value.(*pb.Msg)
		switch n.process(msg) {
		case past:
			n.oddities.alreadyProcessed(n.id, msg)
			x := e
			e = e.Next() // get next before removing current
			n.buffer.Remove(x)
		case current:
			n.buffer.Remove(e)
			return msg
		case future:
			// TODO, this is too aggressive, but useful for debugging
			n.myConfig.Logger.Debug("deferring apply as it's from the future", logBasics(n.id, msg)...)
			e = e.Next()
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
