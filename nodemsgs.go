/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
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
	id            nodeID
	oddities      *oddities
	epochMsgs     *epochMsgs
	networkConfig *pb.NetworkState_Config
	buffer        *msgBuffer
	logger        Logger
	myConfig      *pb.StateEvent_InitialParameters
}

type epochMsgs struct {
	myConfig      *pb.StateEvent_InitialParameters
	epochConfig   *pb.EpochConfig
	networkConfig *pb.NetworkState_Config
	epoch         *activeEpoch
	id            nodeID

	// next maintains the info about the next expected messages for
	// a particular bucket.
	next map[bucketID]*nextMsg
}

// TODO base this buffer on size, not count
type msgBuffer struct {
	buffer   *list.List
	logger   Logger
	myConfig *pb.StateEvent_InitialParameters
}

func newMsgBuffer(myConfig *pb.StateEvent_InitialParameters, logger Logger) *msgBuffer {
	return &msgBuffer{
		buffer:   list.New(),
		logger:   logger,
		myConfig: myConfig,
	}
}

func (mb *msgBuffer) store(msg *pb.Msg) {
	mb.buffer.PushBack(msg)
	if uint32(mb.buffer.Len()) > mb.myConfig.BufferSize {
		e := mb.buffer.Front()
		oldMsg := mb.buffer.Remove(e).(*pb.Msg)
		mb.logger.Warn(fmt.Sprintf("dropping message of type %T", oldMsg.Type))
	}
}

func (mb *msgBuffer) next(filter func(*pb.Msg) applyable) *pb.Msg {
	e := mb.buffer.Front()
	if e == nil {
		return nil
	}

	for e != nil {
		msg := e.Value.(*pb.Msg)
		switch filter(msg) {
		case past:
			x := e
			e = e.Next() // get next before removing current
			mb.buffer.Remove(x)
		case current:
			mb.buffer.Remove(e)
			return msg
		case future:
			e = e.Next()
		case invalid:
			x := e
			e = e.Next() // get next before removing current
			mb.buffer.Remove(x)
		}
	}

	return nil
}

type nextMsg struct {
	leader  bool
	prepare uint64 // Note Prepare is Preprepare if Leader is true
	commit  uint64
}

func newNodeMsgs(nodeID nodeID, networkConfig *pb.NetworkState_Config, logger Logger, myConfig *pb.StateEvent_InitialParameters, oddities *oddities) *nodeMsgs {

	return &nodeMsgs{
		id:       nodeID,
		oddities: oddities,
		logger:   logger,

		buffer:        newMsgBuffer(myConfig, logger),
		myConfig:      myConfig,
		networkConfig: networkConfig,
	}
}

func (n *nodeMsgs) setActiveEpoch(epoch *activeEpoch) {
	if epoch == nil {
		n.epochMsgs = nil
		return
	}
	n.epochMsgs = newEpochMsgs(n.id, epoch, n.myConfig)
}

// ingest the message for management by the nodeMsgs.  This message
// may immediately become available to read from next(), or it may be enqueued
// for future consumption
func (n *nodeMsgs) ingest(outerMsg *pb.Msg) {
	n.buffer.store(outerMsg)
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
	case epoch < n.epochMsgs.epochConfig.Number:
		return past
	case epoch > n.epochMsgs.epochConfig.Number:
		return future
	}

	// current

	return n.epochMsgs.process(outerMsg)
}

func (n *nodeMsgs) next() *pb.Msg {
	return n.buffer.next(n.process)
}

func newEpochMsgs(nodeID nodeID, epoch *activeEpoch, myConfig *pb.StateEvent_InitialParameters) *epochMsgs {
	next := map[bucketID]*nextMsg{}
	for bucketID, leaderID := range epoch.buckets {
		nm := &nextMsg{
			leader:  nodeID == leaderID,
			prepare: 1,
			commit:  1,
		}

		for seqNo := uint64(bucketID) + epoch.lowWatermark(); seqNo <= epoch.highWatermark(); seqNo += uint64(len(epoch.buckets)) {
			seq := epoch.sequence(seqNo)
			if seq.state >= sequencePrepared {
				nm.prepare++
			}
			if seq.state == sequenceCommitted {
				nm.commit++
			}
		}

		next[bucketID] = nm
	}

	return &epochMsgs{
		myConfig:      myConfig,
		epochConfig:   epoch.epochConfig,
		networkConfig: epoch.networkConfig,
		epoch:         epoch,
		next:          next,
		id:            nodeID,
	}
}

func (n *epochMsgs) moveWatermarks(seqNo uint64) {
	for i := seqNo + 1; i < seqNo+1+uint64(len(n.next)); i++ {
		next := n.next[n.seqToBucket(i)]
		column := n.seqToColumn(i)
		if next.prepare < column {
			next.prepare = column
		}
		if next.commit < column {
			next.commit = column
		}
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
	}

	panic("programming error, unreachable")
}

func (em *epochMsgs) seqToBucket(seqNo uint64) bucketID {
	return seqToBucket(seqNo, em.networkConfig)
}

func (em *epochMsgs) seqToColumn(seqNo uint64) uint64 {
	return seqToColumn(seqNo, em.epochConfig, em.networkConfig)
}

func (n *epochMsgs) processPreprepare(msg *pb.Preprepare) applyable {
	next, ok := n.next[n.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
	}

	switch {
	case !next.leader:
		return invalid
	case next.prepare > n.seqToColumn(msg.SeqNo) || msg.SeqNo < n.epoch.lowWatermark():
		return past
	case next.prepare == n.seqToColumn(msg.SeqNo):
		next.prepare++
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processPrepare(msg *pb.Prepare) applyable {
	next, ok := n.next[n.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
	}

	switch {
	case next.leader:
		return invalid
	case next.prepare > n.seqToColumn(msg.SeqNo) || msg.SeqNo < n.epoch.lowWatermark():
		return past
	case next.prepare == n.seqToColumn(msg.SeqNo):
		next.prepare++
		return current
	default:
		return future
	}
}

func (n *epochMsgs) processCommit(msg *pb.Commit) applyable {
	next, ok := n.next[n.seqToBucket(msg.SeqNo)]
	if !ok {
		return invalid
	}

	if msg.SeqNo > n.epoch.highWatermark() {
		return future
	}

	switch {
	case next.commit > n.seqToColumn(msg.SeqNo) || msg.SeqNo < n.epoch.lowWatermark():
		return past
	case next.commit == n.seqToColumn(msg.SeqNo) && next.prepare > next.commit:
		next.commit++
		return current
	default:
		return future
	}
}

func (n *nodeMsgs) status() *status.NodeBuffer {
	if n.epochMsgs == nil {
		return &status.NodeBuffer{
			ID: uint64(n.id),
		}
	}

	bucketStatuses := make([]status.NodeBucket, len(n.epochMsgs.next))
	for bid := range bucketStatuses {
		nextMsg := n.epochMsgs.next[bucketID(bid)]
		bucketStatuses[bid] = status.NodeBucket{
			BucketID:    bid,
			IsLeader:    nextMsg.leader,
			LastPrepare: uint64(nextMsg.prepare - 1), // No underflow is possible, we start at seq 1
			LastCommit:  uint64(nextMsg.commit - 1),
		}
	}

	return &status.NodeBuffer{
		ID:      uint64(n.id),
		Buckets: bucketStatuses,
	}
}
