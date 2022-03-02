/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/messagebuffer"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspbftpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ============================================================
// Auxiliary types
// ============================================================

// PBFTConfig holds PBFT-specific configuration parameters used by a concrete instance of PBFT.
// They are mostly inherited from the ISS configuration at the time of creating the PBFT instance.
type PBFTConfig struct {

	// The IDs of all nodes that execute this instance of the protocol.
	// Must not be empty.
	Membership []t.NodeID

	// The maximum number of logical time ticks between two proposals of new batches during normal operation.
	// This parameter caps the waiting time in order to bound latency.
	// When MaxProposeDelay ticks have elapsed since the last proposal,
	// the protocol tries to propose a new request batch, even if the batch is not full (or even completely empty).
	// Must not be negative.
	MaxProposeDelay int

	// Maximal number of bytes used for message backlogging buffers
	// (only message payloads are counted towards MsgBufCapacity).
	// Same as Config.MsgBufCapacity, but used only for one instance of PBFT.
	// Must not be negative.
	MsgBufCapacity int

	// The maximal number of requests in a proposed request batch.
	// As soon as the number of pending requests reaches MaxBatchSize,
	// the PBFT instance may decide to immediately propose a new request batch.
	// Setting MaxBatchSize to zero signifies no limit on batch size.
	MaxBatchSize t.NumRequests

	// View change timeout for view 0 in ticks.
	// With each view change, the timeout doubles (without changing this value)
	ViewChangeTimeout int
}

// pbftProposalState tracks the state of the pbftInstance related to proposing batches.
// The proposal state is only used if this node is the leader of this instance of PBFT.
type pbftProposalState struct {

	// Tracks the number of proposals already made.
	// Used to calculate the sequence number of the next proposal (from the associated segment's sequence numbers)
	// and to stop proposing when a proposal has been made for all sequence numbers.
	proposalsMade int

	// Saves the number of pending requests that are available to be included in the next proposal,
	// as reported by ISS through the PendingRequests event.
	// When this value exceeds config.MaxBatchSize, a proposal might be made before the config.MaxProposeDelay.
	numPendingRequests t.NumRequests

	// Flag indicating whether a new batch has been requested from ISS.
	// This effectively means that a proposal is in progress and no new proposals should be started.
	// This is required, since the PBFT implementation does not assemble request batches itself,
	// as it is not managing the request buckets
	// (which, in general, do not exclusively concern the orderer and are thus managed by ISS directly).
	// Instead, when the PBFT instance is ready to propose,
	// it requests a new batch from the enclosing ISS implementation to provide the batch.
	batchRequested bool

	// Counts the logical clock ticks since last proposal.
	// Used to detect when config.MaxProposeDelay has elapsed.
	ticksSinceProposal int
}

// pbftSlot tracks the state of the agreement protocol for one sequence number,
// such as messages received, progress of the reliable broadcast, etc.
type pbftSlot struct {

	// The received preprepare message.
	Preprepare *isspbftpb.Preprepare

	// Prepare messages received.
	// A nil entry signifies that an invalid message has been discarded.
	Prepares map[t.NodeID]*isspbftpb.Prepare

	// Valid prepare messages received.
	// Serves mostly as an optimization to not re-validate already validated messages.
	ValidPrepares []*isspbftpb.Prepare

	// Commit messages received.
	Commits map[t.NodeID]*isspbftpb.Commit

	// Valid commit messages received.
	// Serves mostly as an optimization to not re-validate already validated messages.
	ValidCommits []*isspbftpb.Commit

	// The proposed batch
	Batch *requestpb.Batch

	// The digest of the proposed (preprepared) batch
	Digest []byte

	// Flags denoting whether a batch has been, respectively, preprepared, prepared, and committed in this slot
	// Note that Preprepared == true is not equivalent to Preprepare != nil, since the Preprepare message is stored
	// before the node preprepares the proposal (it first has to verify that all requests are available
	// and only then can preprepare the proposal and send the Prepare messages).
	Preprepared bool
	Prepared    bool
	Committed   bool
}

// newPbftSlot allocates a new pbftSlot object and returns it, initializing all its fields.
func newPbftSlot() *pbftSlot {
	return &pbftSlot{
		Preprepare:    nil,
		Prepares:      make(map[t.NodeID]*isspbftpb.Prepare),
		ValidPrepares: make([]*isspbftpb.Prepare, 0),
		Commits:       make(map[t.NodeID]*isspbftpb.Commit),
		ValidCommits:  make([]*isspbftpb.Commit, 0),
		Batch:         nil,
		Digest:        nil,
		Preprepared:   false,
		Prepared:      false,
		Committed:     false,
	}
}

// populateFromPrevious carries over state from a pbftSlot used in the previous view to this pbftSlot,
// based on the state of the previous slot.
// This is used during view change, when the protocol initializes a new PBFT view.
func (slot *pbftSlot) populateFromPrevious(prevSlot *pbftSlot) {

	// If the slot has already committed a batch, just copy over the result.
	// If the slot has preprepared, but not committed a batch, resurrect all the requests in the batch.
	if prevSlot.Committed {
		slot.Committed = true
		slot.Digest = prevSlot.Digest
		slot.Batch = prevSlot.Batch
	} else if prevSlot.Preprepared {

		panic("implement resurrection")
	}
}

// ============================================================
// PBFT orderer type and constructor
// ============================================================

// pbftInstance represents a PBFT orderer.
// It implements the sbInstance (instance of Sequenced broadcast) interface and thus can be used as an orderer for ISS.
type pbftInstance struct {

	// The ID of this node.
	ownID t.NodeID

	// PBFT-specific configuration parameters (e.g. view change timeout, etc.)
	config *PBFTConfig

	// The segment governing this SB instance, specifying the leader, the set of sequence numbers, the buckets, etc.
	segment *segment

	// Buffers representing a backlog of messages destined to future views.
	// A node that already transitioned to a newer view might send messages,
	// while this node is behind (still in an older view) and cannot process these messages yet.
	// Such messages end up in this buffer (if there is buffer space) for later processing.
	// The buffer is checked after each view change.
	messageBuffers map[t.NodeID]*messagebuffer.MessageBuffer

	// Tracks the state related to proposing batches.
	proposal pbftProposalState

	// For each view, slots contains one pbftSlot per sequence number this orderer is responsible for.
	// Each slot tracks the state of the agreement protocol for one sequence number.
	slots map[t.PBFTViewNr]map[t.SeqNr]*pbftSlot

	// Logger for outputting debugging messages.
	logger logging.Logger

	// ISS-provided event creator object.
	// All events produced by this pbftInstance must be created exclusively using the methods of eventService.
	// This ensures that the events are associated with this particular pbftInstance within the ISS protocol.
	eventService *sbEventService

	// PBFT view
	view t.PBFTViewNr

	// Flag indicating whether this node is currently performing a view change.
	// It is set on sending the ViewChange message and cleared on accepting a new view.
	inViewChange bool
}

// newPbftInstance allocates and initializes a new instance of the PBFT orderer.
// It takes the following parameters:
// - ownID:              The ID of this node.
// - segment:            The segment governing this SB instance,
//                       specifying the leader, the set of sequence numbers, the buckets, etc.
// - numPendingRequests: The number of requests currently pending in the buckets
//                       assigned to the new instance (segment.BucketIDs) and ready to be proposed by this PBFT orderer.
//                       This is required for the orderer to know whether it make proposals right away.
// - config:             PBFT-specific configuration parameters.
// - eventService:       Event creator object enabling the orderer to produce events.
//                       All events this orderer creates will be created using the methods of the eventService.
//                       The eventService must be configured to produce events associated with this PBFT orderer,
//                       since the implementation of the orderer does not know its own identity at the level of ISS.
// - logger:             Logger for outputting debugging messages.
func newPbftInstance(
	ownID t.NodeID,
	segment *segment,
	numPendingRequests t.NumRequests,
	config *PBFTConfig,
	eventService *sbEventService,
	logger logging.Logger) *pbftInstance {

	// Set all the necessary fields of the new instance and return it.
	return &pbftInstance{
		ownID:   ownID,
		segment: segment,
		config:  config,
		slots:   make(map[t.PBFTViewNr]map[t.SeqNr]*pbftSlot),
		proposal: pbftProposalState{
			proposalsMade:      0,
			numPendingRequests: numPendingRequests,
			batchRequested:     false,
			ticksSinceProposal: 0,
		},
		messageBuffers: messagebuffer.NewBuffers(
			removeNodeID(config.Membership, ownID), // Create a message buffer for everyone except for myself.
			config.MsgBufCapacity,                  // TODO: Configure this separately for ISS buffers and PBFT buffers.
			//       Even better, share the same buffers with ISS.
			logging.Decorate(logger, "Msgbuf: "),
		),
		logger:       logger,
		eventService: eventService,
		view:         0,
		inViewChange: false,
	}
}

// ============================================================
// SB Instance Interface implementation
// ============================================================

// ApplyEvent receives one event and applies it to the PBFT orderer state machine, potentially altering its state
// and producing a (potentially empty) list of more events.
func (pbft *pbftInstance) ApplyEvent(event *isspb.SBInstanceEvent) *events.EventList {
	switch e := event.Type.(type) {

	case *isspb.SBInstanceEvent_Init:
		return pbft.applyInit()
	case *isspb.SBInstanceEvent_Tick:
		return pbft.applyTick()
	case *isspb.SBInstanceEvent_PendingRequests:
		return pbft.applyPendingRequests(t.NumRequests(e.PendingRequests.NumRequests))
	case *isspb.SBInstanceEvent_BatchReady:
		return pbft.applyBatchReady(e.BatchReady)
	case *isspb.SBInstanceEvent_RequestsReady:
		return pbft.applyRequestsReady(e.RequestsReady)
	case *isspb.SBInstanceEvent_PbftPersistPreprepare:
		return pbft.applyPbftPersistPreprepare(e.PbftPersistPreprepare)
	case *isspb.SBInstanceEvent_MessageReceived:
		return pbft.applyMessageReceived(e.MessageReceived.Msg, t.NodeID(e.MessageReceived.From))
	default:
		// Panic if message type is not known.
		panic(fmt.Sprintf("unknown PBFT SB instance event type: %T", event.Type))
	}
}

// Segment returns the segment associated with this orderer.
func (pbft *pbftInstance) Segment() *segment {
	return pbft.segment
}

// Status returns a protobuf representation of the current state of the orderer that can be later printed.
// This functionality is meant mostly for debugging and is *not* meant to provide an interface for
// serializing and deserializing the whole protocol state.
func (pbft *pbftInstance) Status() *isspb.SBStatus {
	// TODO: Return actual status here, not just a stub.
	return &isspb.SBStatus{Leader: pbft.segment.Leader.Pb()}
}

// ============================================================
// Event application
// ============================================================

// applyInit takes all the actions resulting from the PBFT orderer's initial state.
// The Init event is expected to be the first event applied to the orderer,
// except for events read from the WAL at startup, which are expected to be applied even before the Init event.
func (pbft *pbftInstance) applyInit() *events.EventList {

	// Initialize the first PBFT view
	pbft.initView(0)

	// Make a proposal if one can be made right away.
	if pbft.canPropose() {
		return pbft.requestNewBatch()
	} else {
		return &events.EventList{}
	}

}

// applyTick applies a single tick of the logical clock to the protocol state machine.
func (pbft *pbftInstance) applyTick() *events.EventList {
	eventsOut := &events.EventList{}

	// Update the proposal timer value and start a new proposal if applicable (i.e. if this tick made the timer expire).
	pbft.proposal.ticksSinceProposal++
	if pbft.canPropose() {
		eventsOut.PushBackList(pbft.requestNewBatch())
	}

	return eventsOut
}

// applyPendingRequests processes a notification form ISS about the number of requests in buckets ready to be proposed.
func (pbft *pbftInstance) applyPendingRequests(numRequests t.NumRequests) *events.EventList {

	// Update the orderer's view on the number of pending requests.
	pbft.proposal.numPendingRequests = numRequests

	if pbft.canPropose() {
		// Start a new proposal if applicable (i.e. if the number of pending requests reached config.MaxBatchSize).
		return pbft.requestNewBatch()
	} else {
		// Do nothing otherwise.
		return &events.EventList{}
	}
}

// applyBatchReady processes a new batch ready to be proposed.
// This event is triggered by ISS in response to the CutBatch event produced by this orderer.
func (pbft *pbftInstance) applyBatchReady(batch *isspb.SBBatchReady) *events.EventList {

	// Clear flag that was set in requestNewBatch(), so that new batches can be requested if necessary.
	pbft.proposal.batchRequested = false

	// Propose the received batch and update the number of pending requests that remain after the batch was created.
	return pbft.propose(batch.Batch).PushBackList(pbft.applyPendingRequests(t.NumRequests(batch.PendingRequestsLeft)))
}

// applyRequestsReady processes the notification from ISS
// that all requests in a received preprepare message are now available and authenticated.
func (pbft *pbftInstance) applyRequestsReady(requestsReady *isspb.SBRequestsReady) *events.EventList {

	// Extract the reference from the event
	// The type cast is safe (unless there is a bug in the implementation), since only the PBFT ReqWaitReference
	// type is ever used by PBFT.
	ref := requestsReady.Ref.Type.(*isspb.SBReqWaitReference_Pbft).Pbft

	// Get the slot referenced by the RequestsReady Event.
	// This reference has been created when creating the WaitForRequests Event,
	// to which this RequestsReady Event is a response.
	// That is also why we can be sure that the slot exists and do not need to check for a nil map.
	slot := pbft.slots[t.PBFTViewNr(ref.View)][t.SeqNr(ref.Sn)]

	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBDeliverEvent(
		t.SeqNr(ref.Sn),
		slot.Preprepare.Batch,
	)))
}

// applyPbftPersistPreprepare processes a preprepare message loaded from the WAL.
func (pbft *pbftInstance) applyPbftPersistPreprepare(pp *isspbftpb.PersistPreprepare) *events.EventList {

	// TODO: Implement this.
	pbft.logger.Log(logging.LevelDebug, "Loading WAL event: Preprepare (unimplemented)")
	return &events.EventList{}
}

// applyMessageReceived handles a received PBFT protocol message.
func (pbft *pbftInstance) applyMessageReceived(message *isspb.SBInstanceMessage, from t.NodeID) *events.EventList {

	// Based on the message type, call the appropriate handler method.
	switch msg := message.Type.(type) {
	case *isspb.SBInstanceMessage_PbftPreprepare:
		return pbft.applyMsgPreprepare(msg.PbftPreprepare, from)
	default:
		panic(fmt.Sprintf("unknown ISS PBFT message type: %T", message.Type))
	}
}

// applyMsgPreprepare applies a received preprepare message.
// It performs the necessary checks and, if successful,
// requests a confirmation from ISS that all contained requests have been received and authenticated.
func (pbft *pbftInstance) applyMsgPreprepare(preprepare *isspbftpb.Preprepare, from t.NodeID) *events.EventList {

	// Convenience variables
	sn := t.SeqNr(preprepare.Sn)
	msgView := t.PBFTViewNr(preprepare.View)

	// Check if the sender is the leader of this segment.
	if from != pbft.segment.Leader {
		pbft.logger.Log(logging.LevelWarn, "Ignoring Preprepare message from non-leader.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	// Ignore messages from old views.
	if msgView < pbft.view {
		pbft.logger.Log(logging.LevelDebug, "Ignoring Preprepare message from old view.",
			"sn", sn, "from", from, "msgView", msgView, "localView", pbft.view)
		return &events.EventList{}
	}

	// If message is from a future view, buffer it and return.
	if msgView > pbft.view || pbft.inViewChange {
		pbft.messageBuffers[from].Store(preprepare)
		return &events.EventList{}
		// TODO: When view change is implemented, get the messages out of the buffer.
	}

	// Look up the slot concerned by this message
	// and check if the sequence number is assigned to this PBFT instance.
	slot, ok := pbft.slots[pbft.view][sn]
	if !ok {
		pbft.logger.Log(logging.LevelDebug, "Ignoring Preprepare message. Wrong sequence number.",
			"sn", sn, "from", from, "msgView", msgView)
		return &events.EventList{}
	}

	// Check if the slot has already been committed (this can happen with state transfer).
	if slot.Committed {
		pbft.logger.Log(logging.LevelDebug, "Ignoring Preprepare message. Slot already committed.", "sn", sn)
		return &events.EventList{}
	}

	// Check that this is the first Preprepare message received.
	// Note that checking the pbft.Preprepared flag instead would be incorrect,
	// as that flag is only set upon receiving the RequestsReady Event.
	if slot.Preprepare != nil {
		pbft.logger.Log(logging.LevelDebug, "Ignoring Preprepare message. Already preprepared or prepreparing.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	// Save the received preprepare message.
	slot.Preprepare = preprepare

	// Wait for all the requests to be received in the local buckets.
	// Operation continues on reception of the RequestsReady event.
	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBWaitForRequestsEvent(
		PbftReqWaitReference(sn, pbft.view),
		preprepare.Batch.Requests,
	)))
}

// ============================================================
// Additional protocol logic
// ============================================================

func (pbft *pbftInstance) initView(view t.PBFTViewNr) {
	// Sanity check
	if view < pbft.view {
		panic(fmt.Sprintf("Starting a view (%d) older than the current one (%d)", view, pbft.view))
	}

	// Do not start the same view more than once.
	// View 0 is also started only once (the code makes sure that startView(0) is only called at initialization),
	// it's just that the default value of the variable is already 0 - that's why it needs an exception.
	if view != 0 && view == pbft.view {
		return
	}

	// Sanity check. The view must not yet have been initialized.
	// TODO: Remove this eventually
	if _, ok := pbft.slots[view]; ok {
		panic(fmt.Sprintf("view %d already initialized", view))
	}

	// Initialize PBFT slots for the new view, one for each sequence number.
	pbft.slots[view] = make(map[t.SeqNr]*pbftSlot)
	for _, sn := range pbft.segment.SeqNrs {

		// Create a fresh, empty slot.
		pbft.slots[view][sn] = newPbftSlot()

		// Except for initialization of view 0, carry over state from the previous view.
		if view > 0 {
			pbft.slots[view][sn].populateFromPrevious(pbft.slots[pbft.view][sn])
		}
	}

	// Finally, update the view number.
	pbft.view = view
}

// canPropose returns true if the current state of the PBFT orderer allows for a new batch to be proposed.
// Note that "new batch" means a "fresh" batch proposed during normal operation outside of view change.
// Proposals part of a new view message during a view change do not call this function and are treated separately.
func (pbft *pbftInstance) canPropose() bool {
	return pbft.ownID == pbft.segment.Leader && // Only the leader can propose

		// No regular proposals can be made after a view change.
		// This is specific for the SB-version of PBFT used in ISS and deviates from the standard PBFT protocol.
		pbft.view == 0 &&

		// A new batch must not have been requested (if it has, we are already in the process of proposing).
		!pbft.proposal.batchRequested &&

		// There must still be a free sequence number for which a proposal can be made.
		pbft.proposal.proposalsMade < len(pbft.segment.SeqNrs) &&

		// Either the batch timeout must have passed, or there must be enough requests for a full batch.
		// The value 0 for config.MaxBatchSize means no limit on batch size,
		// i.e., a proposal cannot be triggered just by the number of pending requests.
		(pbft.proposal.ticksSinceProposal >= pbft.config.MaxProposeDelay ||
			(pbft.config.MaxBatchSize != 0 && pbft.proposal.numPendingRequests >= pbft.config.MaxBatchSize))
}

// requestNewBatch asks (by means of a CutBatch event) ISS to assemble a new request batch.
// When the batch is ready, it must be passed to the orderer using the BatchReady event.
func (pbft *pbftInstance) requestNewBatch() *events.EventList {

	// Set a flag indicating that a batch has been requested,
	// so that no new batches will be requested before the reception of this one.
	// It will be cleared when BatchReady is received.
	pbft.proposal.batchRequested = true

	// Emit the CutBatch event.
	// Operation continues on reception of the BatchReady event.
	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBCutBatchEvent(pbft.config.MaxBatchSize)))
}

// propose proposes a new request batch by sending a Preprepare message.
// propose assumes that the state of the PBFT orderer allows sending a new proposal
// and does not perform any checks in this regard.
func (pbft *pbftInstance) propose(batch *requestpb.Batch) *events.EventList {

	// Update proposal counter and reset proposal timer.
	sn := pbft.segment.SeqNrs[pbft.proposal.proposalsMade]
	pbft.proposal.proposalsMade++
	pbft.proposal.ticksSinceProposal = 0

	// Log debug message.
	pbft.logger.Log(logging.LevelDebug, "Proposing.",
		"sn", sn, "batchSize", len(batch.Requests))

	// Create a Preprepare message and an Event for sending it.
	msgSendEvent := pbft.eventService.SendMessage(
		PbftPreprepareMessage(sn, pbft.view, batch, false),
		pbft.segment.Membership,
	)

	// Create a WAL entry and an event to persist it.
	persistEvent := pbft.eventService.WALAppend(PbftPersistPreprepare(sn, batch))

	// First the preprepare needs to be persisted to the WAL, and only then it can be sent to the network.
	persistEvent.Next = []*eventpb.Event{msgSendEvent}
	return (&events.EventList{}).PushBack(persistEvent)
}
