/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspbftpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ============================================================
// Auxiliary types
// ============================================================

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
// TODO: Extend this data structure when implementing proper PBFT.
type pbftSlot struct {

	// The received preprepare message.
	Preprepare *isspbftpb.Preprepare
}

// ============================================================
// PBFT orderer type and constructor
// ============================================================

// pbftInstance represents a PBFT orderer.
// It implements the sbInstance (instance of Sequenced broadcast) interface and thus can be used as an orderer for ISS.
type pbftInstance struct {

	// The ID of this node.
	ownID t.NodeID

	// The ISS configuration
	// TODO: Only using a PBFT-specific configuration. Most of the config parameters in the ISS configuration
	//       are not relevant here and need not (and thus should not) be visible to the orderer implementation.
	config *Config

	// The segment governing this SB instance, specifying the leader, the set of sequence numbers, the buckets, etc.
	segment *segment

	// Tracks the state related to proposing batches.
	proposal pbftProposalState

	// One pbftSlot per sequence number this orderer is responsible for.
	// Each slot tracks the state of the agreement protocol for one sequence number.
	slots map[t.SeqNr]*pbftSlot

	// Logger for outputting debugging messages.
	logger logging.Logger

	// ISS-provided event creator object.
	// All events produced by this pbftInstance must be created exclusively using the methods of eventService.
	// This ensures that the events are associated with this particular pbftInstance within the ISS protocol.
	eventService *sbEventService
}

// newPbftInstance allocates and initializes a new instance of the PBFT orderer.
// It takes the following parameters:
// - ownID:              The ID of this node.
// - segment:            The segment governing this SB instance,
//                       specifying the leader, the set of sequence numbers, the buckets, etc.
// - numPendingRequests: The number of requests currently pending in the buckets
//                       assigned to the new instance (segment.BucketIDs) and ready to be proposed by this PBFT orderer.
//                       This is required for the orderer to know whether it make proposals right away.
// - config:             The ISS configuration.
// - eventService:       Event creator object enabling the orderer to produce events.
//                       All events this orderer creates will be created using the methods of the eventService.
//                       The eventService must be configured to produce events associated with this PBFT orderer,
//                       since the implementation of the orderer does not know its own identity at the level of ISS.
// - logger:             Logger for outputting debugging messages.
func newPbftInstance(
	ownID t.NodeID,
	segment *segment,
	numPendingRequests t.NumRequests,
	config *Config,
	eventService *sbEventService,
	logger logging.Logger) *pbftInstance {

	// Initialize a new slot for each assigned sequence number.
	slots := make(map[t.SeqNr]*pbftSlot)
	for _, sn := range segment.SeqNrs {
		slots[sn] = &pbftSlot{}
	}

	// Set all the necessary fields of the new instance and return it.
	return &pbftInstance{
		ownID:   ownID,
		segment: segment,
		config:  config,
		slots:   slots,
		proposal: pbftProposalState{
			proposalsMade:      0,
			numPendingRequests: numPendingRequests,
			batchRequested:     false,
			ticksSinceProposal: 0,
		},
		logger:       logger,
		eventService: eventService,
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
	slot := pbft.slots[t.SeqNr(requestsReady.Sn)]
	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBDeliverEvent(
		t.SeqNr(requestsReady.Sn),
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

	// Convenience variable
	sn := t.SeqNr(preprepare.Sn)

	// TODO: This is a stub. Perform all the proper checks.

	// Look up the slot concerned by this message.
	slot, ok := pbft.slots[sn]

	// Ignore Preprepare message with invalid sequence number (i.e., pointing to a non-existing slot).
	if !ok {
		pbft.logger.Log(logging.LevelWarn, "Ignoring Preprepare message with invalid sequence number.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	// Ignore the received message
	// if a valid preprepare message with the same sequence number already has been received.
	if slot.Preprepare != nil {
		pbft.logger.Log(logging.LevelWarn, "Ignoring Preprepare message. Already preprepared.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	// Save the received preprepare message.
	slot.Preprepare = preprepare

	// Wait for all the requests to be received in the local buckets.
	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBWaitForRequestsEvent(
		sn,
		preprepare.Batch.Requests,
	)))
}

// ============================================================
// Additional protocol logic
// ============================================================

// canPropose returns true if the current state of the PBFT orderer allows for a new batch to be proposed.
func (pbft *pbftInstance) canPropose() bool {
	return pbft.ownID == pbft.segment.Leader && // Only the leader can propose

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
	return (&events.EventList{}).PushBack(pbft.eventService.SBEvent(SBCutBatchEvent(pbft.config.MaxBatchSize)))
}

// propose proposes a new request batch by sending a preprepare message.
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

	// Create a preprepare message and an event for sending it.
	msgSendEvent := pbft.eventService.SendMessage(PbftPreprepareMessage(sn, batch), pbft.segment.Membership)

	// Create a WAL entry and an event to persist it.
	persistEvent := pbft.eventService.WALAppend(PbftPersistPreprepare(sn, batch))

	// First the preprepare needs to be persisted to the WAL, and only then it can be sent to the network.
	persistEvent.Next = []*eventpb.Event{msgSendEvent}
	return (&events.EventList{}).PushBack(persistEvent)
}
