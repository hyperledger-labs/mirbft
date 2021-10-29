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

type pbftSlot struct {
	Sn                 t.SeqNr
	Preprepare         *isspbftpb.Preprepare
	NumMissingRequests int
}

type pbftProposalState struct {
	proposalsMade      int
	numPendingRequests t.NumRequests
	batchRequested     bool
	// Counts the ticks since last proposal.
	ticksSinceProposal int
}

type pbftInstance struct {
	ownID           t.NodeID
	config          *Config
	segment         *segment
	slots           map[t.SeqNr]*pbftSlot
	missingRequests map[string]*missingRequestInfo
	proposal        pbftProposalState
	logger          logging.Logger
	eventCreator    *sbEventCreator
}

func newPbftInstance(
	ownID t.NodeID,
	segment *segment,
	config *Config,
	eventCreator *sbEventCreator,
	logger logging.Logger) *pbftInstance {

	slots := make(map[t.SeqNr]*pbftSlot)
	for _, sn := range segment.SeqNrs {
		slots[sn] = &pbftSlot{Sn: sn}
	}
	return &pbftInstance{
		ownID:           ownID,
		segment:         segment,
		config:          config,
		slots:           slots,
		proposal:        pbftProposalState{}, // TODO: initialize this somehow.
		missingRequests: make(map[string]*missingRequestInfo),
		logger:          logger,
		eventCreator:    eventCreator,
	}
}

func (pbft *pbftInstance) ApplyEvent(event *isspb.SBInstanceEvent) *events.EventList {
	switch e := event.Type.(type) {

	case *isspb.SBInstanceEvent_MessageReceived:
		return pbft.handleMessage(e.MessageReceived.Msg, t.NodeID(e.MessageReceived.From))
	case *isspb.SBInstanceEvent_PendingRequests:
		return pbft.updatePendingRequests(t.NumRequests(e.PendingRequests.NumRequests))
	case *isspb.SBInstanceEvent_BatchReady:
		pbft.proposal.batchRequested = false
		return pbft.propose(e.BatchReady.Batch).
			PushBackList(pbft.updatePendingRequests(t.NumRequests(e.BatchReady.PendingRequestsLeft)))
	case *isspb.SBInstanceEvent_RequestsReady:
		return pbft.sendPrepare(pbft.slots[t.SeqNr(e.RequestsReady.Sn)])
	case *isspb.SBInstanceEvent_Tick:
		return pbft.handleTick()
	case *isspb.SBInstanceEvent_PbftPersistPreprepare:
		pbft.logger.Log(logging.LevelDebug, "Loading WAL event: Preprepare")
		return &events.EventList{}
	default:
		// Panic if message type is not known.
		panic(fmt.Sprintf("unknown PBFT SB instance event type: %T", event.Type))
	}
}

func (pbft *pbftInstance) Status() *isspb.SBStatus {
	// TODO: Return actual status here, not just a stub.
	return &isspb.SBStatus{Leader: pbft.segment.Leader.Pb()}
}

func (pbft *pbftInstance) Segment() *segment {
	return pbft.segment
}

func (pbft *pbftInstance) handleTick() *events.EventList {
	// Initialize the list of output events.
	eventsOut := &events.EventList{}

	pbft.proposal.ticksSinceProposal++
	if pbft.canPropose() {
		eventsOut.PushBackList(pbft.requestNewBatch())
	}

	return eventsOut
}

func (pbft *pbftInstance) updatePendingRequests(numRequests t.NumRequests) *events.EventList {

	pbft.proposal.numPendingRequests = numRequests

	if pbft.canPropose() {
		return pbft.requestNewBatch()
	} else {
		return &events.EventList{}
	}
}

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

func (pbft *pbftInstance) requestNewBatch() *events.EventList {
	pbft.proposal.batchRequested = true
	return (&events.EventList{}).PushBack(pbft.eventCreator.SBEvent(SBCutBatchEvent(pbft.config.MaxBatchSize)))
}

func (pbft *pbftInstance) propose(batch *requestpb.Batch) *events.EventList {

	sn := pbft.segment.SeqNrs[pbft.proposal.proposalsMade]
	pbft.proposal.proposalsMade++
	pbft.proposal.numPendingRequests -= t.NumRequests(len(batch.Requests))
	pbft.proposal.ticksSinceProposal = 0

	pbft.logger.Log(logging.LevelDebug, "Proposing.",
		"sn", sn, "batchSize", len(batch.Requests))

	// Create a preprepare message and an event for sending it.
	msgSendEvent := pbft.eventCreator.SendMessage(PbftPreprepareMessage(sn, batch), pbft.segment.Membership)

	// Create a WAL entry and an event to persist it.
	persistEvent := pbft.eventCreator.WALAppend(PbftPersistPreprepare(sn, batch))

	// First the preprepare needs to be persisted to the WAL, and only then it can be sent to the network.
	persistEvent.Next = []*eventpb.Event{msgSendEvent}
	return (&events.EventList{}).PushBack(persistEvent)
}

func (pbft *pbftInstance) handleMessage(message *isspb.SBInstanceMessage, from t.NodeID) *events.EventList {
	switch msg := message.Type.(type) {
	case *isspb.SBInstanceMessage_PbftPreprepare:
		return pbft.handlePreprepare(msg.PbftPreprepare, from)
	default:
		panic(fmt.Sprintf("unknown ISS PBFT message type: %T", message.Type))
	}
}

func (pbft *pbftInstance) handlePreprepare(preprepare *isspbftpb.Preprepare, from t.NodeID) *events.EventList {
	pbft.logger.Log(logging.LevelDebug, "Handling Preprepare.", "sn", preprepare.Sn)
	sn := t.SeqNr(preprepare.Sn)

	// TODO: This is a stub. Perform all the proper checks.

	slot, ok := pbft.slots[sn]
	if !ok {
		// Ignore Preprepare message with invalid sequence number.
		pbft.logger.Log(logging.LevelWarn, "Ignoring Preprepare message with invalid sequence number.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	if slot.Preprepare != nil {
		pbft.logger.Log(logging.LevelWarn, "Ignoring Preprepare message. Already preprepared.",
			"sn", sn, "from", from)
		return &events.EventList{}
	}

	slot.Preprepare = preprepare

	// Wait for all the requests to be received in the local buckets
	return (&events.EventList{}).PushBack(pbft.eventCreator.SBEvent(SBWaitForRequestsEvent(
		sn,
		preprepare.Batch.Requests,
	)))
}

func (pbft *pbftInstance) sendPrepare(slot *pbftSlot) *events.EventList {
	return (&events.EventList{}).PushBack(pbft.eventCreator.SBEvent(SBDeliverEvent(slot.Sn, slot.Preprepare.Batch)))
}
