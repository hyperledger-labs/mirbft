/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package iss contains the implementation of the ISS protocol, the new generation of MirBFT.
// For the details of the protocol, see (TODO).
// To use ISS, instantiate it by calling `iss.New` and use it as the Protocol module when instantiating a mirbft.Node.
// A default configuration (to pass, among other arguments, to `iss.New`)
// can be obtained from `iss.DefaultConfig`.
//
// Current status: This package is currently being implemented and is not yet functional.
package iss

import (
	"fmt"
	proto "github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/messagebuffer"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ============================================================
// Auxiliary types
// ============================================================

// The missingRequestInfo type represents information about the requests that the node is waiting to receive
// before accepting a proposal for a sequence number.
// A request counts as "received" by the node when the RequestReady event for that request is applied.
// For example, with the PBFT orderer, after the PBFT implementation of some SB instance receives a preprepare message,
// it will wait until ISS confirms that all requests referenced in the preprepare message have been received.
type missingRequestInfo struct {

	// Sequence number of the proposal for which requests are missing.
	Sn t.SeqNr

	// Set of all missing requests, represented as map of request references
	// indexed by the string representation of those request reference.
	Requests map[string]*requestpb.RequestRef

	// SB instance to be notified (via the RequestsReady event) when all missing requests have been received.
	Orderer sbInstance

	// Ticks until a retransmission of the missing requests is requested.
	TicksUntilNAck int // TODO: implement NAcks
}

// The segment type represents an ISS segment.
// It is use to parametrize an orderer (i.e. the SB instance).
type segment struct {

	// The leader node of the orderer.
	Leader t.NodeID

	// List of all nodes executing the orderer implementation.
	Membership []t.NodeID

	// List of sequence numbers for which the orderer is responsible.
	// This is the actual "segment" of the commit log.
	SeqNrs []t.SeqNr

	// List of IDs of buckets from which the orderer will draw the requests it proposes.
	BucketIDs []int
}

// The commitLogEntry type represents an entry of the commit log, the final output of the ordering process.
// Whenever an orderer delivers a batch (or a special abort value),
// it is inserted to the commit log in form of a commitLogEntry.
type commitLogEntry struct {

	// Sequence number at which this entry has been ordered.
	Sn t.SeqNr

	// The delivered request batch.
	Batch *requestpb.Batch

	// The digest (hash) of the batch.
	// TODO: Implement this. Think of what needs to be hashed here. Include metadata like Aborted too?
	Digest []byte

	// A flag indicating whether this entry is an actual request batch (false)
	// or whether the orderer delivered a special abort value (true).
	// TODO: Implement this.
	Aborted bool

	// In case Aborted is true, this field indicates the ID of the node
	// that is suspected to be the reason for the orderer aborting (usually the leader).
	// This information can be used by the leader selection policy at epoch transition.
	// TODO: Implement this.
	Suspect t.NodeID
}

// ============================================================
// ISS type and constructor
// ============================================================

// The ISS type represents the ISS protocol module to be used when instantiating a node.
// The type should not be instantiated directly, but only properly initialized values
// returned from the New() function should be used.
type ISS struct {

	// --------------------------------------------------------------------------------
	// These fields should only be set at initialization and remain static
	// --------------------------------------------------------------------------------

	// The ID of the node executing this instance of the protocol.
	ownID t.NodeID

	// The buckets holding incoming requests.
	// In each epoch, these buckets are re-distributed to the epoch's orderers,
	// each ordering one segment of the commit log.
	buckets *bucketGroup

	// Logger the ISS implementation uses to output log messages.
	// This is mostly for debugging - not to be confused with the commit log.
	logger logging.Logger

	// --------------------------------------------------------------------------------
	// These fields (or data they point to) might change
	// from epoch to epoch. Modified only by initEpoch()
	// --------------------------------------------------------------------------------

	// The ISS configuration parameters (e.g. number of buckets, batch size, etc...)
	// passed to New() when creating an ISS protocol instance.
	// TODO: Make it possible to change this dynamically.
	config *Config

	// The current epoch number.
	epoch t.EpochNr

	// The next ID to assign to a newly created orderer.
	// The orderers have monotonically increasing IDs that do *not* reset on epoch transitions.
	nextOrdererID t.SBInstanceID

	// Orderers (each of which is an SB instance) indexed by their IDs.
	orderers map[t.SBInstanceID]sbInstance

	// Index of orderers based on the buckets they are assigned.
	// For each bucket ID, this map stores the orderer to which the bucket is assigned in the current epoch.
	bucketOrderers map[int]sbInstance

	// --------------------------------------------------------------------------------
	// These fields are modified throughout an epoch
	// --------------------------------------------------------------------------------

	// Buffers representing a backlog of messages destined to future epochs.
	// Such messages might be sent by a node that already transitioned to a newer epoch,
	// but this node is slightly behind (still in an older epoch) and cannot process these message yet.
	// Such messages end up in this buffer (if there is buffer space) for later processing.
	// The buffer is checked after each epoch transition.
	messageBuffers map[t.NodeID]*messagebuffer.MessageBuffer

	// The final log of committed batches.
	// For each sequence number, it holds the committed batch (or the special abort value).
	// Each Deliver event of an orderer translates to inserting an entry in the commitLog.
	// This, in turn, leads to delivering the batch to the application,
	// as soon as all entries with lower sequence numbers have been delivered.
	// I.e., the entries are not necessarily inserted in order of their sequence numbers,
	// but they are delivered to the application in that order.
	commitLog map[t.SeqNr]*commitLogEntry

	// The first undelivered sequence number in the commitLog.
	// This field drives the in-order delivery of the log entries to the application.
	nextDeliveredSN t.SeqNr

	// For each sequence number, this field holds information about the requests that the node is waiting to receive
	// before accepting a proposal for that sequence number.
	// A request counts as "received" by the node when the RequestReady event for that request is applied.
	// As soon as all requests for a proposal have been received, the corresponding entry is deleted from this map.
	missingRequests map[t.SeqNr]*missingRequestInfo

	// For each request that is missing
	// (i.e. for which the proposal has been received by some orderer, but the request itself has not yet arrived),
	// this field holds a pointer to the object tracking missing requests for the whole proposal.
	// As soon as a request has been received the corresponding entry is deleted from this map.
	missingRequestIndex map[string]*missingRequestInfo
}

// New returns a new initialized instance of the ISS protocol module to be used when instantiating a mirbft.Node.
// Arguments:
// - ownID:  the ID of the node being instantiated with ISS.
// - config: ISS protocol-specific configuration (e.g. number of buckets, batch size, etc...).
//           see the documentation of the Config type for details.
// - logger: Logger the ISS implementation uses to output log messages.
func New(ownID t.NodeID, config *Config, logger logging.Logger) (*ISS, error) {

	// Check whether the passed configuration is valid.
	if err := CheckConfig(config); err != nil {
		return nil, fmt.Errorf("invalid ISS configuration: %w", err)
	}

	// Initialize a new ISS object.
	iss := &ISS{
		// Static fields
		ownID:   ownID,
		buckets: newBuckets(config.NumBuckets, logger),
		logger:  logger,

		// Fields modified only by initEpoch
		config:         config,
		epoch:          0,
		nextOrdererID:  0,
		orderers:       make(map[t.SBInstanceID]sbInstance),
		bucketOrderers: nil,

		// Fields modified throughout an epoch
		commitLog:           make(map[t.SeqNr]*commitLogEntry),
		nextDeliveredSN:     0,
		missingRequests:     nil, // allocated in initEpoch()
		missingRequestIndex: nil, // allocated in initEpoch()
		messageBuffers: messagebuffer.NewBuffers(
			removeNodeID(config.Membership, ownID), // Create a message buffer for everyone except for myself.
			config.MsgBufCapacity,
			logging.Decorate(logger, "Msgbuf: "),
		),
	}

	// Initialize the first epoch (epoch 0).
	// If the starting epoch is different (e.g. because the node is restarting),
	// the corresponding state (including epoch number) must be loaded through applying Events read from the WAL.
	iss.initEpoch(0)

	// Return the initialized protocol module.
	return iss, nil
}

// ============================================================
// Protocol Interface implementation
// ============================================================

// ApplyEvent receives one event and applies it to the ISS protocol state machine, potentially altering its state
// and producing a (potentially empty) list of more events to be applied to other modules.
func (iss *ISS) ApplyEvent(event *eventpb.Event) *events.EventList {
	switch e := event.Type.(type) {
	case *eventpb.Event_Init:
		return iss.applyInit(e.Init)
	case *eventpb.Event_Tick:
		return iss.applyTick(e.Tick)
	case *eventpb.Event_RequestReady:
		return iss.applyRequestReady(e.RequestReady)
	case *eventpb.Event_Iss: // The ISS event type wraps all ISS-specific events.
		switch issEvent := e.Iss.Type.(type) {
		case *isspb.ISSEvent_Sb:
			return iss.applySBEvent(issEvent.Sb)
		default:
			panic(fmt.Sprintf("unknown ISS event type: %T", event.Type))
		}
	case *eventpb.Event_MessageReceived:
		return iss.applyMessageReceived(e.MessageReceived)
	default:
		panic(fmt.Sprintf("unknown protocol (ISS) event type: %T", event.Type))
	}
}

// Status returns a protobuf representation of the current protocol state that can be later printed (TODO: Say how).
// This functionality is meant mostly for debugging and is *not* meant to provide an interface for
// serializing and deserializing the whole protocol state.
func (iss *ISS) Status() (s *statuspb.ProtocolStatus, err error) {
	// TODO: Implement this.
	return nil, nil
}

// ============================================================
// Event application
// ============================================================

// applyInit initializes the ISS protocol.
// This event is only expected to be applied once at startup,
// after all the events stored in the WAL have been applied and before any other event has been applied.
func (iss *ISS) applyInit(init *eventpb.Init) *events.EventList {

	// Trigger an Init event at all orderers.
	return iss.initOrderers()
}

// applyTick applies a single tick of the logical clock to the protocol state machine.
func (iss *ISS) applyTick(tick *eventpb.Tick) *events.EventList {
	eventsOut := &events.EventList{}

	// Relay tick to each orderer.
	sbTick := SBTickEvent()
	for _, orderer := range iss.orderers {
		eventsOut.PushBackList(orderer.ApplyEvent(sbTick))
	}

	// Demand retransmission of requests if retransmission timer expired.
	// TODO: iterate in a deterministic order!
	for _, missingRequests := range iss.missingRequests {
		// For all sequence numbers for which requests are missing,

		// Decrement the timer/counter.
		missingRequests.TicksUntilNAck--

		// If the timer expired (i.e. tick counter reached zero),
		if missingRequests.TicksUntilNAck == 0 {

			// Demand the retransmission of the missing requests.
			eventsOut.PushBackList(iss.demandRequestRetransmission(missingRequests))

			// Reset the timer/counter to wait until demanding the next retransmission.
			missingRequests.TicksUntilNAck = iss.config.RequestNAckTimeout
		}
	}

	return eventsOut
}

// applyRequestReady applies the RequestReady event to the state of the ISS protocol state machine.
// A RequestReady event means that a request is considered valid and authentic by the node and can be processed.
func (iss *ISS) applyRequestReady(requestReady *eventpb.RequestReady) *events.EventList {
	eventsOut := &events.EventList{}

	// Get request reference.
	ref := requestReady.RequestRef

	// Get bucket to which the new request maps.
	bucket := iss.buckets.RequestBucket(ref)

	// Add request to its bucket if it has not been added yet.
	if !bucket.Add(ref) {
		// If the request already has been added, do nothing and return, as if the event did not exist.
		// Returning here is important, because the rest of this function
		// must only be executed once for a request in an epoch (to prevent request duplication).
		return &events.EventList{}
	}

	// If necessary, notify the orderer responsible for this request to continue processing it.
	// (This is necessary in case the request has been "missing", i.e., proposed but not yet received.)
	eventsOut.PushBackList(iss.notifyOrderer(ref))

	// Count number of requests in all the buckets assigned to the same instance as the bucket of the received request.
	// These are all the requests pending to be proposed by the instance.
	// TODO: This is extremely inefficient and on the critical path
	//       (done on every request reception and TotalRequests loops over all buckets in the segment).
	//       Maintain a counter of requests in assigned buckets instead.
	pendingRequests := iss.buckets.Select(iss.bucketOrderers[bucket.ID].Segment().BucketIDs).TotalRequests()

	// If there are enough pending requests to fill a batch,
	// announce the total number of pending requests to the corresponding orderer.
	// Note that this deprives the orderer from the information about the number of pending requests,
	// as long as there are fewer of them than MaxBatchSize, but the orderer (so far) should not need this information.
	if pendingRequests >= iss.config.MaxBatchSize {
		eventsOut.PushBackList(iss.bucketOrderers[bucket.ID].ApplyEvent(SBPendingRequestsEvent(pendingRequests)))
	}

	return eventsOut
}

// applySBEvent applies an event triggered by or addressed to an orderer (i.e., instance of Sequenced Broadcast),
// if that event belongs to the current epoch.
// TODO: Update this comment when the TODO below is addressed.
func (iss *ISS) applySBEvent(event *isspb.SBEvent) *events.EventList {

	switch epoch := t.EpochNr(event.Epoch); {
	case epoch > iss.epoch:
		// Events coming from future epochs should never occur (as, unlike messages, events are all generated locally.)
		panic(fmt.Sprintf("trying to handle ISS event (type %T, instance %d) from future epoch: %d",
			event.Event.Type, event.Instance, event.Epoch))

	case epoch == iss.epoch:
		// If the event is from the current epoch, apply it in relation to the corresponding orderer instance.
		return iss.applySBInstanceEvent(event.Event, t.SBInstanceID(event.Instance))

	default: // epoch < iss.epoch:
		// If the event is from an old epoch, ignore it.
		// This might potentially happen if the epoch advanced while the event has been waiting in some buffer.
		// TODO: Is this really correct? Is it possible that orderers from old epochs
		//       (that have a reason to not yet be garbage-collected) still need to process events?
		iss.logger.Log(logging.LevelDebug, "Ignoring old event.", "epoch", epoch)
		return &events.EventList{}
	}
}

// applyMessageReceived applies a message received over the network.
func (iss *ISS) applyMessageReceived(messageReceived *eventpb.MessageReceived) *events.EventList {

	// Convenience variables used for readability.
	message := messageReceived.Msg
	from := t.NodeID(messageReceived.From)

	// ISS only accepts ISS messages. If another message is applied, the next line panics.
	switch msg := message.Type.(*messagepb.Message_Iss).Iss.Type.(type) {
	case *isspb.ISSMessage_Sb:
		return iss.applySBMessage(msg.Sb, from)
	case *isspb.ISSMessage_RetransmitRequests:
		return iss.applyRetransmitRequestsMessage(msg.RetransmitRequests, from)
	default:
		panic(fmt.Errorf("unknown ISS message type: %T", message.Type))
	}
}

// applySBMessage applies a message destined for an orderer (i.e. a Sequenced Broadcast implementation).
func (iss *ISS) applySBMessage(message *isspb.SBMessage, from t.NodeID) *events.EventList {

	switch epoch := t.EpochNr(message.Epoch); {
	case epoch > iss.epoch:
		// If the message is for a future epoch,
		// it might have been sent by a node that already transitioned to a newer epoch,
		// but this node is slightly behind (still in an older epoch) and cannot process the message yet.
		// In such case, save the message in a backlog (if there is buffer space) for later processing.
		iss.messageBuffers[from].Store(message)
		return &events.EventList{}

	case epoch == iss.epoch:
		// If the message is for the current epoch, check its validity and
		// apply it to the corresponding orderer in form of an SBMessageReceived event.
		if err := iss.validateSBMessage(message, from); err == nil {
			return iss.applySBInstanceEvent(SBMessageReceivedEvent(message.Msg, from), t.SBInstanceID(message.Instance))
		} else {
			iss.logger.Log(logging.LevelWarn, "Ignoring invalid SB message.",
				"type", fmt.Sprintf("%T", message.Msg.Type), "from", from, "error", err)
			return &events.EventList{}
		}

	default: // epoch < iss.epoch:
		// Ignore old messages
		// TODO: In case old SB instances from previous epoch still need to linger around,
		//       they might need to receive messages... Instead of simply checking the message epoch
		//       against the current epoch, we might need to remember which epochs have been already garbage-collected
		//       and use that information to decide what to do with messages.
		iss.logger.Log(logging.LevelWarn, "Ignoring SB message from an old epoch.",
			"type", fmt.Sprintf("%T", message.Msg.Type), "from", from, "epoch", epoch)
		return &events.EventList{}
	}
}

// applyRetransmitRequestsMessage applies a message demanding request retransmission to a node
// that received a proposal containing some requests, but was not yet able to authenticate those requests.
// TODO: Implement this function. See demandRequestRetransmission for more comments.
func (iss *ISS) applyRetransmitRequestsMessage(req *isspb.RetransmitRequests, from t.NodeID) *events.EventList {
	iss.logger.Log(logging.LevelWarn, "UNIMPLEMENTED: Ignoring request retransmission request.",
		"from", from, "numReqs", len(req.Requests))
	return &events.EventList{}
}

// ============================================================
// Additional protocol logic
// ============================================================

// initEpoch initializes a new ISS epoch with the given epoch number.
func (iss *ISS) initEpoch(newEpoch t.EpochNr) {

	// Compute the set of leaders for the new epoch.
	// Note that leader policy is stateful, choosing leaders deterministically based on the state of the system.
	// Its state must be consistent across all nodes when calling Leaders() on it.
	leaders := iss.config.LeaderPolicy.Leaders(newEpoch)

	// Compute the assignment of buckets to orderers (each leader will correspond to one orderer).
	leaderBuckets := iss.buckets.Distribute(leaders, newEpoch)

	// Reset missing request tracking information.
	// These fields could still contain some data if any preprepared batches were not committed on the "good path",
	// e.g., committed using state transfer or not committed at all.
	iss.missingRequests = make(map[t.SeqNr]*missingRequestInfo)
	iss.missingRequestIndex = make(map[string]*missingRequestInfo)

	// Initialize index of orderers based on the buckets they are assigned.
	// Given a bucket, this index helps locate the orderer to which the bucket is assigned.
	iss.bucketOrderers = make(map[int]sbInstance)

	// Create new segments of the commit log, one per leader selected by the leader selection policy.
	// Instantiate one orderer (SB instance) for each segment.
	for i, leader := range leaders {

		// Create segment.
		seg := &segment{
			Leader:     leader,
			Membership: iss.config.Membership,
			SeqNrs: sequenceNumbers(
				iss.nextDeliveredSN+t.SeqNr(i),
				t.SeqNr(len(leaders)),
				iss.config.SegmentLength),
			BucketIDs: leaderBuckets[leader],
		}

		// Instantiate a new PBFT orderer.
		// TODO: When more protocols are implemented, make this configurable, so other orderer types can be chosen.
		sbInst := newPbftInstance(
			iss.ownID,
			seg,
			iss.buckets.Select(seg.BucketIDs).TotalRequests(),
			iss.config,
			&sbEventService{epoch: newEpoch, instanceID: iss.nextOrdererID},
			logging.Decorate(iss.logger, "PBFT: ", "epoch", newEpoch, "instance", iss.nextOrdererID))
		iss.orderers[iss.nextOrdererID] = sbInst

		// Increment the ID to give to the next orderer.
		iss.nextOrdererID++

		// Populate index of orderers based on the buckets they are assigned.
		for _, bID := range seg.BucketIDs {
			iss.bucketOrderers[bID] = sbInst
		}
	}

	// Set the new epoch number as the current epoch.
	iss.epoch = newEpoch
}

func (iss *ISS) initOrderers() *events.EventList {
	eventsOut := &events.EventList{}

	sbInit := SBInitEvent()
	for _, orderer := range iss.orderers {
		eventsOut.PushBackList(orderer.ApplyEvent(sbInit))
	}

	return eventsOut
}

// epochFinished returns true when all the sequence numbers of the current epochs have been committed, otherwise false.
func (iss *ISS) epochFinished() bool {

	// TODO: Instead of checking all sequence numbers every time,
	//       remember the last sequence number of the epoch and compare it to iss.nextDeliveredSN
	for _, orderer := range iss.orderers {
		for _, sn := range orderer.Segment().SeqNrs {
			if iss.commitLog[sn] == nil {
				return false
			}
		}
	}
	return true
}

// validateSBMessage checks whether an SBMessage is valid in the current epoch.
// Returns nil if validation succeeds.
// If validation fails, returns the reason for which the message is considered invalid.
func (iss *ISS) validateSBMessage(message *isspb.SBMessage, from t.NodeID) error {

	// Message must be destined for the current epoch.
	if t.EpochNr(message.Epoch) != iss.epoch {
		return fmt.Errorf("invalid epoch: %d (current epoch is %d)", message.Instance, iss.epoch)
	}

	// Message must refer to a valid SB instance.
	if _, ok := iss.orderers[t.SBInstanceID(message.Instance)]; !ok {
		return fmt.Errorf("invalid SB instance ID: %d", message.Instance)
	}

	// Message must be sent by a node in the current membership.
	// TODO: This lookup is extremely inefficient, computing the membership set on each message validation.
	//       Cache the output of membershipSet() throughout the epoch.
	if _, ok := membershipSet(iss.config.Membership)[from]; !ok {
		return fmt.Errorf("sender of SB message not in the membership: %d", from)
	}

	return nil
}

// notifyOrderer checks whether a request is the last one missing from a proposal
// and, if so, notifies the corresponding orderer.
// If some orderer instance is waiting for a request (i.e., the request has been "missing" - proposed but not received),
// notifyOrderer marks the request as not missing any more and (if no more requests are missing) notifies that orderer
// to continue processing the corresponding proposal.
func (iss *ISS) notifyOrderer(reqRef *requestpb.RequestRef) *events.EventList {

	// Calculate the map key (string representation) of the request reference.
	reqKey := reqStrKey(reqRef)

	// If the request has been missing
	if missingRequests := iss.missingRequestIndex[reqKey]; missingRequests != nil {

		// Remove the request from the missing request index.
		delete(missingRequests.Requests, reqKey)

		// Remove the request from the list of missing requests of the corresponding proposal.
		delete(iss.missingRequestIndex, reqKey)

		// If this was the last missing request of the proposal
		if len(missingRequests.Requests) == 0 {

			// Remove the proposal from the set of proposals with missing requests
			// and notify the corresponding orderer that no more requests from this proposal are missing.
			delete(iss.missingRequests, missingRequests.Sn)
			return missingRequests.Orderer.ApplyEvent(SBRequestsReady(missingRequests.Sn))
		}
	}

	// If the orderer need not be notified yet, do nothing.
	return &events.EventList{}
}

// demandRequestRetransmission asks for the retransmission of missing requests.
// The result of this call should ultimately be a RequestReady event occurring for each of the missing requests.
// TODO: Explain that this can be done by asking the leader for a verifiable request authenticator
//       (e.g. a client signature) or contacting multiple nodes to confirm the reception of this request,
//       or even by asking the client directly to retransmit the request to this node.
// TODO: Implement at least one of these options.
func (iss *ISS) demandRequestRetransmission(reqInfo *missingRequestInfo) *events.EventList {

	// Create a slice of requests for which to demand retransmission.
	// This is only necessary because they are stored in a map.
	requests := make([]*requestpb.RequestRef, 0, len(reqInfo.Requests))
	// TODO: iterate in a deterministic order!
	for _, reqRef := range reqInfo.Requests {
		requests = append(requests, reqRef)
	}

	// Send a message to the leader that made the proposal for which requests are still missing.
	return (&events.EventList{}).PushBack(events.SendMessage(
		RetransmitRequestsMessage(requests), []t.NodeID{reqInfo.Orderer.Segment().Leader},
	))
}

// deliverCommitted delivers entries from the commitLog in order of their sequence numbers.
// Whenever a new entry is inserted in the commitLog, this function must be called
// to create Deliver events for all the batches that can be delivered to the application.
func (iss *ISS) deliverCommitted() *events.EventList {
	eventsOut := &events.EventList{}

	// In case more than one Deliver events are produced, they need to be chained (using the Next field)
	// so they are guaranteed to be processed in the same order as they have been created.
	// firstDeliverEvent is the one ultimately returned by deliverCommitted.
	// lastDeliverEvent is the one every new event is appended to (before becoming the lastDeliverEvent itself)
	var firstDeliverEvent *eventpb.Event = nil
	var lastDeliverEvent *eventpb.Event = nil

	// The iss.nextDeliveredSN variable always contains the lowest sequence number
	// for which no batch has been delivered yet.
	// As long as there is an entry in the commitLog with that sequence number,
	// deliver the corresponding batch and advance to the next sequence number.
	for iss.commitLog[iss.nextDeliveredSN] != nil {

		// Create a new Deliver event.
		deliverEvent := events.Deliver(iss.nextDeliveredSN, iss.commitLog[iss.nextDeliveredSN].Batch)

		// Output debugging information.
		iss.logger.Log(logging.LevelDebug, "Delivering entry.",
			"sn", iss.nextDeliveredSN, "nReq", len(iss.commitLog[iss.nextDeliveredSN].Batch.Requests))

		if firstDeliverEvent == nil {
			// If this is the first event produced, it is the first and last one at the same time
			firstDeliverEvent = deliverEvent
			lastDeliverEvent = deliverEvent
		} else {
			// If an event already has been produced,
			// append the new event to the end of the event chain and make it the last event.
			lastDeliverEvent.Next = append(lastDeliverEvent.Next, deliverEvent)
			lastDeliverEvent = deliverEvent
		}

		iss.nextDeliveredSN++
	}

	// If at least one deliver event occurred,
	// output the first produced Deliver event (with potential others chained to it using its Next field).
	if firstDeliverEvent != nil {
		eventsOut.PushBack(firstDeliverEvent)
	}

	// If the epoch is finished, transition to the next epoch.
	if iss.epochFinished() {

		// Initialize the internal data structures
		iss.initEpoch(iss.epoch + 1)

		// Give the init signals to the newly instantiated orderers.
		eventsOut.PushBackList(iss.initOrderers())

		// Process backlog of buffered SB messages.
		eventsOut.PushBackList(iss.applyBufferedMessages())
	}

	return eventsOut
}

// applyBufferedMessages applies all SB messages destined to the current epoch
// that have been buffered during past epochs.
// This function is always called directly after initializing a new epoch, except for epoch 0.
func (iss *ISS) applyBufferedMessages() *events.EventList {
	eventsOut := &events.EventList{}

	// Iterate over the all messages in all buffers
	for _, buffer := range iss.messageBuffers {
		buffer.Iterate(func(source t.NodeID, msg proto.Message) messagebuffer.Applicable {

			// Select messages destined to the current epoch as "current" (the second function will be called on those).
			switch epoch := t.EpochNr(msg.(*isspb.SBMessage).Epoch); {
			case epoch < iss.epoch:
				return messagebuffer.Past
			case epoch == iss.epoch:
				return messagebuffer.Current
			default: // e > iss.epoch
				return messagebuffer.Future
			}
			// Note that validation is not performed here, as it is performed anyway when applying the message.
			// Thus, the messagebuffer.Invalid option is not used.

		}, func(source t.NodeID, msg proto.Message) {

			// Apply all selected messages.
			eventsOut.PushBackList(iss.applySBMessage(msg.(*isspb.SBMessage), source))
		})
	}

	return eventsOut
}

// removeFromBuckets removes the given requests from their corresponding buckets.
// This happens when a batch is committed.
// TODO: Implement marking requests as "in flight"/proposed, so we don't accept proposals with duplicates.
//       This could maybe be done on the WaitForRequests/RequestsReady path...
func (iss *ISS) removeFromBuckets(requests []*requestpb.RequestRef) {

	// Remove each request from its bucket.
	for _, reqRef := range requests {
		iss.buckets.RequestBucket(reqRef).Remove(reqRef)
	}
}

// ============================================================
// Auxiliary functions
// ============================================================

// sequenceNumbers returns a list of sequence numbers of length `length`,
// starting with sequence number `start`, with the difference between two consecutive sequence number being `step`.
// This function is used to compute the sequence numbers of a segment.
// When there is `step` segments, their interleaving creates a consecutive block of sequence numbers
// that constitutes an epoch.
func sequenceNumbers(start t.SeqNr, step t.SeqNr, length int) []t.SeqNr {
	seqNrs := make([]t.SeqNr, length)
	for i, nextSn := 0, start; i < length; i, nextSn = i+1, nextSn+step {
		seqNrs[i] = nextSn
	}
	return seqNrs
}

// reqStrKey takes a request reference and transforms it to a string for using as a map key.
func reqStrKey(reqRef *requestpb.RequestRef) string {
	return fmt.Sprintf("%d-%d.%v", reqRef.ClientId, reqRef.ReqNo, reqRef.Digest)
}

// membershipSet takes a list of node IDs and returns a map of empty structs with an entry for each node ID in the list.
// The returned map is effectively a set representation of the given list,
// useful for testing whether any given node ID is in the set.
func membershipSet(membership []t.NodeID) map[t.NodeID]struct{} {

	// Allocate a new map representing a set of node IDs
	set := make(map[t.NodeID]struct{})

	// Add an empty struct for each node ID in the list.
	for _, nodeID := range membership {
		set[nodeID] = struct{}{}
	}

	// Return the resulting set of node IDs.
	return set
}

// removeNodeID emoves a node ID from a list of node IDs.
// Takes a membership list and a Node ID and returns a new list of nodeIDs containing all IDs from the membership list,
// except for (if present) the specified nID.
// This is useful for obtaining the list of "other nodes" by removing the own ID from the membership.
func removeNodeID(membership []t.NodeID, nID t.NodeID) []t.NodeID {

	// Allocate the new node list.
	others := make([]t.NodeID, 0, len(membership))

	// Add all membership IDs except for the specified one.
	for _, nodeID := range membership {
		if nodeID != nID {
			others = append(others, nodeID)
		}
	}

	// Return the new list.
	return others
}
