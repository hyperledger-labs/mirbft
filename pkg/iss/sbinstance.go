/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// sbInstance represents an instance of Sequenced Broadcast and is the type of each ISS orderer.
// Each orderer (being an sbInstance) is assigned a segment and is responsible for
// proposing and delivering request batches for all sequence numbers described by the segment,
// while the batches only contain requests belonging to buckets referenced by the segment.
type sbInstance interface {

	// ApplyEvent receives one event and applies it to the SB implementation's state machine,
	// potentially altering its state and producing a (potentially empty) list of more events
	// to be applied to other modules.
	// Since the SB instance is always part of ISS, it is only the ISS code that supplies events to this function.
	// The isspb.SBInstanceEvent type defines the events that can be exchanged between an SB instance and ISS.
	// The events returned from ApplyEvent must be produced by an sbEventService
	// injected to the SB instance at creation.
	ApplyEvent(event *isspb.SBInstanceEvent) *events.EventList

	// Segment returns the segment assigned to this SB instance.
	Segment() *segment

	// Status returns a protobuf representation of the current state of the SB instance's implementation.
	// This functionality is meant mostly for debugging and is *not* meant to provide an interface for
	// serializing and deserializing the whole protocol state.
	Status() *isspb.SBStatus
}

// ============================================================
// ISS methods handling SB instance events
// ============================================================

// applySBInstanceEvent applies one event produced by an orderer to the ISS state, potentially altering its state
// and producing a (potentially empty) list of events to be applied to other modules.
func (iss *ISS) applySBInstanceEvent(event *isspb.SBInstanceEvent, instance t.SBInstanceID) *events.EventList {
	switch e := event.Type.(type) {
	case *isspb.SBInstanceEvent_Deliver:
		return iss.applySBInstDeliver(e.Deliver, instance)
	case *isspb.SBInstanceEvent_CutBatch:
		return iss.applySBInstCutBatch(instance, t.NumRequests(e.CutBatch.MaxSize))
	case *isspb.SBInstanceEvent_WaitForRequests:
		return iss.applySBInstWaitForRequests(instance, e.WaitForRequests)
	default:
		if orderer, ok := iss.orderers[instance]; ok {
			return orderer.ApplyEvent(event)
		} else {
			panic(fmt.Sprintf("invalid SB event instance id (event type %T): %d", event.Type, instance))
		}
	}
}

// applySBInstDeliver processes the event of an SB instance delivering a request batch (or the special abort value)
// for a sequence number. It creates a corresponding commitLog entry and requests the computation of its hash.
// Note that applySBInstDeliver does not yet insert the entry to the commitLog. This will be done later.
// Operation continues on reception of the HashResult event.
func (iss *ISS) applySBInstDeliver(deliver *isspb.SBDeliver, instance t.SBInstanceID) *events.EventList {

	// Remove the delivered requests from their respective buckets.
	iss.removeFromBuckets(deliver.Batch.Requests)

	// Create a new preliminary log entry based on the delivered batch and hash it.
	// Note that, although tempting, the hash used internally by the SB implementation cannot be re-used.
	// Apart from making the SB abstraction extremely leaky (reason enough not to do it), it would also be incorrect.
	// E.g., in PBFT, if the digest of the corresponding Preprepare message was used, the hashes at different nodes
	// might mismatch, if they commit in different PBFT views (and thus using different Preprepares).
	unhashedEntry := &CommitLogEntry{
		Sn:      t.SeqNr(deliver.Sn),
		Batch:   deliver.Batch,
		Digest:  nil,
		Aborted: deliver.Aborted,
		Suspect: iss.orderers[instance].Segment().Leader,
	}

	// Save the preliminary hash entry to a map where it can be looked up when the hash result arrives.
	iss.unhashedLogEntries[unhashedEntry.Sn] = unhashedEntry

	// Create a HashRequest for the commit log entry with the newly delivered hash.
	// The hash is required for state transfer.
	// Only after the hash is computed, the log entry can be stored in the log (and potentially delivered to the App).
	return (&events.EventList{}).PushBack(events.HashRequest(
		serializeLogEntryForHashing(unhashedEntry),
		LogEntryHashOrigin(unhashedEntry.Sn),
	))
}

// applySBInstCutBatch processes a request by an orderer for a new request batch that the orderer will propose.
// applySBInstCutBatch removes up to maxBatchSize requests from the buckets currently assigned to the orderer
// with ID instanceID, constructs a batch containing those requests, and submits the batch to the orderer
// via a BatchReady event.
// If there are no requests in the corresponding buckets, applySBInstCutBatch still provides an empty batch immediately.
func (iss *ISS) applySBInstCutBatch(instanceID t.SBInstanceID, maxBatchSize t.NumRequests) *events.EventList {

	// Look up the orderer that asks for a new batch.
	orderer := iss.orderers[instanceID]

	// Look up the relevant buckets, based on the orderer's segment.
	buckets := iss.buckets.Select(orderer.Segment().BucketIDs)

	// Create a new batch, removing its requests from their buckets.
	batch := buckets.CutBatch(maxBatchSize)

	// Count the remaining requests in the buckets.
	requestsLeft := buckets.TotalRequests()

	// Notify submit the new batch to the orderer.
	return orderer.ApplyEvent(SBBatchReadyEvent(batch, requestsLeft))
}

// applySBInstWaitForRequests processes the WaitForRequests event triggered by an orderer.
// This event is triggered when the orderer received a proposal and is verifying
// whether all the requests contained in the proposal are available to the local node.
// applySBInstWaitForRequests checks for the availability of those requests in the local buckets
// and, if all requests are available, immediately notifies the orderer via a RequestsReady event.
// Otherwise, it creates a missingRequestInfo entry referencing all the missing requests,
// such that the orderer can be notified as soon as all missing requests become available.
// If the missing requests do not become available within a configured timeout (config.RequestNAckTimeout),
// ISS will actively retry obtaining the requests (and their corresponding authentication data).
func (iss *ISS) applySBInstWaitForRequests(
	instanceID t.SBInstanceID,
	waitForRequests *isspb.SBWaitForRequests,
) *events.EventList {

	// Get reference of the proposal being verified.
	ref := missingRequestInfoRef{
		Orderer: instanceID,
		SBRef:   waitForRequests.Reference,
	}

	// Initialize a new missingRequestInfo entry that will contain a reference to all missing requests.
	missingReqs := &missingRequestInfo{
		Ref:            ref,
		Requests:       make(map[string]*requestpb.RequestRef, 0),
		Orderer:        iss.orderers[instanceID],
		TicksUntilNAck: iss.config.RequestNAckTimeout,
	}

	// Check for the presence of each request in its corresponding bucket.
	for _, reqRef := range waitForRequests.Requests {

		// If the request is not in the bucket it maps to, register it as missing.
		if !iss.buckets.RequestBucket(reqRef).Contains(reqRef) {
			reqKey := reqStrKey(reqRef)

			// Associate missing request with this WaitForRequests event.
			// Once this and all other request associated with this event are available, the orderer can be notified.
			missingReqs.Requests[reqKey] = reqRef

			// Create a global index entry for the missing request.
			// This is used to locate this missingRequestInfo once the request is received.
			iss.missingRequestIndex[reqKey] = missingReqs
		}
	}

	if len(missingReqs.Requests) > 0 {
		// If any requests are missing, register the missingRequestInfo and do not notify the orderer.
		iss.missingRequests[ref] = missingReqs
		return &events.EventList{}
	} else {
		// If all requests are already available, notify the orderer directly.
		return missingReqs.Orderer.ApplyEvent(SBRequestsReady(ref.SBRef))
	}
}
