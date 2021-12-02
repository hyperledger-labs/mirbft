/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package events

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// Strip removes the follow-up events from event (stored under event.Next) and sets event.Next to nil.
// The removed events are stored in a new EventList that Strip returns a pointer to.
func Strip(event *eventpb.Event) *EventList {

	// Create new EventList.
	nextList := &EventList{}

	// Add all follow-up events to the new EventList.
	for _, e := range event.Next {
		nextList.PushBack(e)
	}

	// Delete follow-up events from original event.
	event.Next = nil

	// Return new EventList.
	return nextList
}

// ============================================================
// Event Constructors
// ============================================================

// Init returns an event instructing a module to initialize.
// This event is the first to be applied to a module after applying all events from the WAL.
func Init() *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Init{Init: &eventpb.Init{}}}
}

// Tick returns an event representing a tick - the event of one step of logical time having elapsed.
func Tick() *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Tick{Tick: &eventpb.Tick{}}}
}

// SendMessage returns an event of sending the message message to destinations.
// destinations is a slice of replica IDs that will be translated to actual addresses later.
func SendMessage(message *messagepb.Message, destinations []t.NodeID) *eventpb.Event {

	// TODO: This conversion can potentially be very inefficient!

	return &eventpb.Event{Type: &eventpb.Event_SendMessage{SendMessage: &eventpb.SendMessage{
		Destinations: t.NodeIDSlicePb(destinations),
		Msg:          message,
	}}}
}

// MessageReceived returns an event representing the reception of a message from another node.
// The from parameter is the ID of the node the message was received from.
func MessageReceived(from t.NodeID, message *messagepb.Message) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_MessageReceived{MessageReceived: &eventpb.MessageReceived{
		From: from.Pb(),
		Msg:  message,
	}}}
}

// ClientRequest returns an event representing the reception of a request from a client.
func ClientRequest(clientID t.ClientID, reqNo t.ReqNo, data []byte, authenticator []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Request{Request: &requestpb.Request{
		ClientId:      clientID.Pb(),
		ReqNo:         reqNo.Pb(),
		Data:          data,
		Authenticator: authenticator,
	}}}
}

// HashRequest returns an event representing a request to the hashing module for computing the hash of data.
// the origin is an object used to maintain the context for the requesting module and will be included in the
// HashResult produced by the hashing module.
func HashRequest(data [][]byte, origin *eventpb.HashOrigin) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_HashRequest{HashRequest: &eventpb.HashRequest{
		Data:   data,
		Origin: origin,
	}}}
}

// HashResult returns an event representing the computation of a hash by the hashing module.
// It contains the computed digest and the HashOrigin, an object used to maintain the context for the requesting module,
// i.e., information about what to do with the contained digest.
func HashResult(digest []byte, origin *eventpb.HashOrigin) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_HashResult{HashResult: &eventpb.HashResult{
		Digest: digest,
		Origin: origin,
	}}}
}

// RequestReady returns an event signifying that a new request is ready to be inserted into the protocol state machine.
// This normally occurs when the request has been received, persisted, authenticated, and an authenticator is available.
func RequestReady(requestRef *requestpb.RequestRef) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_RequestReady{RequestReady: &eventpb.RequestReady{
		RequestRef: requestRef,
	}}}
}

// WALAppend returns an event of appending a new entry to the WAL.
// This event is produced by the protocol state machine for persisting its state.
func WALAppend(event *eventpb.Event, retentionIndex t.WALRetIndex) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_WalAppend{WalAppend: &eventpb.WALAppend{
		Event:          event,
		RetentionIndex: retentionIndex.Pb(),
	}}}
}

// WALEntry returns an event of reading an entry from the WAL.
// Those events are used at system initialization.
func WALEntry(persistedEvent *eventpb.Event, retentionIndex t.WALRetIndex) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_WalEntry{WalEntry: &eventpb.WALEntry{
		Event: persistedEvent,
	}}}
}

// Deliver returns an event of delivering a request batch to the application in sequence number order.
func Deliver(sn t.SeqNr, batch *requestpb.Batch) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Deliver{Deliver: &eventpb.Deliver{
		Sn:    sn.Pb(),
		Batch: batch,
	}}}
}

// VerifyRequestSig returns an event of a client tracker requesting the verification of a client request signature.
// This event is routed to the Crypto module that issues a RequestSigVerified event in response.
func VerifyRequestSig(reqRef *requestpb.RequestRef, signature []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_VerifyRequestSig{VerifyRequestSig: &eventpb.VerifyRequestSig{
		RequestRef: reqRef,
		Signature:  signature,
	}}}
}

// RequestSigVerified represents the result of a client signature verification by the Crypto module.
// It is routed to the client tracker that initially issued a VerifyRequestSig event.
func RequestSigVerified(reqRef *requestpb.RequestRef, valid bool, error string) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_RequestSigVerified{RequestSigVerified: &eventpb.RequestSigVerified{
		RequestRef: reqRef,
		Valid:      valid,
		Error:      error,
	}}}
}

// StoreVerifiedRequest returns an event representing an event the ClientTracker emits
// to request storing a request, including its payload and authenticator, in the request store.
func StoreVerifiedRequest(reqRef *requestpb.RequestRef, data []byte, authenticator []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_StoreVerifiedRequest{StoreVerifiedRequest: &eventpb.StoreVerifiedRequest{
		RequestRef:    reqRef,
		Data:          data,
		Authenticator: authenticator,
	}}}
}

// AppSnapshotRequest returns an event representing the protocol module asking the application for a state snapshot.
// sn is the number of batches delivered to the application when taking the snapshot
// (i.e. the sequence number of the first unprocessed batch).
// The application itself need not be aware of sn, it is only by the protocol
// to be able to identify the response of the application in form of an AppSnapshot event.
func AppSnapshotRequest(sn t.SeqNr) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_AppSnapshotRequest{AppSnapshotRequest: &eventpb.AppSnapshotRequest{
		Sn: sn.Pb(),
	}}}
}

// AppSnapshot returns an event representing the application making a snapshot of its state.
// sn is the number of batches delivered to the application when taking the snapshot
// (i.e. the sequence number of the first unprocessed batch)
// and data is the serialized application state (the snapshot itself).
func AppSnapshot(sn t.SeqNr, data []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_AppSnapshot{AppSnapshot: &eventpb.AppSnapshot{
		Sn:   sn.Pb(),
		Data: data,
	}}}
}

// ============================================================
// DUMMY EVENTS FOR TESTING PURPOSES ONLY.
// ============================================================

func PersistDummyBatch(sn t.SeqNr, batch *requestpb.Batch) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_PersistDummyBatch{PersistDummyBatch: &eventpb.PersistDummyBatch{
		Sn:    sn.Pb(),
		Batch: batch,
	}}}
}

func AnnounceDummyBatch(sn t.SeqNr, batch *requestpb.Batch) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_AnnounceDummyBatch{AnnounceDummyBatch: &eventpb.AnnounceDummyBatch{
		Sn:    sn.Pb(),
		Batch: batch,
	}}}
}

func StoreDummyRequest(reqRef *requestpb.RequestRef, data []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_StoreDummyRequest{StoreDummyRequest: &eventpb.StoreDummyRequest{
		RequestRef: reqRef,
		Data:       data,
	}}}
}
