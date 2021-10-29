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
func ClientRequest(clientID t.ClientID, reqNo t.ReqNo, data []byte) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Request{Request: &requestpb.Request{
		ClientId: clientID.Pb(),
		ReqNo:    reqNo.Pb(),
		Data:     data,
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

func Deliver(sn t.SeqNr, batch *requestpb.Batch) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Deliver{Deliver: &eventpb.Deliver{
		Sn:    sn.Pb(),
		Batch: batch,
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
