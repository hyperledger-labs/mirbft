/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package events

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
)

// TODO: Change the package of protobuf generated events from state to event.

// Strip removes the follow-up events from event (stored under event.Next) and sets event.Next to nil.
// The removed events are stored in a new EventList that Strip returns a pointer to.
func Strip(event *state.Event) *EventList {

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
func Tick() *state.Event {
	return &state.Event{Type: &state.Event_Tick{Tick: &state.EventTick{}}}
}

// SendMessage returns an event of sending the message message to destinations.
// destinations is a slice of replica IDs that will be translated to actual addresses later.
func SendMessage(message *msgs.Message, destinations []uint64) *state.Event {
	return &state.Event{Type: &state.Event_SendMessage{SendMessage: &state.EventSendMessage{
		Destinations: destinations,
		Msg:          message,
	}}}
}

// MessageReceived returns an event representing the reception of a message from another node.
// The from parameter is the ID of the node the message was received from.
func MessageReceived(from uint64, message *msgs.Message) *state.Event {
	return &state.Event{Type: &state.Event_MessageReceived{MessageReceived: &state.EventMessageReceived{
		From: from,
		Msg:  message,
	}}}
}

// ClientRequest returns an event representing the reception of a request from a client.
func ClientRequest(clientID uint64, reqNo uint64, data []byte) *state.Event {
	return &state.Event{Type: &state.Event_Request{Request: &msgs.Request{
		ClientId: clientID,
		ReqNo:    reqNo,
		Data:     data,
	}}}
}

// HashRequest returns an event representing a request to the hashing module for computing the hash of data.
// the origin is an object used to maintain the context for the requesting module and will be included in the
// HashResult produced by the hashing module.
func HashRequest(data [][]byte, origin *state.HashOrigin) *state.Event {
	return &state.Event{Type: &state.Event_HashRequest{HashRequest: &state.EventHashRequest{
		Data:   data,
		Origin: origin,
	}}}
}

// HashResult returns an event representing the computation of a hash by the hashing module.
// It contains the computed digest and the HashOrigin, an object used to maintain the context for the requesting module,
// i.e., information about what to do with the contained digest.
func HashResult(digest []byte, origin *state.HashOrigin) *state.Event {
	return &state.Event{Type: &state.Event_HashResult{HashResult: &state.EventHashResult{
		Digest: digest,
		Origin: origin,
	}}}
}

// RequestReady returns an event signifying that a new request is ready to be inserted into the protocol state machine.
// This normally occurs when the request has been received, persisted, authenticated, and an authenticator is available.
func RequestReady(requestRef *msgs.RequestRef) *state.Event {
	return &state.Event{Type: &state.Event_RequestReady{RequestReady: &state.EventRequestReady{
		RequestRef: requestRef,
	}}}
}

// ============================================================
// DUMMY EVENTS FOR TESTING PURPOSES ONLY.
// ============================================================

func PersistDummyBatch(sn uint64, batch *msgs.Batch) *state.Event {
	return &state.Event{Type: &state.Event_PersistDummyBatch{PersistDummyBatch: &state.EventPersistDummyBatch{
		Sn:    sn,
		Batch: batch,
	}}}
}

func AnnounceDummyBatch(sn uint64, batch *msgs.Batch) *state.Event {
	return &state.Event{Type: &state.Event_AnnounceDummyBatch{AnnounceDummyBatch: &state.EventAnnounceDummyBatch{
		Sn:    sn,
		Batch: batch,
	}}}
}

// ============================================================
// LEGACY EVENTS.
// ============================================================
// TODO: Clean this up.

func (el *EventList) Initialize(initialParms *state.EventInitialParameters) *EventList {
	el.PushBack(EventInitialize(initialParms))
	return el
}

func EventInitialize(initialParms *state.EventInitialParameters) *state.Event {
	return &state.Event{
		Type: &state.Event_Initialize{
			Initialize: initialParms,
		},
	}
}

func (el *EventList) LoadPersistedEntry(index uint64, entry *msgs.Persistent) *EventList {
	el.PushBack(EventLoadPersistedEntry(index, entry))
	return el
}

func EventLoadPersistedEntry(index uint64, entry *msgs.Persistent) *state.Event {
	return &state.Event{
		Type: &state.Event_LoadPersistedEntry{
			LoadPersistedEntry: &state.EventLoadPersistedEntry{
				Index: index,
				Entry: entry,
			},
		},
	}
}

func (el *EventList) CompleteInitialization() *EventList {
	el.PushBack(EventCompleteInitialization())
	return el
}

func EventCompleteInitialization() *state.Event {
	return &state.Event{
		Type: &state.Event_CompleteInitialization{
			CompleteInitialization: &state.EventLoadCompleted{},
		},
	}
}

func (el *EventList) CheckpointResult(value []byte, pendingReconfigurations []*msgs.Reconfiguration, actionCheckpoint *state.ActionCheckpoint) *EventList {
	el.PushBack(EventCheckpointResult(value, pendingReconfigurations, actionCheckpoint))
	return el
}

func EventCheckpointResult(value []byte, pendingReconfigurations []*msgs.Reconfiguration, actionCheckpoint *state.ActionCheckpoint) *state.Event {
	return &state.Event{
		Type: &state.Event_CheckpointResult{
			CheckpointResult: &state.EventCheckpointResult{
				SeqNo: actionCheckpoint.SeqNo,
				Value: value,
				NetworkState: &msgs.NetworkState{
					Config:                  actionCheckpoint.NetworkConfig,
					Clients:                 actionCheckpoint.ClientStates,
					PendingReconfigurations: pendingReconfigurations,
				},
			},
		},
	}
}

func (el *EventList) RequestPersisted(ack *msgs.RequestAck) *EventList {
	el.PushBack(EventRequestPersisted(ack))
	return el
}

func EventRequestPersisted(ack *msgs.RequestAck) *state.Event {
	return &state.Event{
		Type: &state.Event_RequestPersisted{
			RequestPersisted: &state.EventRequestPersisted{
				RequestAck: ack,
			},
		},
	}
}

func (el *EventList) StateTransferComplete(networkState *msgs.NetworkState, actionStateTransfer *state.ActionStateTarget) *EventList {
	el.PushBack(EventStateTransferComplete(networkState, actionStateTransfer))
	return el
}

func EventStateTransferComplete(networkState *msgs.NetworkState, actionStateTransfer *state.ActionStateTarget) *state.Event {
	return &state.Event{
		Type: &state.Event_StateTransferComplete{
			StateTransferComplete: &state.EventStateTransferComplete{
				SeqNo:           actionStateTransfer.SeqNo,
				CheckpointValue: actionStateTransfer.Value,
				NetworkState:    networkState,
			},
		},
	}
}

func (el *EventList) StateTransferFailed(actionStateTransfer *state.ActionStateTarget) *EventList {
	el.PushBack(EventStateTransferFailed(actionStateTransfer))
	return el
}

func EventStateTransferFailed(actionStateTransfer *state.ActionStateTarget) *state.Event {
	return &state.Event{
		Type: &state.Event_StateTransferFailed{
			StateTransferFailed: &state.EventStateTransferFailed{
				SeqNo:           actionStateTransfer.SeqNo,
				CheckpointValue: actionStateTransfer.Value,
			},
		},
	}
}

func (el *EventList) Step(source uint64, msg *msgs.Msg) *EventList {
	el.PushBack(EventStep(source, msg))
	return el
}

func EventStep(source uint64, msg *msgs.Msg) *state.Event {
	return &state.Event{
		Type: &state.Event_Step{
			Step: &state.EventStep{
				Source: source,
				Msg:    msg,
			},
		},
	}
}

func (el *EventList) TickElapsed() *EventList {
	el.PushBack(EventTickElapsed())
	return el
}

func EventTickElapsed() *state.Event {
	return &state.Event{
		Type: &state.Event_TickElapsed{
			TickElapsed: &state.EventTickElapsed{},
		},
	}
}

func (el *EventList) ActionsReceived() *EventList {
	el.PushBack(EventActionsReceived())
	return el
}

func EventActionsReceived() *state.Event {
	return &state.Event{
		Type: &state.Event_ActionsReceived{
			ActionsReceived: &state.EventActionsReceived{},
		},
	}
}
