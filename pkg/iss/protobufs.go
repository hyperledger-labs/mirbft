/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// This file provides constructors for protobuf messages (also used to represent events) used by ISS.
// The primary purpose is convenience and improved readability of the ISS code,
// As creating protobuf objects is rather verbose in Go.
// Moreover, in case the definitions of some protocol buffers change,
// this file should be the only one that will potentially need to change.

// TODO: Write documentation comments for the functions in this file.
//       Part of the text can probably be copy-pasted from the documentation of the functions handling those events.

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ============================================================
// Events
// ============================================================

// ------------------------------------------------------------
// ISS Events

func Event(event *isspb.ISSEvent) *eventpb.Event {
	return &eventpb.Event{Type: &eventpb.Event_Iss{Iss: event}}
}

func HashOrigin(origin *isspb.ISSHashOrigin) *eventpb.HashOrigin {
	return &eventpb.HashOrigin{Type: &eventpb.HashOrigin_Iss{origin}}
}

func PersistCheckpointEvent(sn t.SeqNr, appSnapshot []byte) *eventpb.Event {
	return Event(&isspb.ISSEvent{Type: &isspb.ISSEvent_PersistCheckpoint{PersistCheckpoint: &isspb.PersistCheckpoint{
		Sn:          sn.Pb(),
		AppSnapshot: appSnapshot,
	}}})
}

func PersistStableCheckpointEvent(stableCheckpoint *isspb.StableCheckpoint) *eventpb.Event {
	return Event(&isspb.ISSEvent{Type: &isspb.ISSEvent_PersistStableCheckpoint{
		PersistStableCheckpoint: &isspb.PersistStableCheckpoint{
			StableCheckpoint: stableCheckpoint,
		},
	}})
}

func StableCheckpointEvent(stableCheckpoint *isspb.StableCheckpoint) *eventpb.Event {
	return Event(&isspb.ISSEvent{Type: &isspb.ISSEvent_StableCheckpoint{
		StableCheckpoint: stableCheckpoint,
	}})
}

func SBEvent(epoch t.EpochNr, instance t.SBInstanceID, event *isspb.SBInstanceEvent) *eventpb.Event {
	return Event(&isspb.ISSEvent{Type: &isspb.ISSEvent_Sb{Sb: &isspb.SBEvent{
		Epoch:    epoch.Pb(),
		Instance: instance.Pb(),
		Event:    event,
	}}})
}

func LogEntryHashOrigin(logEntrySN t.SeqNr) *eventpb.HashOrigin {
	return HashOrigin(&isspb.ISSHashOrigin{Type: &isspb.ISSHashOrigin_LogEntrySn{LogEntrySn: logEntrySN.Pb()}})
}

func SBHashOrigin(epoch t.EpochNr, instance t.SBInstanceID, origin *isspb.SBInstanceHashOrigin) *eventpb.HashOrigin {
	return HashOrigin(&isspb.ISSHashOrigin{Type: &isspb.ISSHashOrigin_Sb{Sb: &isspb.SBHashOrigin{
		Epoch:    epoch.Pb(),
		Instance: instance.Pb(),
		Origin:   origin,
	}}})
}

// ------------------------------------------------------------
// SB Instance Events

func SBInitEvent() *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_Init{Init: &isspb.SBInit{}}}
}

func SBTickEvent() *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_Tick{Tick: &isspb.SBTick{}}}
}

func SBDeliverEvent(sn t.SeqNr, batch *requestpb.Batch, aborted bool) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_Deliver{
		Deliver: &isspb.SBDeliver{
			Sn:      sn.Pb(),
			Batch:   batch,
			Aborted: aborted,
		},
	}}
}

func SBMessageReceivedEvent(message *isspb.SBInstanceMessage, from t.NodeID) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_MessageReceived{
		MessageReceived: &isspb.SBMessageReceived{
			From: from.Pb(),
			Msg:  message,
		},
	}}
}

func SBPendingRequestsEvent(numRequests t.NumRequests) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_PendingRequests{
		PendingRequests: &isspb.SBPendingRequests{
			NumRequests: numRequests.Pb(),
		},
	}}
}

func SBCutBatchEvent(maxSize t.NumRequests) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_CutBatch{CutBatch: &isspb.SBCutBatch{
		MaxSize: maxSize.Pb(),
	}}}
}

func SBBatchReadyEvent(batch *requestpb.Batch, pendingReqsLeft t.NumRequests) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_BatchReady{BatchReady: &isspb.SBBatchReady{
		Batch:               batch,
		PendingRequestsLeft: pendingReqsLeft.Pb(),
	}}}
}

func SBWaitForRequestsEvent(reference *isspb.SBReqWaitReference, requests []*requestpb.RequestRef) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_WaitForRequests{
		WaitForRequests: &isspb.SBWaitForRequests{
			Reference: reference,
			Requests:  requests,
		},
	}}
}

func SBRequestsReady(ref *isspb.SBReqWaitReference) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_RequestsReady{RequestsReady: &isspb.SBRequestsReady{
		Ref: ref,
	}}}
}

func SBHashResultEvent(digest []byte, origin *isspb.SBInstanceHashOrigin) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_HashResult{HashResult: &isspb.SBHashResult{
		Digest: digest,
		Origin: origin,
	}}}
}

// ============================================================
// Messages
// ============================================================

func Message(msg *isspb.ISSMessage) *messagepb.Message {
	return &messagepb.Message{Type: &messagepb.Message_Iss{Iss: msg}}
}

func SBMessage(epoch t.EpochNr, instance t.SBInstanceID, msg *isspb.SBInstanceMessage) *messagepb.Message {
	return Message(&isspb.ISSMessage{Type: &isspb.ISSMessage_Sb{Sb: &isspb.SBMessage{
		Epoch:    epoch.Pb(),
		Instance: instance.Pb(),
		Msg:      msg,
	}}})
}

func CheckpointMessage(epoch t.EpochNr, sn t.SeqNr) *messagepb.Message {
	return Message(&isspb.ISSMessage{Type: &isspb.ISSMessage_Checkpoint{Checkpoint: &isspb.Checkpoint{
		Epoch: epoch.Pb(),
		Sn:    sn.Pb(),
	}}})
}

func RetransmitRequestsMessage(requests []*requestpb.RequestRef) *messagepb.Message {
	return Message(&isspb.ISSMessage{Type: &isspb.ISSMessage_RetransmitRequests{
		RetransmitRequests: &isspb.RetransmitRequests{
			Requests: requests,
		},
	}})
}
