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

func SBEvent(epoch t.EpochNr, instance t.SBInstanceID, event *isspb.SBInstanceEvent) *eventpb.Event {
	return Event(&isspb.ISSEvent{Type: &isspb.ISSEvent_Sb{Sb: &isspb.SBEvent{
		Epoch:    epoch.Pb(),
		Instance: instance.Pb(),
		Event:    event,
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

func SBDeliverEvent(sn t.SeqNr, batch *requestpb.Batch) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_Deliver{
		Deliver: &isspb.SBDeliver{
			Sn:    sn.Pb(),
			Batch: batch,
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

func SBWaitForRequestsEvent(sn t.SeqNr, requests []*requestpb.RequestRef) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_WaitForRequests{
		WaitForRequests: &isspb.SBWaitForRequests{
			Sn:       sn.Pb(),
			Requests: requests,
		},
	}}
}

func SBRequestsReady(sn t.SeqNr) *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_RequestsReady{RequestsReady: &isspb.SBRequestsReady{
		Sn: sn.Pb(),
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

func RetransmitRequestsMessage(requests []*requestpb.RequestRef) *messagepb.Message {
	return Message(&isspb.ISSMessage{Type: &isspb.ISSMessage_RetransmitRequests{
		RetransmitRequests: &isspb.RetransmitRequests{
			Requests: requests,
		},
	}})
}
