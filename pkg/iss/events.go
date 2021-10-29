/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

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

func SBTickEvent() *isspb.SBInstanceEvent {
	return &isspb.SBInstanceEvent{Type: &isspb.SBInstanceEvent_Tick{Tick: &isspb.SBTick{}}}
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
