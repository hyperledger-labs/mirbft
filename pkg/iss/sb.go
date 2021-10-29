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
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type sbInstance interface {
	ApplyEvent(event *isspb.SBInstanceEvent) *events.EventList
	Status() *isspb.SBStatus
	Segment() *segment
}

type sbEventCreator struct {
	epoch      t.EpochNr
	instanceID t.SBInstanceID
}

func (ec *sbEventCreator) SendMessage(message *isspb.SBInstanceMessage, destinations []t.NodeID) *eventpb.Event {
	return events.SendMessage(SBMessage(ec.epoch, ec.instanceID, message), destinations)
}

func (ec *sbEventCreator) WALAppend(event *isspb.SBInstanceEvent) *eventpb.Event {
	return events.WALAppend(SBEvent(ec.epoch, ec.instanceID, event), t.WALRetIndex(ec.epoch))
}

func (ec *sbEventCreator) SBEvent(event *isspb.SBInstanceEvent) *eventpb.Event {
	return SBEvent(ec.epoch, ec.instanceID, event)
}

func (iss *ISS) handleSBMessage(message *isspb.SBMessage, from t.NodeID) *events.EventList {
	switch epoch := t.EpochNr(message.Epoch); {
	case epoch > iss.epoch:
		// TODO: Buffer this message
		return &events.EventList{}
	case epoch == iss.epoch:
		return iss.applySBEvent(SBMessageReceivedEvent(message.Msg, from), t.SBInstanceID(message.Instance))
	default: // epoch < iss.epoch:
		// Ignore old messages
		return &events.EventList{}
	}
}

func (iss *ISS) handleSBEvent(event *isspb.SBEvent) *events.EventList {
	switch epoch := t.EpochNr(event.Epoch); {
	case epoch > iss.epoch:
		panic(fmt.Sprintf("trying to handle ISS event (type %T, instance %d) from future epoch: %d",
			event.Event.Type, event.Instance, event.Epoch))
	case epoch == iss.epoch:
		return iss.applySBEvent(event.Event, t.SBInstanceID(event.Instance))
	default: // epoch < iss.epoch:
		iss.logger.Log(logging.LevelDebug, "Ignoring old event.", "epoch", epoch)
		// Ignore old events.
		return &events.EventList{}
	}
}

func (iss *ISS) applySBEvent(event *isspb.SBInstanceEvent, instance t.SBInstanceID) *events.EventList {
	switch e := event.Type.(type) {
	case *isspb.SBInstanceEvent_Deliver:
		return iss.handleSBDeliver(e.Deliver, instance)
	case *isspb.SBInstanceEvent_CutBatch:
		return iss.cutBatch(instance, t.NumRequests(e.CutBatch.MaxSize))
	case *isspb.SBInstanceEvent_WaitForRequests:
		return iss.waitForRequests(instance, t.SeqNr(e.WaitForRequests.Sn), e.WaitForRequests.Requests)
	default:
		if orderer, ok := iss.orderers[instance]; ok {
			return orderer.ApplyEvent(event)
		} else {
			iss.logger.Log(logging.LevelWarn, "Ignoring SB instance event with invalid instance number",
				"instNr", instance, "eventType", fmt.Sprintf("%T", event.Type))
			return &events.EventList{}
		}
	}
}

func (iss *ISS) handleSBDeliver(deliver *isspb.SBDeliver, instance t.SBInstanceID) *events.EventList {
	iss.commitLog[t.SeqNr(deliver.Sn)] = &commitLogEntry{
		Sn:    t.SeqNr(deliver.Sn),
		Batch: deliver.Batch,
		// TODO: Fill the rest of the fields (especially the digest)!
	}
	iss.removeFromBuckets(deliver.Batch.Requests)
	return iss.deliverCommitted()
}
