/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// The sbEventService is an object used by an orderer (an instance of Sequenced Broadcast) to create events.
// It implements a dependency injection where the ISS code is the injector and the orderer is the client.
//
// Normally, ISS maintains multiple orderers, each associated with an epoch and with an instance ID.
// However, the epoch and instance should not interfere with the implementation of the orderer.
// The orderer should work completely independently (and not be aware) of other orderers or the context it is used in,
// in order to be as flexible as possible.
// The ISS implementation, however, must distinguish between events produced by different orderers where necessary
// (e.g., when an orderer requests a new batch, ISS must pass the created batch back to the right orderer),
// while at the same time being transparent to events of no direct concern (e.g. WAL events).
// Intercepting all events produced by all orderers and augmenting them by the orderers' identities
// is impractical and clutters the ISS code.
// Instead, each orderer receives a specialized instance of sbEventService that it must use to create all its events.
//
// The orderer only works with isspb.SBInstanceMessage and isspb.SBInstanceEvent types,
// completely unaffected by Node-level events and messages.
// Through its methods, the sbEventService controls which of these Node-level events an orderer can use and how.
// It thus defines the interface for communication of the orderer with the outside.
type sbEventService struct {
	epoch      t.EpochNr
	instanceID t.SBInstanceID
}

// SendMessage creates an event for sending a message that will be processed
// by the corresponding orderer instance at each of the destination.
func (ec *sbEventService) SendMessage(message *isspb.SBInstanceMessage, destinations []t.NodeID) *eventpb.Event {
	return events.SendMessage(SBMessage(ec.epoch, ec.instanceID, message), destinations)
}

// WALAppend creates an event for appending an isspb.SBInstanceEvent to the WAL.
// On recovery, this event will be fed back to the same orderer instance
// (which, however, must be created during the recovery process).
func (ec *sbEventService) WALAppend(event *isspb.SBInstanceEvent) *eventpb.Event {
	return events.WALAppend(SBEvent(ec.epoch, ec.instanceID, event), t.WALRetIndex(ec.epoch))
}

func (ec *sbEventService) HashRequest(data [][]byte, origin *isspb.SBInstanceHashOrigin) *eventpb.Event {
	return events.HashRequest(data, SBHashOrigin(ec.epoch, ec.instanceID, origin))
}

// SBEvent creates an event to be processed by ISS in association with the orderer that created it (e.g. Deliver).
func (ec *sbEventService) SBEvent(event *isspb.SBInstanceEvent) *eventpb.Event {
	return SBEvent(ec.epoch, ec.instanceID, event)
}
