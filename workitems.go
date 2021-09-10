/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
)

// WorkItems is a buffer for storing outstanding events that need to be processed by the node.
// It contains a separate list for each type of event.
type WorkItems struct {
	wal      *events.EventList
	net      *events.EventList
	hash     *events.EventList
	client   *events.EventList
	app      *events.EventList
	reqStore *events.EventList
	protocol *events.EventList
}

// NewWorkItems allocates and returns a pointer to a new WorkItems object.
func NewWorkItems() *WorkItems {
	return &WorkItems{
		wal:      &events.EventList{},
		net:      &events.EventList{},
		hash:     &events.EventList{},
		client:   &events.EventList{},
		app:      &events.EventList{},
		reqStore: &events.EventList{},
		protocol: &events.EventList{},
	}
}

// AddEvents adds events produced by modules to the WorkItems buffer.
// According to their types, the events are distributed to the appropriate internal sub-buffers.
// When AddEvents returns a non-nil error, any subset of the events may have been added.
func (wi *WorkItems) AddEvents(events *events.EventList) error {
	iter := events.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch t := event.Type.(type) {
		case *eventpb.Event_SendMessage:
			wi.net.PushBack(event)
		case *eventpb.Event_MessageReceived:
			wi.protocol.PushBack(event)
		case *eventpb.Event_Request:
			wi.client.PushBack(event)
		case *eventpb.Event_HashRequest:
			wi.hash.PushBack(event)
		case *eventpb.Event_HashResult:
			// For hash results, their origin determines the destination.
			switch t.HashResult.Origin.Type.(type) {
			case *eventpb.HashOrigin_Request:
				// If the origin is a request received directly from a client,
				// it is the client tracker that created the request and the result goes back to it.
				wi.client.PushBack(event)
			}
		case *eventpb.Event_Tick:
			wi.protocol.PushBack(event)
			// TODO: Should the Tick event also go elsewhere? Clients?
		case *eventpb.Event_RequestReady:
			wi.protocol.PushBack(event)
		case *eventpb.Event_WalEntry:
			switch walEntry := t.WalEntry.Event.Type.(type) {
			case *eventpb.Event_PersistDummyBatch:
				wi.protocol.PushBack(t.WalEntry.Event)
			default:
				return fmt.Errorf("unsupported WAL entry event type %T", walEntry)
			}

		// TODO: Remove these eventually.
		case *eventpb.Event_PersistDummyBatch:
			wi.wal.PushBack(event)
		case *eventpb.Event_AnnounceDummyBatch:
			wi.app.PushBack(event)
		default:
			return fmt.Errorf("cannot add event of unknown type %T", t)
		}
	}
	return nil
}

// Getters.

func (wi *WorkItems) WAL() *events.EventList {
	return wi.wal
}

func (wi *WorkItems) Net() *events.EventList {
	return wi.net
}

func (wi *WorkItems) Hash() *events.EventList {
	return wi.hash
}

func (wi *WorkItems) Client() *events.EventList {
	return wi.client
}

func (wi *WorkItems) App() *events.EventList {
	return wi.app
}

func (wi *WorkItems) ReqStore() *events.EventList {
	return wi.reqStore
}

func (wi *WorkItems) Protocol() *events.EventList {
	return wi.protocol
}

// Methods for clearing the buffers.
// Each of them returns the list of events that have been removed from WorkItems.

func (wi *WorkItems) ClearWAL() *events.EventList {
	return clearEventList(&wi.wal)
}

func (wi *WorkItems) ClearNet() *events.EventList {
	return clearEventList(&wi.net)
}

func (wi *WorkItems) ClearHash() *events.EventList {
	return clearEventList(&wi.hash)
}

func (wi *WorkItems) ClearClient() *events.EventList {
	return clearEventList(&wi.client)
}

func (wi *WorkItems) ClearApp() *events.EventList {
	return clearEventList(&wi.app)
}

func (wi *WorkItems) ClearReqStore() *events.EventList {
	return clearEventList(&wi.reqStore)
}

func (wi *WorkItems) ClearProtocol() *events.EventList {
	return clearEventList(&wi.protocol)
}

func clearEventList(listPtr **events.EventList) *events.EventList {
	oldList := *listPtr
	*listPtr = &events.EventList{}
	return oldList
}
