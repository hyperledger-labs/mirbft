/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
)

// WorkItems is a buffer for storing outstanding events that need to be processed by the node.
// It contains a separate list for each type of event.
type WorkItems struct {
	wal          *events.EventList
	net          *events.EventList
	hash         *events.EventList
	client       *events.EventList
	app          *events.EventList
	reqStore     *events.EventList
	stateMachine *events.EventList
}

// NewWorkItems allocates and returns a pointer to a new WorkItems object.
func NewWorkItems() *WorkItems {
	return &WorkItems{
		wal:          &events.EventList{},
		net:          &events.EventList{},
		hash:         &events.EventList{},
		client:       &events.EventList{},
		app:          &events.EventList{},
		reqStore:     &events.EventList{},
		stateMachine: &events.EventList{},
	}
}

// AddEvents adds events produced by modules to the WorkItems buffer.
// According to their types, the events are distributed to the appropriate internal sub-buffers.
// When AddEvents returns a non-nil error, any subset of the events may have been added.
func (wi *WorkItems) AddEvents(events *events.EventList) error {
	iter := events.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch t := event.Type.(type) {
		case *state.Event_SendMessage:
			wi.net.PushBack(event)
		case *state.Event_HashRequest:
			wi.hash.PushBack(event)
		case *state.Event_HashResult:
			// For hash results, their origin determines the destination.
			switch t.HashResult.Origin.Type.(type) {
			case *state.HashOrigin_Request:
				// If the origin is a request received directly from a client,
				// it is the client tracker that created the request and the result goes back to it.
				wi.client.PushBack(event)
			}
		case *state.Event_Tick:
			wi.stateMachine.PushBack(event)
			// TODO: Should the Tick event also go elsewhere? Clients?
		case *state.Event_RequestReady:
			wi.stateMachine.PushBack(event)

		// TODO: Remove these eventually.
		case *state.Event_PersistDummyBatch:
			wi.wal.PushBack(event)
		case *state.Event_AnnounceDummyBatch:
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

func (wi *WorkItems) StateMachine() *events.EventList {
	return wi.stateMachine
}

// Methods for clearing the buffers.

func (wi *WorkItems) ClearWAL() {
	wi.wal = &events.EventList{}
}

func (wi *WorkItems) ClearNet() {
	wi.net = &events.EventList{}
}

func (wi *WorkItems) ClearHash() {
	wi.hash = &events.EventList{}
}

func (wi *WorkItems) ClearClient() {
	wi.client = &events.EventList{}
}

func (wi *WorkItems) ClearApp() {
	wi.app = &events.EventList{}
}

func (wi *WorkItems) ClearReqStore() {
	wi.reqStore = &events.EventList{}
}

func (wi *WorkItems) ClearStateMachine() {
	wi.stateMachine = &events.EventList{}
}
