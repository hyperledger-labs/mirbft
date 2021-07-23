/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
)

// WorkItems is a buffer for storing outstanding events that need to be processed by the node.
// It contains a separate list for each type of event.
type WorkItems struct {
	wal          *statemachine.EventList
	net          *statemachine.EventList
	hash         *statemachine.EventList
	client       *statemachine.EventList
	app          *statemachine.EventList
	reqStore     *statemachine.EventList
	stateMachine *statemachine.EventList
}

// NewWorkItems allocates and returns a pointer to a new WorkItems object.
func NewWorkItems() *WorkItems {
	return &WorkItems{
		wal:          &statemachine.EventList{},
		net:          &statemachine.EventList{},
		hash:         &statemachine.EventList{},
		client:       &statemachine.EventList{},
		app:          &statemachine.EventList{},
		reqStore:     &statemachine.EventList{},
		stateMachine: &statemachine.EventList{},
	}
}

// AddEvents adds events produced by modules to the WorkItems buffer.
// According to their types, the events are distributed to the appropriate internal sub-buffers.
func (wi *WorkItems) AddEvents(events *statemachine.EventList) {
	iter := events.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch t := event.Type.(type) {
		case *state.Event_HashResult:
			wi.StateMachine().PushBack(event)
		case *state.Event_TickElapsed:
			wi.StateMachine().PushBack(event)
			// TODO: Should the TickElapsed event also go elsewhere?
		default:
			panic(fmt.Sprintf("unknown event type %T", t))
		}
	}
}

// Getters.

func (wi *WorkItems) WAL() *statemachine.EventList {
	return wi.wal
}

func (wi *WorkItems) Net() *statemachine.EventList {
	return wi.net
}

func (wi *WorkItems) Hash() *statemachine.EventList {
	return wi.hash
}

func (wi *WorkItems) Client() *statemachine.EventList {
	return wi.client
}

func (wi *WorkItems) App() *statemachine.EventList {
	return wi.app
}

func (wi *WorkItems) ReqStore() *statemachine.EventList {
	return wi.reqStore
}

func (wi *WorkItems) StateMachine() *statemachine.EventList {
	return wi.stateMachine
}

// Methods for clearing the buffers.

func (wi *WorkItems) ClearWAL() {
	wi.wal = &statemachine.EventList{}
}

func (wi *WorkItems) ClearNet() {
	wi.net = &statemachine.EventList{}
}

func (wi *WorkItems) ClearHash() {
	wi.hash = &statemachine.EventList{}
}

func (wi *WorkItems) ClearClient() {
	wi.client = &statemachine.EventList{}
}

func (wi *WorkItems) ClearApp() {
	wi.app = &statemachine.EventList{}
}

func (wi *WorkItems) ClearReqStore() {
	wi.reqStore = &statemachine.EventList{}
}

func (wi *WorkItems) ClearStateMachine() {
	wi.stateMachine = &statemachine.EventList{}
}
