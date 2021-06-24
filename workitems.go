/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
)

// WorkItems is a buffer for storing outstanding actions and events that need to be processed by the node.
// It contains a separate list for each type of action / event.
type WorkItems struct {
	walActions     *statemachine.ActionList
	netActions     *statemachine.ActionList
	hashActions    *statemachine.ActionList
	clientActions  *statemachine.ActionList
	appActions     *statemachine.ActionList
	reqStoreEvents *statemachine.EventList // TODO: Check whether the name is appropriate
	resultEvents   *statemachine.EventList // TODO: Rename this
}

// NewWorkItems allocates and returns a pointer to a new WorkItems object.
func NewWorkItems() *WorkItems {
	return &WorkItems{
		walActions:     &statemachine.ActionList{},
		netActions:     &statemachine.ActionList{},
		hashActions:    &statemachine.ActionList{},
		clientActions:  &statemachine.ActionList{},
		appActions:     &statemachine.ActionList{},
		reqStoreEvents: &statemachine.EventList{},
		resultEvents:   &statemachine.EventList{},
	}
}

// AddActions adds actions produced by modules to the WorkItems buffer.
// According to their types, the actions are distributed to the appropriate internal sub-buffers.
func (wi *WorkItems) AddActions(actions *statemachine.ActionList) {
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			walDependent := false
			// TODO, make sure this switch captures all the safe ones
			switch t.Send.Msg.Type.(type) {
			case *msgs.Msg_RequestAck:
			case *msgs.Msg_Checkpoint:
			case *msgs.Msg_FetchBatch:
			case *msgs.Msg_ForwardBatch:
			default:
				walDependent = true
			}
			if walDependent {
				wi.WALActions().PushBack(action)
			} else {
				wi.NetActions().PushBack(action)
			}
		case *state.Action_Hash:
			wi.HashActions().PushBack(action)
		case *state.Action_AppendWriteAhead,
			*state.Action_TruncateWriteAhead:
			wi.WALActions().PushBack(action)
		case *state.Action_Commit,
			*state.Action_Checkpoint,
			*state.Action_StateTransfer:
			wi.AppActions().PushBack(action)
		case *state.Action_AllocatedRequest,
			*state.Action_CorrectRequest,
			*state.Action_StateApplied:
			wi.ClientActions().PushBack(action)
			// TODO, create replicas
		case *state.Action_ForwardRequest:
			// XXX address
		default:
			panic(fmt.Sprintf("unknown event type %T", t))
		}
	}
}

// AddEvents adds events produced by modules to the WorkItems buffer.
// According to their types, the events are distributed to the appropriate internal sub-buffers.
func (wi *WorkItems) AddEvents(events *statemachine.EventList) {
	iter := events.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch t := event.Type.(type) {
		case *state.Event_HashResult:
			wi.ResultEvents().PushBack(event)
		default:
			panic(fmt.Sprintf("unknown event type %T", t))
		}
	}
}

// Getters.

func (wi *WorkItems) WALActions() *statemachine.ActionList {
	return wi.walActions
}

func (wi *WorkItems) NetActions() *statemachine.ActionList {
	return wi.netActions
}

func (wi *WorkItems) HashActions() *statemachine.ActionList {
	return wi.hashActions
}

func (wi *WorkItems) ClientActions() *statemachine.ActionList {
	return wi.clientActions
}

func (wi *WorkItems) AppActions() *statemachine.ActionList {
	return wi.appActions
}

func (wi *WorkItems) ReqStoreEvents() *statemachine.EventList {
	return wi.reqStoreEvents
}

func (wi *WorkItems) ResultEvents() *statemachine.EventList {
	return wi.resultEvents
}

// Methods for clearing the buffers.

func (wi *WorkItems) ClearWALActions() {
	wi.walActions = &statemachine.ActionList{}
}

func (wi *WorkItems) ClearNetActions() {
	wi.netActions = &statemachine.ActionList{}
}

func (wi *WorkItems) ClearHashActions() {
	wi.hashActions = &statemachine.ActionList{}
}

func (wi *WorkItems) ClearClientActions() {
	wi.clientActions = &statemachine.ActionList{}
}

func (wi *WorkItems) ClearAppActions() {
	wi.appActions = &statemachine.ActionList{}
}

func (wi *WorkItems) ClearReqStoreEvents() {
	wi.reqStoreEvents = &statemachine.EventList{}
}

func (wi *WorkItems) ClearResultEvents() {
	wi.resultEvents = &statemachine.EventList{}
}

// Legacy methods for adding items to the buffers.
// TODO: handle all these cases in the AddActions() and AddEvents methods and remove those functions.

func (wi *WorkItems) AddHashResults(events *statemachine.EventList) {
	wi.ResultEvents().PushBackList(events)
}

func (wi *WorkItems) AddNetResults(events *statemachine.EventList) {
	wi.ResultEvents().PushBackList(events)
}

func (wi *WorkItems) AddAppResults(events *statemachine.EventList) {
	wi.ResultEvents().PushBackList(events)
}

func (wi *WorkItems) AddClientResults(events *statemachine.EventList) {
	wi.ReqStoreEvents().PushBackList(events)
}

func (wi *WorkItems) AddWALResults(actions *statemachine.ActionList) {
	wi.NetActions().PushBackList(actions)
}

func (wi *WorkItems) AddReqStoreResults(events *statemachine.EventList) {
	wi.ResultEvents().PushBackList(events)
}
