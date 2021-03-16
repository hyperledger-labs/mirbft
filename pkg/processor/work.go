/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package processor

import (
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
)

type WorkItems struct {
	walActions     *statemachine.ActionList
	netActions     *statemachine.ActionList
	hashActions    *statemachine.ActionList
	clientActions  *statemachine.ActionList
	appActions     *statemachine.ActionList
	reqStoreEvents *statemachine.EventList
	resultEvents   *statemachine.EventList
}

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

func (pi *WorkItems) WALActions() *statemachine.ActionList {
	result := pi.walActions
	pi.walActions = &statemachine.ActionList{}
	return result
}

func (pi *WorkItems) NetActions() *statemachine.ActionList {
	result := pi.netActions
	pi.netActions = &statemachine.ActionList{}
	return result
}

func (pi *WorkItems) HashActions() *statemachine.ActionList {
	result := pi.hashActions
	pi.hashActions = &statemachine.ActionList{}
	return result
}

func (pi *WorkItems) ClientActions() *statemachine.ActionList {
	result := pi.clientActions
	pi.clientActions = &statemachine.ActionList{}
	return result
}

func (pi *WorkItems) AppActions() *statemachine.ActionList {
	result := pi.appActions
	pi.appActions = &statemachine.ActionList{}
	return result
}

func (pi *WorkItems) ReqStoreEvents() *statemachine.EventList {
	result := pi.reqStoreEvents
	pi.reqStoreEvents = &statemachine.EventList{}
	return result
}

func (pi *WorkItems) ResultEvents() *statemachine.EventList {
	result := pi.resultEvents
	pi.resultEvents = &statemachine.EventList{}
	return result
}

func (pi *WorkItems) HasWALActions() bool {
	return pi.walActions.Len() > 0
}

func (pi *WorkItems) HasNetActions() bool {
	return pi.netActions.Len() > 0
}

func (pi *WorkItems) HasHashActions() bool {
	return pi.hashActions.Len() > 0
}

func (pi *WorkItems) HasClientActions() bool {
	return pi.clientActions.Len() > 0
}

func (pi *WorkItems) HasAppActions() bool {
	return pi.appActions.Len() > 0
}

func (pi *WorkItems) HasReqStoreEvents() bool {
	return pi.reqStoreEvents.Len() > 0
}

func (pi *WorkItems) HasResultEvents() bool {
	return pi.resultEvents.Len() > 0
}

func (pi *WorkItems) AddWALActions(actions *statemachine.ActionList) {
	pi.walActions.PushBackList(actions)
}

func (pi *WorkItems) AddNetActions(actions *statemachine.ActionList) {
	pi.netActions.PushBackList(actions)
}

func (pi *WorkItems) AddHashActions(actions *statemachine.ActionList) {
	pi.hashActions.PushBackList(actions)
}

func (pi *WorkItems) AddClientActions(actions *statemachine.ActionList) {
	pi.clientActions.PushBackList(actions)
}

func (pi *WorkItems) AddAppActions(actions *statemachine.ActionList) {
	pi.appActions.PushBackList(actions)
}

func (pi *WorkItems) AddReqStoreEvents(events *statemachine.EventList) {
	pi.reqStoreEvents.PushBackList(events)
}

func (pi *WorkItems) AddResultEvents(events *statemachine.EventList) {
	pi.resultEvents.PushBackList(events)
}

func (pi *WorkItems) AddWALAction(action *state.Action) {
	pi.walActions.PushBack(action)
}

func (pi *WorkItems) AddNetAction(action *state.Action) {
	pi.netActions.PushBack(action)
}

func (pi *WorkItems) AddHashAction(action *state.Action) {
	pi.hashActions.PushBack(action)
}

func (pi *WorkItems) AddClientAction(action *state.Action) {
	pi.clientActions.PushBack(action)
}

func (pi *WorkItems) AddAppAction(action *state.Action) {
	pi.appActions.PushBack(action)
}

func (pi *WorkItems) AddReqStoreEvent(event *state.Event) {
	pi.reqStoreEvents.PushBack(event)
}

func (pi *WorkItems) AddResultEvent(event *state.Event) {
	pi.resultEvents.PushBack(event)
}

func (pi *WorkItems) AddStateMachineActions(actions *statemachine.ActionList) {
	// First we'll handle everything that's not a network send
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
				pi.walActions.PushBack(action)
			} else {
				pi.netActions.PushBack(action)
			}
		case *state.Action_Hash:
			pi.hashActions.PushBack(action)
		case *state.Action_AppendWriteAhead:
			pi.walActions.PushBack(action)
		case *state.Action_TruncateWriteAhead:
			pi.walActions.PushBack(action)
		case *state.Action_Commit:
			pi.appActions.PushBack(action)
		case *state.Action_Checkpoint:
			pi.appActions.PushBack(action)
		case *state.Action_AllocatedRequest:
			pi.clientActions.PushBack(action)
		case *state.Action_CorrectRequest:
			pi.clientActions.PushBack(action)
		case *state.Action_StateApplied:
			// TODO, handle
		case *state.Action_ForwardRequest:
			// XXX address
		case *state.Action_StateTransfer:
			pi.appActions.PushBack(action)
		}
	}
}
