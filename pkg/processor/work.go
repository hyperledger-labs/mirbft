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
	WALActions     *statemachine.ActionList
	NetActions     *statemachine.ActionList
	HashActions    *statemachine.ActionList
	ClientActions  *statemachine.ActionList
	AppActions     *statemachine.ActionList
	ReqStoreEvents *statemachine.EventList
	ResultEvents   *statemachine.EventList
}

func NewWorkItems() *WorkItems {
	return &WorkItems{
		WALActions:     &statemachine.ActionList{},
		NetActions:     &statemachine.ActionList{},
		HashActions:    &statemachine.ActionList{},
		ClientActions:  &statemachine.ActionList{},
		AppActions:     &statemachine.ActionList{},
		ReqStoreEvents: &statemachine.EventList{},
		ResultEvents:   &statemachine.EventList{},
	}
}

func (pi *WorkItems) AddResultEvents(events *statemachine.EventList) {
	pi.ResultEvents.PushBackList(events)
}

func (pi *WorkItems) AddReqStoreEvents(events *statemachine.EventList) {
	pi.ReqStoreEvents.PushBackList(events)
}

func (pi *WorkItems) AddWALActions(actions *statemachine.ActionList) {
	// We know only network sends are blocked by WAL writes
	pi.NetActions.PushBackList(actions)
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
				pi.WALActions.PushBack(action)
			} else {
				pi.NetActions.PushBack(action)
			}
		case *state.Action_Hash:
			pi.HashActions.PushBack(action)
		case *state.Action_AppendWriteAhead:
			pi.WALActions.PushBack(action)
		case *state.Action_TruncateWriteAhead:
			pi.WALActions.PushBack(action)
		case *state.Action_Commit:
			pi.AppActions.PushBack(action)
		case *state.Action_Checkpoint:
			pi.AppActions.PushBack(action)
		case *state.Action_AllocatedRequest:
			pi.ClientActions.PushBack(action)
		case *state.Action_CorrectRequest:
			pi.ClientActions.PushBack(action)
		case *state.Action_StateApplied:
			// TODO, handle
		case *state.Action_ForwardRequest:
			// XXX address
		case *state.Action_StateTransfer:
			pi.AppActions.PushBack(action)
		}
	}
}
