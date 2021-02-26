/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemachine

import (
	"container/list"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
)

type EventList struct {
	list *list.List
}

func (el *EventList) Iterator() *EventListIterator {
	if el.list == nil {
		return &EventListIterator{}
	}

	return &EventListIterator{
		currentElement: el.list.Front(),
	}
}

func (el *EventList) PushBack(action *state.Event) {
	if el.list == nil {
		el.list = list.New()
	}

	el.list.PushBack(action)
}

func (el *EventList) PushBackList(actionList *EventList) {
	if actionList.list != nil {
		if el.list == nil {
			el.list = list.New()
		}
		el.list.PushBackList(actionList.list)
	}
}

func (el *EventList) Len() int {
	if el.list == nil {
		return 0
	}
	return el.list.Len()
}

func (el *EventList) Initialize(initialParms *state.EventInitialParameters) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_Initialize{
			Initialize: initialParms,
		},
	})

	return el
}

func (el *EventList) LoadPersistedEntry(index uint64, entry *msgs.Persistent) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_LoadPersistedEntry{
			LoadPersistedEntry: &state.EventLoadPersistedEntry{
				Index: index,
				Entry: entry,
			},
		},
	})
	return el
}

func (el *EventList) CompleteInitialization() *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_CompleteInitialization{
			CompleteInitialization: &state.EventLoadCompleted{},
		},
	})
	return el
}

func (el *EventList) HashResult(digest []byte, origin *state.HashOrigin) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_HashResult{
			HashResult: &state.EventHashResult{
				Digest: digest,
				Origin: origin,
			},
		},
	})
	return el
}

func (el *EventList) CheckpointResult(value []byte, pendingReconfigurations []*msgs.Reconfiguration, actionCheckpoint *state.ActionCheckpoint) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_CheckpointResult{
			CheckpointResult: &state.EventCheckpointResult{
				SeqNo: actionCheckpoint.SeqNo,
				Value: value,
				NetworkState: &msgs.NetworkState{
					Config:                  actionCheckpoint.NetworkConfig,
					Clients:                 actionCheckpoint.ClientStates,
					PendingReconfigurations: pendingReconfigurations,
				},
			},
		},
	})
	return el
}

func (el *EventList) RequestPersisted(ack *msgs.RequestAck) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_RequestPersisted{
			RequestPersisted: &state.EventRequestPersisted{
				RequestAck: ack,
			},
		},
	})
	return el
}
func (el *EventList) StateTransferComplete(networkState *msgs.NetworkState, actionStateTransfer *state.ActionStateTarget) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_StateTransferComplete{
			StateTransferComplete: &state.EventStateTransferComplete{
				SeqNo:           actionStateTransfer.SeqNo,
				CheckpointValue: actionStateTransfer.Value,
				NetworkState:    networkState,
			},
		},
	})
	return el
}

func (el *EventList) StateTransferFailed(actionStateTransfer *state.ActionStateTarget) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_StateTransferFailed{
			StateTransferFailed: &state.EventStateTransferFailed{
				SeqNo:           actionStateTransfer.SeqNo,
				CheckpointValue: actionStateTransfer.Value,
			},
		},
	})
	return el
}

func (el *EventList) Step(source uint64, msg *msgs.Msg) *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_Step{
			Step: &state.EventStep{
				Source: source,
				Msg:    msg,
			},
		},
	})
	return el
}

func (el *EventList) TickElapsed() *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_TickElapsed{
			TickElapsed: &state.EventTickElapsed{},
		},
	})

	return el
}

func (el *EventList) ActionsReceived() *EventList {
	el.PushBack(&state.Event{
		Type: &state.Event_ActionsReceived{
			ActionsReceived: &state.EventActionsReceived{},
		},
	})

	return el
}

type EventListIterator struct {
	currentElement *list.Element
}

// Next will return the next value until the end of the list is encountered.
// Thereafter, it will return nil.
func (ali *EventListIterator) Next() *state.Event {
	if ali.currentElement == nil {
		return nil
	}

	result := ali.currentElement.Value.(*state.Event)
	ali.currentElement = ali.currentElement.Next()

	return result
}
