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

type ActionList struct {
	list *list.List
}

func (a *ActionList) Iterator() *ActionListIterator {
	if a.list == nil {
		return &ActionListIterator{}
	}

	return &ActionListIterator{
		currentElement: a.list.Front(),
	}
}

func (a *ActionList) PushBack(action *state.Action) {
	if a.list == nil {
		a.list = list.New()
	}

	a.list.PushBack(action)
}

func (a *ActionList) PushBackList(actionList *ActionList) {
	a.concat(actionList)
}

func (a *ActionList) Len() int {
	if a.list == nil {
		return 0
	}
	return a.list.Len()
}

func (a *ActionList) send(targets []uint64, msg *msgs.Msg) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_Send{
			Send: &state.ActionSend{
				Targets: targets,
				Msg:     msg,
			},
		},
	})

	return a
}

func (a *ActionList) allocateRequest(clientID, reqNo uint64) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_AllocatedRequest{
			AllocatedRequest: &state.ActionRequestSlot{
				ClientId: clientID,
				ReqNo:    reqNo,
			},
		},
	})
	return a
}

func (a *ActionList) forwardRequest(targets []uint64, requestAck *msgs.RequestAck) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_ForwardRequest{
			ForwardRequest: &state.ActionForward{
				Targets: targets,
				Ack:     requestAck,
			},
		},
	})
	return a
}

func (a *ActionList) truncate(index uint64) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_WriteAhead{
			WriteAhead: &state.ActionWrite{
				Truncate: index,
			},
		},
	})
	return a
}

func (a *ActionList) persist(index uint64, p *msgs.Persistent) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_WriteAhead{
			WriteAhead: &state.ActionWrite{
				Append: index,
				Data:   p,
			},
		},
	})
	return a
}

func (a *ActionList) commit(qEntry *msgs.QEntry) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_Commit{
			Commit: &state.ActionCommit{
				Batch: qEntry,
			},
		},
	})
	return a
}
func (a *ActionList) checkpoint(seqNo uint64, networkConfig *msgs.NetworkState_Config, clientStates []*msgs.NetworkState_Client) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_Commit{
			Commit: &state.ActionCommit{
				SeqNo:         seqNo,
				NetworkConfig: networkConfig,
				ClientStates:  clientStates,
			},
		},
	})
	return a
}

func (a *ActionList) correctRequest(ack *msgs.RequestAck) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_CorrectRequest{
			CorrectRequest: ack,
		},
	})
	return a
}

func (a *ActionList) hash(data [][]byte, origin *state.HashResult) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_Hash{
			Hash: &state.ActionHashRequest{
				Data:   data,
				Origin: origin,
			},
		},
	})
	return a
}

func (a *ActionList) stateTransfer(seqNo uint64, value []byte) *ActionList {
	a.PushBack(&state.Action{
		Type: &state.Action_StateTransfer{
			StateTransfer: &state.ActionStateTarget{
				SeqNo: seqNo,
				Value: value,
			},
		},
	})

	return a
}

func (a *ActionList) isEmpty() bool {
	return a.list == nil || a.list.Len() == 0
}
func (a *ActionList) concat(o *ActionList) *ActionList {
	if o.list != nil {
		if a.list == nil {
			a.list = list.New()
		}
		a.list.PushBackList(o.list)
	}
	return a
}

type ActionListIterator struct {
	currentElement *list.Element
}

// Next will return the next value until the end of the list is encountered.
// Thereafter, it will return nil.
func (ali *ActionListIterator) Next() *state.Action {
	if ali.currentElement == nil {
		return nil
	}

	result := ali.currentElement.Value.(*state.Action)
	ali.currentElement = ali.currentElement.Next()

	return result
}
