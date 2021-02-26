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

func (al *ActionList) Iterator() *ActionListIterator {
	if al.list == nil {
		return &ActionListIterator{}
	}

	return &ActionListIterator{
		currentElement: al.list.Front(),
	}
}

func (al *ActionList) PushBack(action *state.Action) {
	if al.list == nil {
		al.list = list.New()
	}

	al.list.PushBack(action)
}

func (al *ActionList) PushBackList(actionList *ActionList) {
	al.concat(actionList)
}

func (al *ActionList) Len() int {
	if al.list == nil {
		return 0
	}
	return al.list.Len()
}

func (al *ActionList) send(targets []uint64, msg *msgs.Msg) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_Send{
			Send: &state.ActionSend{
				Targets: targets,
				Msg:     msg,
			},
		},
	})

	return al
}

func (al *ActionList) allocateRequest(clientID, reqNo uint64) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_AllocatedRequest{
			AllocatedRequest: &state.ActionRequestSlot{
				ClientId: clientID,
				ReqNo:    reqNo,
			},
		},
	})
	return al
}

func (al *ActionList) forwardRequest(targets []uint64, requestAck *msgs.RequestAck) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_ForwardRequest{
			ForwardRequest: &state.ActionForward{
				Targets: targets,
				Ack:     requestAck,
			},
		},
	})
	return al
}

func (al *ActionList) truncate(index uint64) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_TruncateWriteAhead{
			TruncateWriteAhead: &state.ActionTruncate{
				Index: index,
			},
		},
	})
	return al
}

func (al *ActionList) persist(index uint64, p *msgs.Persistent) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_AppendWriteAhead{
			AppendWriteAhead: &state.ActionWrite{
				Index: index,
				Data:  p,
			},
		},
	})
	return al
}

func (al *ActionList) commit(qEntry *msgs.QEntry) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_Commit{
			Commit: &state.ActionCommit{
				Batch: qEntry,
			},
		},
	})
	return al
}
func (al *ActionList) checkpoint(seqNo uint64, networkConfig *msgs.NetworkState_Config, clientStates []*msgs.NetworkState_Client) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_Checkpoint{
			Checkpoint: &state.ActionCheckpoint{
				SeqNo:         seqNo,
				NetworkConfig: networkConfig,
				ClientStates:  clientStates,
			},
		},
	})
	return al
}

func (al *ActionList) correctRequest(ack *msgs.RequestAck) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_CorrectRequest{
			CorrectRequest: ack,
		},
	})
	return al
}

func (al *ActionList) hash(data [][]byte, origin *state.HashOrigin) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_Hash{
			Hash: &state.ActionHashRequest{
				Data:   data,
				Origin: origin,
			},
		},
	})
	return al
}

func (al *ActionList) stateTransfer(seqNo uint64, value []byte) *ActionList {
	al.PushBack(&state.Action{
		Type: &state.Action_StateTransfer{
			StateTransfer: &state.ActionStateTarget{
				SeqNo: seqNo,
				Value: value,
			},
		},
	})

	return al
}

func (al *ActionList) isEmpty() bool {
	return al.list == nil || al.list.Len() == 0
}
func (al *ActionList) concat(o *ActionList) *ActionList {
	if o.list != nil {
		if al.list == nil {
			al.list = list.New()
		}
		al.list.PushBackList(o.list)
	}
	return al
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
