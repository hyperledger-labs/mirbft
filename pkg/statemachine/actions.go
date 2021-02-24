/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemachine

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

type actionSet struct {
	pb.StateEventResult
}

func (a *actionSet) send(targets []uint64, msg *pb.Msg) *actionSet {
	a.Send = append(a.Send, &pb.StateEventResult_Send{
		Targets: targets,
		Msg:     msg,
	})

	return a
}

func (a *actionSet) allocateRequest(clientID, reqNo uint64) *actionSet {
	a.AllocatedRequests = append(a.AllocatedRequests, &pb.StateEventResult_RequestSlot{ClientId: clientID, ReqNo: reqNo})
	return a
}

func (a *actionSet) forwardRequest(targets []uint64, requestAck *pb.RequestAck) *actionSet {
	a.ForwardRequests = append(a.ForwardRequests, &pb.StateEventResult_Forward{
		Targets: targets,
		Ack:     requestAck,
	})
	return a
}

func (a *actionSet) persist(index uint64, p *pb.Persistent) *actionSet {
	a.WriteAhead = append(a.WriteAhead,
		&pb.StateEventResult_Write{
			Append: index,
			Data:   p,
		})
	return a
}

func (a *actionSet) correctRequest(ack *pb.RequestAck) *actionSet {
	a.CorrectRequests = append(a.CorrectRequests, ack)
	return a
}

func (a *actionSet) isEmpty() bool {
	return len(a.Send) == 0 &&
		len(a.Hash) == 0 &&
		len(a.WriteAhead) == 0 &&
		len(a.AllocatedRequests) == 0 &&
		len(a.ForwardRequests) == 0 &&
		len(a.CorrectRequests) == 0 &&
		len(a.Commits) == 0 &&
		a.StateTransfer == nil
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *actionSet) concat(o *actionSet) *actionSet {
	a.Send = append(a.Send, o.Send...)
	a.Commits = append(a.Commits, o.Commits...)
	a.Hash = append(a.Hash, o.Hash...)
	a.WriteAhead = append(a.WriteAhead, o.WriteAhead...)
	a.AllocatedRequests = append(a.AllocatedRequests, o.AllocatedRequests...)
	a.CorrectRequests = append(a.CorrectRequests, o.CorrectRequests...)
	a.ForwardRequests = append(a.ForwardRequests, o.ForwardRequests...)
	if o.StateTransfer != nil {
		if a.StateTransfer != nil {
			panic("attempted to concatenate two concurrent state transfer requests")
		}
		a.StateTransfer = o.StateTransfer
	}
	return a
}
