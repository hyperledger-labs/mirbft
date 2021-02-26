/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
)

func toActions(a *statemachine.ActionList) (*statemachine.ActionList, *ClientActions) {
	iter := a.Iterator()
	aResult := &statemachine.ActionList{}
	caResult := &ClientActions{}
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			aResult.PushBack(action)
		case *state.Action_Hash:
			aResult.PushBack(action)
		case *state.Action_TruncateWriteAhead:
			aResult.PushBack(action)
		case *state.Action_AppendWriteAhead:
			aResult.PushBack(action)
		case *state.Action_AllocatedRequest:
			caResult.AllocatedRequests = append(caResult.AllocatedRequests, RequestSlot{
				ClientID: t.AllocatedRequest.ClientId,
				ReqNo:    t.AllocatedRequest.ReqNo,
			})
		case *state.Action_CorrectRequest:
			caResult.CorrectRequests = append(caResult.CorrectRequests, t.CorrectRequest)
		case *state.Action_ForwardRequest:
			caResult.ForwardRequests = append(caResult.ForwardRequests, Forward{
				Targets:    t.ForwardRequest.Targets,
				RequestAck: t.ForwardRequest.Ack,
			})
		case *state.Action_Commit:
			aResult.PushBack(action)
		case *state.Action_Checkpoint:
			aResult.PushBack(action)
		case *state.Action_StateTransfer:
			aResult.PushBack(action)
		default:
			panic(fmt.Sprintf("unhandled type: %T", t))
		}
	}

	return aResult, caResult
}

type ClientActions struct {
	// AllocatedRequests is a set of client request numbers which are eligible for
	// clients to begin filling.  It is the responsibility of the consumer to ensure
	// that a client request has been allocated for a particular client's request number,
	// then it must validate the request, and finally pass the allocation information
	// along with a hash of the request back into the state machine via the Propose API.
	AllocatedRequests []RequestSlot

	// CorrectRequests is a list of requests and their identifying digests which are known
	// to be correct and should be stored if forwarded.  As a special case, if the request is
	// the null request (the digest is nil) then the consumer need not way for the request
	// to be forwarded and should immediately store a null request.
	CorrectRequests []*msgs.RequestAck

	// ForwardRequest is a list of requests which must be sent to another replica in the
	// network.  and their destinations.
	ForwardRequests []Forward
}

// clear nils out all of the fields.
func (ca *ClientActions) clear() {
	ca.AllocatedRequests = nil
	ca.CorrectRequests = nil
	ca.ForwardRequests = nil
}

func (ca *ClientActions) isEmpty() bool {
	return len(ca.AllocatedRequests) == 0 &&
		len(ca.CorrectRequests) == 0 &&
		len(ca.ForwardRequests) == 0
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (ca *ClientActions) concat(o *ClientActions) *ClientActions {
	ca.AllocatedRequests = append(ca.AllocatedRequests, o.AllocatedRequests...)
	ca.CorrectRequests = append(ca.CorrectRequests, o.CorrectRequests...)
	ca.ForwardRequests = append(ca.ForwardRequests, o.ForwardRequests...)
	return ca
}

type ClientActionResults struct {
	// PersistedRequests should be the set of new client requests that
	// have been persisted (because they are are known to be correct).
	PersistedRequests []*msgs.RequestAck
}

func (car *ClientActionResults) persisted(ack *msgs.RequestAck) *ClientActionResults {
	car.PersistedRequests = append(car.PersistedRequests, ack)
	return car
}

type RequestSlot struct {
	ClientID uint64
	ReqNo    uint64
}

// Forward is an action much like send, but requires the consumer to first
// fetch the desired request and form the message before sending.
type Forward struct {
	Targets    []uint64
	RequestAck *msgs.RequestAck
}
