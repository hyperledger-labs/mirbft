/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type RequestStore interface {
	GetAllocation(clientID, reqNo uint64) ([]byte, error)
	PutAllocation(clientID, reqNo uint64, digest []byte) error
	GetRequest(requestAck *pb.RequestAck) ([]byte, error)
	PutRequest(requestAck *pb.RequestAck, data []byte) error
	Sync() error
}

// ClientProcessor is the client half of the processor components.
// It accepts client related actions from the state machine and injects
// new client requests.
type ClientProcessor struct {
	Node         *Node
	RequestStore RequestStore
}

func (cp *ClientProcessor) Process(ca *ClientActions) {
	for _, r := range ca.AllocatedRequests {
		digest, err := cp.RequestStore.GetAllocation(r.ClientID, r.ReqNo)
		if err != nil {
			panic(fmt.Sprintf("could not get key for %d.%d: %s", r.ClientID, r.ReqNo, err))
		}

		if digest != nil {
			if err := cp.Node.Propose(context.Background(), r, digest); err != nil {
				panic(err)
			}
			continue
		}

		// TODO, do something like
		//   p.RequestStore.PutLocalRequest(r.ClientID, r.ReqNo, nil)
		// with a client lock held, that way when the client goes to propose this
		// sequence it will find that it's been allocated, then continue
		// for now to sort of not break the tests, we're automatically generating
		// client requests.
	}

	if err := cp.RequestStore.Sync(); err != nil {
		panic(fmt.Sprintf("could not sync request store, unsafe to continue: %s\n", err))
	}

	// XXX address
	/*
	   for _, r := range actions.ForwardRequests {
	           requestData, err := p.RequestStore.Get(r.RequestAck)
	           if err != nil {
	                   panic(fmt.Sprintf("could not store request, unsafe to continue: %s\n", err))
	           }

	           fr := &pb.Msg{
	                   Type: &pb.Msg_ForwardRequest{
	                           &pb.ForwardRequest{
	                                   RequestAck:  r.RequestAck,
	                                   RequestData: requestData,
	                           },
	                   },
	           }
	           for _, replica := range r.Targets {
	                   if replica == p.Node.Config.ID {
	                           p.Node.Step(context.Background(), replica, fr)
	                   } else {
	                           p.Link.Send(replica, fr)
	                   }
	           }
	   }
	*/
}
