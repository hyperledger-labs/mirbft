/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"

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
	clients      map[uint64]*Client
}

func (cp *ClientProcessor) client(clientID uint64) *Client {
	if cp.clients == nil {
		cp.clients = map[uint64]*Client{}
	}

	c, ok := cp.clients[clientID]
	if !ok {
		c = newClient(clientID, cp.RequestStore)
		cp.clients[clientID] = c
	}
	return c
}

func (cp *ClientProcessor) Process(ca *ClientActions) {
	for _, r := range ca.AllocatedRequests {
		client := cp.client(r.ClientID)
		ack, err := client.allocate(r.ReqNo)
		if err != nil {
			panic(err)
		}

		if ack != nil {
			if err := cp.Node.Propose(context.Background(), r, ack.Digest); err != nil {
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

type Client struct {
	mutex        sync.Mutex
	clientID     uint64
	requestStore RequestStore
	requests     *list.List
	reqNoMap     map[uint64]*list.Element
}

func newClient(clientID uint64, reqStore RequestStore) *Client {
	return &Client{
		clientID:     clientID,
		requestStore: reqStore,
		requests:     list.New(),
		reqNoMap:     map[uint64]*list.Element{},
	}
}

type clientRequest struct {
	localAllocationAck *pb.RequestAck
	remoteCorrectAcks  []*pb.RequestAck
}

func (c *Client) allocate(reqNo uint64) (*pb.RequestAck, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	el, ok := c.reqNoMap[reqNo]
	if ok {
		clientReq := el.Value.(*clientRequest)
		return clientReq.localAllocationAck, nil
	}

	cr := &clientRequest{}
	el = c.requests.PushBack(cr)
	c.reqNoMap[reqNo] = el

	digest, err := c.requestStore.GetAllocation(c.clientID, reqNo)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not get key for %d.%d", c.clientID, reqNo)
	}

	cr.localAllocationAck = &pb.RequestAck{
		ClientId: c.clientID,
		ReqNo:    reqNo,
		Digest:   digest,
	}

	return cr.localAllocationAck, nil
}
