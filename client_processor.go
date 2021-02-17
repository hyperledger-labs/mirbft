/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"container/list"
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
	Hasher       Hasher
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

func (cp *ClientProcessor) Process(ca *ClientActions) (*ClientActionResults, error) {
	results := &ClientActionResults{}

	for _, r := range ca.AllocatedRequests {
		client := cp.client(r.ClientID)
		digest, err := client.allocate(r.ReqNo)
		if err != nil {
			return nil, err
		}

		if digest != nil {
			results.persisted(&pb.RequestAck{
				ClientId: r.ClientID,
				ReqNo:    r.ReqNo,
				Digest:   digest,
			})
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
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
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

	return results, nil
}

type Client struct {
	mutex        sync.Mutex
	hasher       Hasher
	clientID     uint64
	requestStore RequestStore
	requests     *list.List
	reqNoMap     map[uint64]*list.Element
	nextReqNo    uint64
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
	reqNo                 uint64
	localAllocationDigest []byte
	remoteCorrectDigests  [][]byte
}

func (c *Client) allocate(reqNo uint64) ([]byte, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	el, ok := c.reqNoMap[reqNo]
	if ok {
		clientReq := el.Value.(*clientRequest)
		return clientReq.localAllocationDigest, nil
	}

	cr := &clientRequest{
		reqNo: reqNo,
	}
	el = c.requests.PushBack(cr)
	c.reqNoMap[reqNo] = el

	digest, err := c.requestStore.GetAllocation(c.clientID, reqNo)
	if err != nil {
		return nil, errors.WithMessagef(err, "could not get key for %d.%d", c.clientID, reqNo)
	}

	cr.localAllocationDigest = digest

	return digest, nil
}

func (c *Client) NextReqNo() (uint64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.requests.Len() == 0 {
		return 0, errors.New("client does not yet exist")
	}

	return c.nextReqNo, nil
}

func (c *Client) Propose(reqNo uint64, data []byte) error {
	h := c.hasher()
	h.Write(data)
	digest := h.Sum(nil)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.requests.Len() == 0 {
		return errors.New("client does not yet exist") // TODO consolidate not exist err
	}

	if reqNo < c.nextReqNo {
		return nil
	}

	if reqNo > c.nextReqNo {
		return errors.Errorf("client must submit req_no %d next", c.nextReqNo)
	}

	el, ok := c.reqNoMap[reqNo]
	if !ok {
		// TODO, limit the distance ahead a client can allocate?
		el = c.requests.PushBack(&clientRequest{
			reqNo: reqNo,
		})
		c.reqNoMap[reqNo] = el
	}

	cr := el.Value.(*clientRequest)

	if cr.localAllocationDigest != nil {
		if bytes.Equal(cr.localAllocationDigest, digest) {
			return nil
		}

		return errors.Errorf("cannot store request with digest %x, already stored request with different digest %x", digest, cr.localAllocationDigest)
	}

	if len(cr.remoteCorrectDigests) > 0 {
		found := false
		for _, rd := range cr.remoteCorrectDigests {
			if bytes.Equal(rd, digest) {
				found = true
				break
			}
		}

		if !found {
			return errors.New("other known correct digest exist for reqno")
		}
	}

	ack := &pb.RequestAck{
		ClientId: c.clientID,
		ReqNo:    reqNo,
		Digest:   digest,
	}

	err := c.requestStore.PutRequest(ack, data)
	if err != nil {
		return errors.WithMessage(err, "could not store requests")
	}

	if cr.localAllocationDigest == nil {
		err := c.requestStore.PutAllocation(c.clientID, reqNo, digest)
		if err != nil {
			return err
		}
		cr.localAllocationDigest = digest
	}

	return nil
}
