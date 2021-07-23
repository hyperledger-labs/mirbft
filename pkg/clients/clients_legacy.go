/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package clients

import (
	"bytes"
	"container/list"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"sync"

	"github.com/pkg/errors"

	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
)

var ErrClientNotExist error = errors.New("client does not exist")

type Clients struct {
	Hasher       modules.Hasher
	RequestStore modules.RequestStore

	mutex   sync.Mutex
	clients map[uint64]*Client
}

func (cs *Clients) Client(clientID uint64) modules.Client {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if cs.clients == nil {
		cs.clients = map[uint64]*Client{}
	}

	c, ok := cs.clients[clientID]
	if !ok {
		c = newClient(clientID, cs.Hasher, cs.RequestStore)
		cs.clients[clientID] = c
	}
	return c
}

// TODO, client needs to be updated based on the state applied events, to give it a low watermark
// minimally and to clean up the reqNoMap
type Client struct {
	mutex        sync.Mutex
	hasher       modules.Hasher
	clientID     uint64
	nextReqNo    uint64
	requestStore modules.RequestStore
	requests     *list.List
	reqNoMap     map[uint64]*list.Element
}

func newClient(clientID uint64, hasher modules.Hasher, reqStore modules.RequestStore) *Client {
	return &Client{
		clientID:     clientID,
		hasher:       hasher,
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

func (c *Client) StateApplied(state *msgs.NetworkState_Client) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for reqNo, el := range c.reqNoMap {
		if reqNo < state.LowWatermark {
			c.requests.Remove(el)
			delete(c.reqNoMap, reqNo)
		}
	}
	if c.nextReqNo < state.LowWatermark {
		c.nextReqNo = state.LowWatermark
	}
}

func (c *Client) Allocate(reqNo uint64) ([]byte, error) {
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

func (c *Client) AddCorrectDigest(reqNo uint64, digest []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.requests.Len() == 0 {
		return ErrClientNotExist
	}

	el, ok := c.reqNoMap[reqNo]
	if !ok {
		if reqNo < c.requests.Front().Value.(*clientRequest).reqNo {
			return nil
		}
		return errors.Errorf("unallocated client request for req_no=%d marked correct", reqNo)
	}

	clientReq := el.Value.(*clientRequest)
	for _, otherDigest := range clientReq.remoteCorrectDigests {
		if bytes.Equal(digest, otherDigest) {
			return nil
		}
	}

	clientReq.remoteCorrectDigests = append(clientReq.remoteCorrectDigests, digest)

	return nil
}

// TODO: Make sure this function is useful. If not, remove it.
func (c *Client) NextReqNo() (uint64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.requests.Len() == 0 {
		return 0, ErrClientNotExist
	}

	return c.nextReqNo, nil
}

func (c *Client) Propose(reqNo uint64, data []byte) (*statemachine.EventList, error) {
	h := c.hasher.New()
	h.Write(data)
	digest := h.Sum(nil)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.requests.Len() == 0 {
		return nil, ErrClientNotExist
	}

	if reqNo < c.nextReqNo {
		return &statemachine.EventList{}, nil
	}

	if reqNo == c.nextReqNo {
		for {
			c.nextReqNo++
			// Keep incrementing 'nextRequest' until we find one we don't have

			el, ok := c.reqNoMap[c.nextReqNo]
			if !ok {
				break
			}

			if el.Value.(*clientRequest).localAllocationDigest == nil {
				break
			}
		}
	}

	el, ok := c.reqNoMap[reqNo]
	previouslyAllocated := ok
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
			return &statemachine.EventList{}, nil
		}

		return nil, errors.Errorf("cannot store request with digest %x, already stored request with different digest %x", digest, cr.localAllocationDigest)
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
			return nil, errors.New("other known correct digest exist for reqno")
		}
	}

	ack := &msgs.RequestAck{
		ClientId: c.clientID,
		ReqNo:    reqNo,
		Digest:   digest,
	}

	// SWAP THESE to return to original version.
	//err := c.requestStore.PutRequest(ack, data)
	err := c.requestStore.PutRequest(&msgs.RequestRef{
		ClientId: ack.ClientId,
		ReqNo:    ack.ReqNo,
		Digest:   ack.Digest,
	}, data)

	if err != nil {
		return nil, errors.WithMessage(err, "could not store requests")
	}

	err = c.requestStore.PutAllocation(c.clientID, reqNo, digest)
	if err != nil {
		return nil, err
	}
	cr.localAllocationDigest = digest

	if previouslyAllocated {
		return (&statemachine.EventList{}).RequestPersisted(ack), nil
	}

	return &statemachine.EventList{}, nil
}
