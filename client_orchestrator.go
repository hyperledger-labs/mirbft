/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"sync"

	pb "github.com/IBM/mirbft/mirbftpb"
)

// ClientOrchestrator is designed to help assist applications injecting new
// client requests into the system.  It is designed with most use cases in mind, but it
// is similarly possible for an application to implement its own version.
type ClientOrchestrator struct {
	mutex           sync.RWMutex
	clientProposers map[uint64]*ClientProposer
	node            *Node
}

func NewClientOrchestrator(node *Node, initialState *pb.NetworkState, reqStore RequestStore) (*ClientOrchestrator, error) {
	clientProposers := map[uint64]*ClientProposer{}
	for _, clientState := range initialState.Clients {
		clientProposers[clientState.Id] = newClientProposer(clientState)
	}

	err := reqStore.Uncommitted(func(ack *pb.RequestAck) {
		err := node.Propose(context.Background(), ack)
		clientProposer, ok := clientProposers[ack.ClientId]
		if !ok {
			// Odd, but perhaps some reconfiguration has not
			// yet taken effect, hang onto it.
			clientProposer = newClientProposer(&pb.NetworkState_Client{})
			clientProposers[ack.ClientId] = clientProposer
		}
		clientProposer.outstandingReqs[ack.ReqNo] = ack
	})

	if err != nil {
		return nil, err
	}

	return &ClientOrchestrator{
		clientProposers: clientProposers,
		activeState:     initialState,
		node:            node,
	}, nil
}

// UpdateNetworkState must be called each time after a new NetworkState commits.
// inside the state machine.  A new network state may commit either when the application
// log applies an action result containing a checkpoint (with a new network state),
// or when the application informs the state machine of a successful state transfer.
func (co *ClientOrchestrator) UpdateNetworkState(ns *pb.NetworkState) {
	co.mutex.Lock()
	defer co.mutex.Unlock()
	for _, clientState := range ns.Clients {
		clientProposer, ok := clientProposers[ack.ClientId]
		if ok {
			clientProposer.updateState(clientState)
		} else {
			// new client
			clientProposers[ack.ClientId] = newClientProposer(clientState)
		}
	}
}

// ClientProposer returns an instance of the client proposer for a given client ID.
// If no such client ID exists, nil is returned.
func (co *ClientOrchestrator) ClientProposer(clientID uint64) *ClientProposer {
	co.mutex.RLock()
	defer co.mutex.RUnlock()
	cp, ok := clientProposers[clientID]
	if !ok {
		return nil
	}
	return cp
}

type ClientProposer struct {
	mutex           sync.Mutex
	id              uint64
	lowWatermark    uint64
	nextReqNo       uint64
	availableWidth  uint64
	outstandingReqs map[uint64]*pb.RequestAck
}

func newClientProposer(clientState *pb.NetworkState_Client) *ClientProposer {
	cp := &ClientProposer{
		id:              clientState.Id,
		outstandingReqs: map[uint64]*pb.RequestAck{},
	}
	cp.updateState(clientState)
	return cp
}

func (cp *ClientProposer) updateState(newState *pb.NetworkState_Client) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	maxReqNoInCP := cp.lowWatermark + cp.availableWidth
	for reqNo := cp.lowWatermark; reqNo <= maxReqNoInCP && reqNo < newState.lowWatermark; reqNo++ {
		delete(cp.outstandingReqs, reqNo)
	}

	cp.lowWatermark = newState.lowWatermark
	if cp.nextReqNo < cp.lowWatermark {
		cp.nextReqNo = newState.lowWatermark
	}
	cp.availableWidth = newState.Width - newState.WidthConsumedLastCheckpoint
}

func (cp *ClientProposer) NextReq(ctx context.Context) (reqNo uint64, propFunc func(ctx context.Context, data []byte) error) {
	cp.mutex.Lock()
	reqNo := cp.nextReqNo
	cp.mutex.Unlock()

	// IDEA -- why don't we have a go routine per client (or a shared one?)
	// which is responsible for shoving available proposal functions into a
	// buffered channel -- this NextReq guy will read off of this channel,
	// until it unblocks, or the context cancels.

	for {
		cp.nextReqNo++
	}

	// TODO, we need a shared/synchronizing pool to ensure that hashing is done in parallel
	// but that writes to store to the db and inject into the state machine are done
	// serially.

	return nextReqNo, func(ctx context.Context, data []byte) error {
		// TODO, share the hash pool with processor
		hash := Hasher()
		hash.Write(uint64ToBytes(cp.id))
		hash.Write(uint64ToBytes(nextReqNo))
		hash.Write(data)
		digest := hash.Sum(nil)

		cp.mutex.Lock()
		defer cp.mutex.Unlock()
		if cp.lowWatermark > reqNo {
			// this reqno was committed while we waited
			return nil
		}

		return cp.node.Propose(ctx, &pb.RequestAck{
			ClientId: cp.id,
			ReqNo:    nextReqNo,
			Digest:   digest,
		})
	}
}

func (cp *ClientProposer) Propose(reqNo uint64, msg []byte) error {

}
