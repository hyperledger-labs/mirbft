/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

func newOutstandingReqs(clientTracker *clientTracker, networkState *pb.NetworkState) *allOutstandingReqs {
	ao := &allOutstandingReqs{
		numBuckets:          uint64(networkState.Config.NumberOfBuckets),
		buckets:             map[bucketID]*bucketOutstandingReqs{},
		correctRequests:     map[string]*pb.RequestAck{},
		outstandingRequests: map[string]*sequence{},
		availableIterator:   clientTracker.availableList.iterator(),
	}

	numBuckets := int(networkState.Config.NumberOfBuckets)

	for i := bucketID(0); i < bucketID(numBuckets); i++ {
		bo := &bucketOutstandingReqs{
			clients: map[uint64]*clientOutstandingReqs{},
		}
		ao.buckets[i] = bo

		for _, client := range networkState.Clients {
			var skipRequests map[uint64]struct{}
			bm := bitmask(client.CommittedMask)
			var firstUncommitted uint64
			var firstOffset int
			for j := 0; j < numBuckets; j++ {
				reqNo := client.LowWatermark + uint64(j)
				if clientReqToBucket(client.Id, reqNo, networkState.Config) == i {
					firstUncommitted = reqNo
					firstOffset = j
					break
				}
			}

			for bitIndex := firstOffset; bitIndex < bm.bits(); bitIndex += numBuckets {
				if !bm.isBitSet(bitIndex) {
					continue
				}

				reqNo := client.LowWatermark + uint64(bitIndex)
				if reqNo == firstUncommitted {
					firstUncommitted += uint64(numBuckets)
					continue
				}

				if skipRequests == nil {
					skipRequests = map[uint64]struct{}{}
				}

				skipRequests[reqNo] = struct{}{}
			}

			bo.clients[client.Id] = &clientOutstandingReqs{
				nextReqNo:    firstUncommitted,
				skipRequests: skipRequests,
			}
		}
	}

	ao.advanceRequests() // Note, this can return no actions as no sequences have allocated

	return ao
}

type allOutstandingReqs struct {
	numBuckets          uint64
	buckets             map[bucketID]*bucketOutstandingReqs
	availableIterator   *availableIterator
	lastCorrectReq      *list.Element
	correctRequests     map[string]*pb.RequestAck
	outstandingRequests map[string]*sequence
}

type bucketOutstandingReqs struct {
	clients map[uint64]*clientOutstandingReqs // TODO, obvious optimization is to make this active clients and initialize this lazily
}

type clientOutstandingReqs struct {
	nextReqNo    uint64
	skipRequests map[uint64]struct{}
}

func (ao *allOutstandingReqs) advanceRequests() *Actions {
	actions := &Actions{}
	for ao.availableIterator.hasNext() {
		clientRequest := ao.availableIterator.next()
		key := string(clientRequest.ack.Digest)

		if seq, ok := ao.outstandingRequests[key]; ok {
			delete(ao.outstandingRequests, key)
			actions.concat(seq.satisfyOutstanding(clientRequest.ack))
			continue
		}

		ao.correctRequests[key] = clientRequest.ack
	}

	return actions
}

func (ao *allOutstandingReqs) applyBatch(bucket bucketID, batch []*pb.RequestAck) error {
	bo, ok := ao.buckets[bucket]
	if !ok {
		panic("dev sanity test")
	}

	for _, req := range batch {
		co, ok := bo.clients[req.ClientId]
		if !ok {
			return fmt.Errorf("no such client")
		}

		if co.nextReqNo != req.ReqNo {
			return fmt.Errorf("expected ClientId=%d next request for Bucket=%d to have ReqNo=%d but got ReqNo=%d", req.ClientId, bucket, co.nextReqNo, req.ReqNo)
		}

		co.nextReqNo += ao.numBuckets
	}

	return nil
}

// TODO, bucket probably can/should be stored in the *sequence
func (ao *allOutstandingReqs) applyAcks(bucket bucketID, seq *sequence, batch []*pb.RequestAck) (*Actions, error) {
	bo, ok := ao.buckets[bucket]
	if !ok {
		panic("dev sanity test")
	}

	outstandingReqs := map[string]struct{}{}

	for _, req := range batch {
		co, ok := bo.clients[req.ClientId]
		if !ok {
			return nil, fmt.Errorf("no such client")
		}

		if co.nextReqNo != req.ReqNo {
			return nil, fmt.Errorf("expected ClientId=%d next request for Bucket=%d to have ReqNo=%d but got ReqNo=%d", req.ClientId, bucket, co.nextReqNo, req.ReqNo)
		}

		key := string(req.Digest)
		if _, ok := ao.correctRequests[key]; ok {
			delete(ao.correctRequests, key)
		} else {
			ao.outstandingRequests[key] = seq
			outstandingReqs[key] = struct{}{}
		}

		co.nextReqNo += ao.numBuckets
	}

	return seq.allocate(batch, outstandingReqs), nil
}
