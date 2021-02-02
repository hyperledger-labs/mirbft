/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

func newOutstandingReqs(clientTracker *clientTracker, networkState *pb.NetworkState, logger Logger) *allOutstandingReqs {
	clientTracker.availableList.resetIterator()

	ao := &allOutstandingReqs{
		buckets:             map[bucketID]*bucketOutstandingReqs{},
		correctRequests:     map[string]*pb.RequestAck{},
		outstandingRequests: map[string]*sequence{},
		availableIterator:   clientTracker.availableList,
	}

	numBuckets := int(networkState.Config.NumberOfBuckets)

	for i := bucketID(0); i < bucketID(numBuckets); i++ {
		bo := &bucketOutstandingReqs{
			clients: map[uint64]*clientOutstandingReqs{},
		}
		ao.buckets[i] = bo

		for _, client := range networkState.Clients {
			var firstUncommitted uint64
			for j := 0; j < numBuckets; j++ {
				reqNo := client.LowWatermark + uint64(j)
				if clientReqToBucket(client.Id, reqNo, networkState.Config) == i {
					firstUncommitted = reqNo
					break
				}
			}

			ctClient, _ := clientTracker.client(client.Id)

			logger.Log(LevelDebug, "initializing outstanding reqs for client", "client_id", client.Id, "bucket_id", i, "low_watermark", client.LowWatermark)

			cors := &clientOutstandingReqs{
				nextReqNo:  firstUncommitted,
				numBuckets: uint64(networkState.Config.NumberOfBuckets),
				client:     ctClient,
			}
			cors.advance()
			bo.clients[client.Id] = cors
		}
	}

	ao.advanceRequests() // Note, this can return no actions as no sequences have allocated

	return ao
}

type allOutstandingReqs struct {
	buckets             map[bucketID]*bucketOutstandingReqs
	availableIterator   *availableList
	correctRequests     map[string]*pb.RequestAck
	outstandingRequests map[string]*sequence
}

type bucketOutstandingReqs struct {
	clients map[uint64]*clientOutstandingReqs // TODO, obvious optimization is to make this active clients and initialize this lazily
}

type clientOutstandingReqs struct {
	nextReqNo  uint64
	numBuckets uint64
	client     *client
}

func (cors *clientOutstandingReqs) advance() {
	for cors.nextReqNo <= cors.client.highWatermark {
		crn := cors.client.reqNo(cors.nextReqNo)
		if crn.committed != nil {
			cors.nextReqNo += cors.numBuckets
			continue
		}

		break
	}
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

// TODO, bucket probably can/should be stored in the *sequence
func (ao *allOutstandingReqs) applyAcks(bucket bucketID, seq *sequence, batch []*pb.RequestAck) (*Actions, error) {
	bo, ok := ao.buckets[bucket]
	assertTruef(ok, "told to apply acks for bucket %d which does not exist", bucket)

	outstandingReqs := map[string]struct{}{}

	for _, req := range batch {
		co, ok := bo.clients[req.ClientId]
		if !ok {
			return nil, fmt.Errorf("no such client")
		}

		if co.nextReqNo != req.ReqNo {
			return nil, fmt.Errorf("expected ClientId=%d next request for Bucket=%d to have ReqNo=%d but got ReqNo=%d", req.ClientId, bucket, co.nextReqNo, req.ReqNo)
		}

		// TODO, return an error if the request proposed is for a seqno before this request is valid

		key := string(req.Digest)
		if _, ok := ao.correctRequests[key]; ok {
			delete(ao.correctRequests, key)
		} else {
			ao.outstandingRequests[key] = seq
			outstandingReqs[key] = struct{}{}
		}

		co.nextReqNo += co.numBuckets
		co.advance()
	}

	return seq.allocate(batch, outstandingReqs), nil
}
