/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

func newOutstandingReqs(networkConfig *pb.NetworkConfig) *allOutstandingReqs {
	ao := &allOutstandingReqs{
		numBuckets: uint64(networkConfig.NumberOfBuckets),
		buckets:    map[BucketID]*bucketOutstandingReqs{},
	}

	for i := BucketID(0); i < BucketID(networkConfig.NumberOfBuckets); i++ {
		bo := &bucketOutstandingReqs{
			clients: map[uint64]*clientOutstandingReqs{},
		}
		ao.buckets[i] = bo

		for _, client := range networkConfig.Clients {
			bo.clients[client.Id] = &clientOutstandingReqs{
				nextReqNo: client.BucketLowWatermarks[int(i)],
			}
		}
	}

	return ao
}

type allOutstandingReqs struct {
	numBuckets uint64
	buckets    map[BucketID]*bucketOutstandingReqs
}

type bucketOutstandingReqs struct {
	clients map[uint64]*clientOutstandingReqs // TODO, obvious optimization is to make this active clients and initialize this lazily
}

type clientOutstandingReqs struct {
	nextReqNo uint64
}

func (ao *allOutstandingReqs) applyBatch(bucket BucketID, batch []*pb.ForwardRequest) error {
	bo, ok := ao.buckets[bucket]
	if !ok {
		panic("dev sanity test")
	}

	for _, req := range batch {
		co, ok := bo.clients[req.Request.ClientId]
		if !ok {
			return fmt.Errorf("no such client")
		}

		if co.nextReqNo != req.Request.ReqNo {
			return fmt.Errorf("expected ClientId=%d next request for Bucket=%d to have ReqNo=%d but got ReqNo=%d", req.Request.ClientId, bucket, co.nextReqNo, req.Request.ReqNo)
		}

		co.nextReqNo += ao.numBuckets
	}

	return nil
}

func (ao *allOutstandingReqs) applyAcks(bucket BucketID, batch []*pb.RequestAck) error {
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
