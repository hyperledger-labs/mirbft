/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"encoding/binary"

	pb "github.com/IBM/mirbft/mirbftpb"
)

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type proposer struct {
	myConfig *pb.StateEvent_InitialParameters

	proposalBuckets map[BucketID]*proposalBucket
}

type proposalBucket struct {
	totalBuckets int
	lastReadyReq *list.Element
	readyList    *list.List
	requestCount uint32
	pending      []*clientRequest
	bucketID     BucketID
}

func newProposer(myConfig *pb.StateEvent_InitialParameters, clientWindows *clientWindows, buckets map[BucketID]NodeID) *proposer {
	proposalBuckets := map[BucketID]*proposalBucket{}
	for bucketID, nodeID := range buckets {
		if nodeID != NodeID(myConfig.Id) {
			continue
		}
		proposalBuckets[bucketID] = &proposalBucket{
			bucketID:     bucketID,
			totalBuckets: len(buckets),
			readyList:    clientWindows.readyList,
			requestCount: myConfig.BatchSize,
			pending:      make([]*clientRequest, 0, 1), // TODO, might be interesting to play with not preallocating for performance reasons
		}
	}

	return &proposer{
		myConfig:        myConfig,
		proposalBuckets: proposalBuckets,
	}
}

func (p *proposer) proposalBucket(bucketID BucketID) *proposalBucket {
	return p.proposalBuckets[bucketID]
}

func (prb *proposalBucket) advance() {
	for uint32(len(prb.pending)) < prb.requestCount {
		var nextReadyReq *list.Element
		if prb.lastReadyReq == nil {
			nextReadyReq = prb.readyList.Front()
		} else {
			nextReadyReq = prb.lastReadyReq.Next()
		}

		if nextReadyReq == nil {
			break
		}

		prb.lastReadyReq = nextReadyReq

		crn := nextReadyReq.Value.(*clientReqNo)
		if crn.committed != nil {
			// This seems like an odd check, but the ready list is not constantly GC-ed
			continue
		}

		bucket := BucketID((crn.reqNo + crn.clientID) % uint64(prb.totalBuckets))

		if bucket != prb.bucketID {
			continue
		}

		prb.pending = append(prb.pending, crn.strongRequest)
	}
}

func (prb *proposalBucket) hasOutstanding() bool {
	prb.advance()
	return uint32(len(prb.pending)) > 0
}

func (prb *proposalBucket) hasPending() bool {
	prb.advance()
	return len(prb.pending) > 0 && uint32(len(prb.pending)) == prb.requestCount
}

func (prb *proposalBucket) next() []*clientRequest {
	result := prb.pending
	prb.pending = make([]*clientRequest, 0, prb.requestCount)
	return result
}
