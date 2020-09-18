/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
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

	proposalBuckets map[bucketID]*proposalBucket
}

type proposalBucket struct {
	totalBuckets int
	lastReadyReq *readyEntry
	requestCount uint32
	pending      []*clientRequest
	bucketID     bucketID
}

func newProposer(myConfig *pb.StateEvent_InitialParameters, clientTracker *clientTracker, buckets map[bucketID]nodeID) *proposer {
	proposalBuckets := map[bucketID]*proposalBucket{}
	for bucketID, id := range buckets {
		if id != nodeID(myConfig.Id) {
			continue
		}
		proposalBuckets[bucketID] = &proposalBucket{
			bucketID:     bucketID,
			totalBuckets: len(buckets),
			lastReadyReq: clientTracker.readyHead,
			requestCount: myConfig.BatchSize,
			pending:      make([]*clientRequest, 0, 1), // TODO, might be interesting to play with not preallocating for performance reasons
		}
	}

	return &proposer{
		myConfig:        myConfig,
		proposalBuckets: proposalBuckets,
	}
}

func (p *proposer) proposalBucket(bucketID bucketID) *proposalBucket {
	return p.proposalBuckets[bucketID]
}

func (prb *proposalBucket) advance() {
	for uint32(len(prb.pending)) < prb.requestCount {
		if prb.lastReadyReq.next == nil {
			break
		}

		prb.lastReadyReq = prb.lastReadyReq.next

		crn := prb.lastReadyReq.clientReqNo
		if crn.committed != nil {
			// This seems like an odd check, but the ready list is not constantly GC-ed
			continue
		}

		bucket := bucketID((crn.reqNo + crn.clientID) % uint64(prb.totalBuckets))

		if bucket != prb.bucketID {
			continue
		}

		if len(crn.strongRequests) > 1 {
			if _, ok := crn.strongRequests[""]; !ok {
				panic("dev sanity test")
			}

			// We must have a null request here, so prefer it.
			prb.pending = append(prb.pending, crn.strongRequests[""])
		} else {
			if len(crn.strongRequests) != 1 {
				panic("dev sanity test")
			}

			// There must be exactly one strong request
			for _, clientReq := range crn.strongRequests {
				prb.pending = append(prb.pending, clientReq)
				break
			}
		}
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
