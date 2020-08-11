/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"encoding/binary"
)

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type proposer struct {
	myConfig      *Config
	clientWindows *clientWindows
	lastReadyReq  *list.Element

	totalBuckets    int
	proposalBuckets map[BucketID]*proposalBucket
}

type proposalBucket struct {
	queue     []*clientRequest
	sizeBytes int
	pending   [][]*clientRequest
}

func newProposer(myConfig *Config, clientWindows *clientWindows, buckets map[BucketID]NodeID) *proposer {
	proposalBuckets := map[BucketID]*proposalBucket{}
	for bucketID, nodeID := range buckets {
		if nodeID != NodeID(myConfig.ID) {
			continue
		}
		proposalBuckets[bucketID] = &proposalBucket{}
	}

	return &proposer{
		myConfig:        myConfig,
		clientWindows:   clientWindows,
		proposalBuckets: proposalBuckets,
		totalBuckets:    len(buckets),
	}
}

func (p *proposer) stepAllClientWindows() {
	for {
		var nextReadyReq *list.Element
		if p.lastReadyReq == nil {
			nextReadyReq = p.clientWindows.readyList.Front()
		} else {
			nextReadyReq = p.lastReadyReq.Next()
		}

		if nextReadyReq == nil {
			break
		}
		p.lastReadyReq = nextReadyReq

		crn := nextReadyReq.Value.(*clientReqNo)
		bucket := BucketID((crn.reqNo + crn.clientID) % uint64(p.totalBuckets))

		proposalBucket, ok := p.proposalBuckets[bucket]
		if !ok {
			// I don't lead this bucket this epoch
			continue
		}

		if crn.committed != nil {
			continue
		}

		proposalBucket.queue = append(proposalBucket.queue, crn.strongRequest)
		proposalBucket.sizeBytes += len(crn.strongRequest.data.Data)
		if proposalBucket.sizeBytes >= p.myConfig.BatchParameters.CutSizeBytes {
			proposalBucket.pending = append(proposalBucket.pending, proposalBucket.queue)
			proposalBucket.queue = nil
			proposalBucket.sizeBytes = 0
		}
	}
}

func (p *proposer) hasOutstanding(bucket BucketID) bool {
	proposalBucket := p.proposalBuckets[bucket]

	return len(proposalBucket.queue) > 0 || len(proposalBucket.pending) > 0
}

func (p *proposer) hasPending(bucket BucketID) bool {
	return len(p.proposalBuckets[bucket].pending) > 0
}

func (p *proposer) next(bucket BucketID) []*clientRequest {
	proposalBucket := p.proposalBuckets[bucket]

	if len(proposalBucket.pending) > 0 {
		n := proposalBucket.pending[0]
		proposalBucket.pending = proposalBucket.pending[1:]
		return n
	}

	if len(proposalBucket.queue) > 0 {
		n := proposalBucket.queue
		proposalBucket.queue = nil
		proposalBucket.sizeBytes = 0
		return n
	}

	panic("called next when nothing outstanding")
}
