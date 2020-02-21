/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"encoding/binary"
)

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

func bytesToUint64(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}

type proposer struct {
	myConfig                *Config
	requestWindowProcessors map[string]*requestWindowProcessor

	totalBuckets    int
	proposalBuckets map[BucketID]*proposalBucket
}

type requestWindowProcessor struct {
	lastProcessed uint64
	requestWindow *requestWindow
}

type proposalBucket struct {
	queue     []*request
	sizeBytes int
	pending   [][]*request
}

func newProposer(myConfig *Config, requestWindows map[string]*requestWindow, buckets map[BucketID]NodeID) *proposer {
	proposalBuckets := map[BucketID]*proposalBucket{}
	for bucketID, nodeID := range buckets {
		if nodeID != NodeID(myConfig.ID) {
			continue
		}
		proposalBuckets[bucketID] = &proposalBucket{}
	}

	requestWindowProcessors := map[string]*requestWindowProcessor{}
	for clientID, requestWindow := range requestWindows {
		rwp := &requestWindowProcessor{
			lastProcessed: requestWindow.lowWatermark - 1,
			requestWindow: requestWindow,
		}
		requestWindowProcessors[clientID] = rwp
	}

	return &proposer{
		myConfig:                myConfig,
		requestWindowProcessors: requestWindowProcessors,
		proposalBuckets:         proposalBuckets,
		totalBuckets:            len(buckets),
	}
}

func (p *proposer) stepAllRequestWindows() {
	// TODO, this is kind of dumb to get a key from a map, and then
	// look it up in the map again
	for clientID := range p.requestWindowProcessors {
		p.stepRequestWindow(clientID)
	}
}

func (p *proposer) stepRequestWindow(clientID string) {
	rwp, ok := p.requestWindowProcessors[clientID]
	if !ok {
		panic("unexpected")
	}

	for rwp.lastProcessed < rwp.requestWindow.highWatermark {
		request := rwp.requestWindow.request(rwp.lastProcessed + 1)
		if request == nil {
			break
		}

		rwp.lastProcessed++

		bucket := BucketID(bytesToUint64(request.digest) % uint64(p.totalBuckets))
		proposalBucket, ok := p.proposalBuckets[bucket]
		if !ok {
			// I don't lead this bucket this epoch
			continue
		}

		if request.state != Uninitialized {
			// Already proposed by another node in a previous epoch
			continue
		}

		proposalBucket.queue = append(proposalBucket.queue, request)
		proposalBucket.sizeBytes += len(request.requestData.Data)
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

func (p *proposer) next(bucket BucketID) []*request {
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
