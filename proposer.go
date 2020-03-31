/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"encoding/binary"
	"fmt"
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
	myConfig               *Config
	clientWindowProcessors map[string]*clientWindowProcessor
	clientWindows          *clientWindows

	totalBuckets    int
	proposalBuckets map[BucketID]*proposalBucket
}

type clientWindowProcessor struct {
	lastProcessed uint64
	clientWindow  *clientWindow
}

type proposalBucket struct {
	queue     []*request
	sizeBytes int
	pending   [][]*request
}

func newProposer(myConfig *Config, clientWindows *clientWindows, buckets map[BucketID]NodeID) *proposer {
	proposalBuckets := map[BucketID]*proposalBucket{}
	for bucketID, nodeID := range buckets {
		if nodeID != NodeID(myConfig.ID) {
			continue
		}
		proposalBuckets[bucketID] = &proposalBucket{}
	}

	clientWindowProcessors := map[string]*clientWindowProcessor{}
	for clientID, clientWindow := range clientWindows.windows {
		rwp := &clientWindowProcessor{
			lastProcessed: clientWindow.lowWatermark - 1,
			clientWindow:  clientWindow,
		}
		clientWindowProcessors[clientID] = rwp
	}

	return &proposer{
		myConfig:               myConfig,
		clientWindowProcessors: clientWindowProcessors,
		clientWindows:          clientWindows,
		proposalBuckets:        proposalBuckets,
		totalBuckets:           len(buckets),
	}
}

func (p *proposer) stepAllClientWindows() {
	for _, clientID := range p.clientWindows.clients {
		// TODO, this logic favors clients with lower IDs, we really should
		// remember where we last left off to prevent starvation
		p.stepClientWindow([]byte(clientID))
	}
}

func (p *proposer) stepClientWindow(clientID []byte) {
	rwp, ok := p.clientWindowProcessors[string(clientID)]
	if !ok {
		rw, ok := p.clientWindows.clientWindow(clientID)
		if !ok {
			panic(fmt.Sprintf("unexpected, missing client %x", []byte(clientID)))
		}

		rwp = &clientWindowProcessor{
			lastProcessed: rw.lowWatermark - 1,
			clientWindow:  rw,
		}
		p.clientWindowProcessors[string(clientID)] = rwp
	}

	for rwp.lastProcessed < rwp.clientWindow.highWatermark {
		request := rwp.clientWindow.request(rwp.lastProcessed + 1)
		if request == nil {
			break
		}

		rwp.lastProcessed++

		// TODO, maybe offset the bucket ID by something in the client ID so not all start in bucket 1?
		// maybe some sort of client index?
		bucket := BucketID(request.requestData.ReqNo % uint64(p.totalBuckets))
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
