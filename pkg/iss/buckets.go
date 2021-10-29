/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"container/list"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type requestBucket struct {
	ID      int
	reqMap  map[string]*list.Element
	reqList list.List
}

func newRequestBucket(id int) *requestBucket {
	return &requestBucket{
		ID:      id,
		reqMap:  make(map[string]*list.Element),
		reqList: list.List{},
	}
}

func (b *requestBucket) Len() int {
	return b.reqList.Len()
}

func (b *requestBucket) Add(reqRef *requestpb.RequestRef) {

	// Compute map key of request.
	key := reqStrKey(reqRef)

	// If request has already been added to the bucket, do nothing.
	// TODO: Write comment about garbage collection and about the map being a superset of the list.
	if _, ok := b.reqMap[key]; ok {
		return
	}

	// Add request to the bucket.
	e := b.reqList.PushBack(reqRef)
	b.reqMap[key] = e
}

// TODO: Say that Contains still counts in the "removed" requests until garbage collection at the end of the epoch.
func (b *requestBucket) Contains(reqRef *requestpb.RequestRef) bool {
	_, ok := b.reqMap[reqStrKey(reqRef)]
	return ok
}

// RemoveFirst removes the first up to n requests from the bucket and appends them to the accumulator acc.
// Returns the resulting slice obtained by appending the Requests to acc.
func (b *requestBucket) RemoveFirst(n int, acc []*requestpb.RequestRef) []*requestpb.RequestRef {
	for ; b.Len() > 0 && n > 0; n-- {
		acc = append(acc, b.reqList.Remove(b.reqList.Front()).(*requestpb.RequestRef))
	}

	return acc
}

func bucketId(reqRef *requestpb.RequestRef, numBuckets int) int {
	return int(reqRef.ClientId+reqRef.ReqNo) % numBuckets // If types change, this might need to be updated.
}

// leaders must not be empty!
// Will need to be updated to have a more sophisticated, liveness-ensuring implementation.
func distributeBuckets(buckets []*requestBucket, leaders []t.NodeID) map[t.NodeID][]int {
	leaderBuckets := make(map[t.NodeID][]int)
	for _, leader := range leaders {
		leaderBuckets[leader] = make([]int, 0)
	}

	leaderIdx := 0
	for bID, _ := range buckets {
		leaderBuckets[leaders[leaderIdx]] = append(leaderBuckets[leaders[leaderIdx]], bID)
		leaderIdx++
		if leaderIdx == len(leaders) {
			leaderIdx = 0
		}
	}

	return leaderBuckets
}

func cutBatch(buckets []*requestBucket, maxBatchSize int) *requestpb.Batch {
	batch := &requestpb.Batch{Requests: make([]*requestpb.RequestRef, 0, maxBatchSize)}

	totalRequests := 0
	for _, b := range buckets {
		totalRequests += b.Len()
	}

	initCut := 0
	if maxBatchSize <= totalRequests {
		initCut = maxBatchSize / len(buckets)
	} else {
		initCut = totalRequests / len(buckets)
	}

	for _, b := range buckets {
		batch.Requests = b.RemoveFirst(initCut, batch.Requests)
	}

	// Fill rest of the batch with any requests, iterating over all buckets.
	for _, b := range buckets {

		if len(batch.Requests) < maxBatchSize { // If we are still missing some requests
			// Add up to the missing number of requests (maxBatchSize - len(newBatch.Requests)).
			batch.Requests = b.RemoveFirst(maxBatchSize-len(batch.Requests), batch.Requests)

		} else { // Stop iterating over buckets as soon as enough requests were collected.
			break
		}
	}

	return batch
}
