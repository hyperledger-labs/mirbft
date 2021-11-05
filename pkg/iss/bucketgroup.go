/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// bucketGroup represents a group of request buckets.
// It is used to represent both the set of all buckets used by ISS throughout the whole execution (across epochs),
// and subsets of it used to create request batches.
type bucketGroup []*requestBucket

// newBuckets returns a new group of numBuckets initialized buckets.
// The logger will be used to output bucket-related debugging messages.
func newBuckets(numBuckets int, logger logging.Logger) *bucketGroup {
	buckets := make([]*requestBucket, numBuckets, numBuckets)
	for i := 0; i < numBuckets; i++ {
		buckets[i] = newRequestBucket(i, logging.Decorate(logger, "Bucket: ", "bID", i))
	}
	return (*bucketGroup)(&buckets)
}

// Get returns the bucket with id bID.
func (buckets bucketGroup) Get(bID int) *requestBucket {
	return buckets[bID]
}

// TotalRequests returns the total number of requests in all buckets of this group.
func (buckets bucketGroup) TotalRequests() t.NumRequests {
	numRequests := t.NumRequests(0)
	for _, bucket := range buckets {
		numRequests += t.NumRequests(bucket.Len())
	}
	return numRequests
}

// Select returns a subgroup of buckets consisting only of buckets from this group with the given IDs.
// Select does not make deep copies of the selected buckets
// and the buckets underlying both the original and the new group are the same.
// If any of the given IDs is not represented in this group, Select panics.
func (buckets bucketGroup) Select(bucketIDs []int) bucketGroup {
	selectedBuckets := make([]*requestBucket, len(bucketIDs))
	for i, bID := range bucketIDs {
		selectedBuckets[i] = buckets[bID]
	}
	return selectedBuckets
}

// RequestBucket returns the bucket from this group to which the given request maps.
// Note that this depends on the whole bucket group (not just the request), as RequestBucket employs a hash function
// to evenly distribute requests among the buckets in the group.
// Thus, the same request may map to some bucket in one group and to a different bucket in a different group,
// even if the former bucket is part of the latter group.
func (buckets bucketGroup) RequestBucket(reqRef *requestpb.RequestRef) *requestBucket {
	bucketID := int(reqRef.ClientId+reqRef.ReqNo) % len(buckets) // If types change, this might need to be updated.
	return buckets.Get(bucketID)
}

// Distribute takes a list of node IDs (representing the leaders of the given epoch)
// and assigns a list of bucket IDs to each of the node (leader) IDs,
// such that the ID of each bucket is assigned to a exactly one leader.
// Distribute guarantees that if some node is part of `leaders` for infinitely many consecutive epochs
// (i.e., infinitely many invocations of Distribute with the `epoch` parameter values increasing monotonically),
// the ID of each bucket in the group will be assigned to the node infinitely many times.
// Distribute also makes best effort to distribute the buckets evenly among the leaders.
// If `leaders` is empty, Distribute returns an empty map.
// TODO: Update this to have a more sophisticated, livenes-ensuring implementation,
//       to actually implement what is written above.
//       An additional parameter with all the nodes (even non-leaders) might help there.
func (buckets bucketGroup) Distribute(leaders []t.NodeID, epoch t.EpochNr) map[t.NodeID][]int {

	// Catch the corner case where the input is empty.
	if len(leaders) == 0 {
		return map[t.NodeID][]int{}
	}

	// Initialize result data structure
	leaderBuckets := make(map[t.NodeID][]int)
	for _, leader := range leaders {
		leaderBuckets[leader] = make([]int, 0)
	}

	// Assign buckets to leaders in a round-robin way, offset by the epoch number.

	// Pick the leader to which to assign the first bucket (implements the epoch offset).
	leaderIdx := int(epoch) % len(leaders)

	// Assign each bucket to the next leader, wrapping around if necessary.
	for bID := range buckets {
		leaderBuckets[leaders[leaderIdx]] = append(leaderBuckets[leaders[leaderIdx]], bID)
		leaderIdx++
		if leaderIdx == len(leaders) {
			leaderIdx = 0
		}
	}

	// Return final assignment.
	return leaderBuckets
}

// CutBatch assembles and returns a new request batch from requests in the bucket group,
// removing those requests from their respective buckets.
// The size of the returned batch will be min(buckets.TotalRequests(), maxBatchSize).
// If possible, requests are taken from every non-empty bucket in the group.
func (buckets bucketGroup) CutBatch(maxBatchSize t.NumRequests) *requestpb.Batch {

	// Allocate a new request batch.
	batch := &requestpb.Batch{Requests: make([]*requestpb.RequestRef, 0, maxBatchSize)}

	// Count all requests in the bucket group.
	totalRequests := buckets.TotalRequests()

	if totalRequests <= maxBatchSize {
		// If all requests in the bucket group fit in the batch,
		// put all requests from all buckets in the batch and return.
		for _, b := range buckets {
			batch.Requests = b.RemoveFirst(b.Len(), batch.Requests)
		}
	} else {
		// If there are more requests than fit in the batch,
		// first cut off a fair portion of each bucket
		// (so that requests from each bucket can make it in the batch).
		fairCut := int(maxBatchSize) / len(buckets)
		for _, b := range buckets {
			batch.Requests = b.RemoveFirst(fairCut, batch.Requests)
		}

		// Fill rest of the batch with any requests, iterating over all buckets.
		// This is required in case the requests are distributed unevenly across the buckets
		// and not all buckets were able to provide their "fair" share of requests.
		for _, b := range buckets {

			if t.NumRequests(len(batch.Requests)) < maxBatchSize { // If we are still missing some requests
				// Add up to the missing number of requests (maxBatchSize - len(newBatch.Requests)).
				batch.Requests = b.RemoveFirst(int(maxBatchSize)-len(batch.Requests), batch.Requests)

			} else { // Stop iterating over buckets as soon as enough requests were collected.
				break
			}
		}
	}

	return batch
}
