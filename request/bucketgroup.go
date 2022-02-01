// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package request

import (
	"sort"
	"sync/atomic"
	"time"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/membership"
)

type BucketGroup struct {
	// List of buckets in the group.
	// Must be sorted by Bucket ID and, when locking buckets, locks must be acquired in list order.
	buckets []*Bucket

	// Total number of requests in the buckets.
	// Must be read / updated atomically when requests are being added to buckets and not all buckets are locked.
	totalRequests int32

	// When CutBatch() has been called but the Buckets have fewer than BatchSize requests in total, CutBatch() blocks.
	// Before blocking, it sets cutThreshold to the number of requests it is waiting for.
	// When CutBatch() is not waiting, cutThreshold is -1.
	cutThreshold int

	// When CutBatch() is waiting for more requests to be added to the Bucket, this variable holds the Timer
	// implementing a timeout, after which CutBatch() proceeds regardless of the number of requests in the Bucket.
	// When CutBatch() is not waiting, timer is nil.
	timer *time.Timer

	// When CutBatch() is waiting for more requests to be added to the Buckets, it blocks on reading from this channel.
	// A value can be pushed to this channel by
	// - a timeout
	// - the Add() method of some bucket, when enough requests have been added.
	batchTrigger chan struct{}

	// When waiting for a batch, wait at least until this time.
	// This is used to moderate the speed at which batches are cut when the bucket is full.
	// It is useful to limit the rate at which data is put on the wire, decreasing the likelihood of view changes
	// when too much data is sent out concurrently.
	nextBatchTimestamp int64
}

// Creates a new BucketGroup and returns a pointer to it.
// The buckets parameter is a list of bucket IDs. NewBucketGroup() sorts this list!
// Sorting by bucket ID is important to prevent deadlocks, as buckets are always locked in the order of this list.
// NOTE: Currently there is always only one bucket group, so these deadlocks cannot occur, but this might change.
func NewBucketGroup(bucketIDs []int) *BucketGroup {

	// Sort bucket IDs.
	sort.Ints(bucketIDs)

	// Create a new list of all the buckets based on their IDs
	bucketList := make([]*Bucket, len(bucketIDs), len(bucketIDs))
	for i, bucketID := range bucketIDs {
		bucketList[i] = Buckets[bucketID]
	}

	return &BucketGroup{
		buckets:            bucketList,
		totalRequests:      0,
		cutThreshold:       -1,
		timer:              nil,
		batchTrigger:       make(chan struct{}),
		nextBatchTimestamp: 0,
	}
}

// Returns a new request batch assembled from requests in the bucket group.
// Blocks until the Buckets contain at least size requests, but at most for the duration of timeout.
// On timeout, returns a batch with all requests in the Buckets, even if all the Buckets are empty.
func (bg *BucketGroup) CutBatch(size int, timeout time.Duration) *Batch {
	alreadyWaited := bg.waitMinimum()
	bg.lockBuckets()
	defer bg.unlockBuckets()

	// Wait for batch to fill or for the timeout to fire.
	// May release and re-acquire the bucket locks before returning.
	bg.waitForRequestsLocked(size, timeout-time.Duration(alreadyWaited)*time.Nanosecond)

	// Create new request batch
	newBatch := Batch{Requests: make([]*Request, 0, size)}

	// If the bucket group is empty (contains no buckets), return an empty batch.
	// (This is a corner case that should not occur with a reasonable configuration.)
	// Handling this here is necessary to avoid dividing by zero.
	if len(bg.buckets) == 0 {
		return &newBatch
	}

	// Calculate the initial number of Requests that can be fairly taken from each Bucket.
	// (Requests from each Bucket must have the chance to make it in the Batch.)
	var initCut = 0
	if size <= int(bg.totalRequests) {
		initCut = size / len(bg.buckets)
	} else {
		initCut = int(bg.totalRequests) / len(bg.buckets)
	}

	// Add initial requests to the batch.
	for _, b := range bg.buckets {
		newBatch.Requests = b.RemoveFirst(initCut, newBatch.Requests)
	}

	// Fill rest of the batch with any requests, iterating over all buckets.
	for _, b := range bg.buckets {

		// If we are still missing some requests
		if len(newBatch.Requests) < size {
			// Add up to the missing number of requests (size - len(newBatch.Requests)).
			newBatch.Requests = b.RemoveFirst(size-len(newBatch.Requests), newBatch.Requests)

			// Stop iterating over buckets if enough requests were collected.
		} else {
			break
		}
	}

	// TODO: Possible optimization (but probably irrelevant):
	//       We could mark requests as "in flight" here, instead of outside of this function.

	// Compute starting from when the next batch can be cut, in order to respect the throughput cap.
	// This enforces a limit on the rate batch data is being put on the wire.
	// If, for some reason, too many batches are sent concurrently (e.g. when the bucket is very full),
	// all of them will be slow, increasing the likelihood of timeouts.
	totalReq := len(newBatch.Requests) * membership.NumNodes()
	if config.Config.LeaderPolicy == "Single" {
		totalReq /= membership.NumNodes() // For Single leader policy, use the raw throughput cap, not adjusted to system size.
	}
	waitingTime := 1000000000 * int64(totalReq/config.Config.ThroughputCap) // In nanoseconds
	atomic.StoreInt64(&bg.nextBatchTimestamp, time.Now().UnixNano()+waitingTime)

	logger.Debug().
		Int("nBuckets", len(bg.buckets)).
		Int("nReq", len(newBatch.Requests)).
		Int("left", bg.CountRequests()).
		Int64("next", waitingTime/1000000). // In milliseconds
		Msg("Batch cut.")

	return &newBatch
}

// Blocks until the buckets in the BucketGroup (cumulatively) contain numRequests requests or until timeout elapses.
// When WaitForRequests returns, bg.totalRequests accurately represents the total number of requests in the BucketGroup.
func (bg *BucketGroup) WaitForRequests(numRequests int, timeout time.Duration) {
	alreadyWaited := bg.waitMinimum()
	bg.lockBuckets()
	bg.waitForRequestsLocked(numRequests, timeout-time.Duration(alreadyWaited)*time.Nanosecond)
	bg.unlockBuckets()
}

// Blocks until the buckets in the BucketGroup (cumulatively) contain numRequests requests or until timeout elapses.
// When WaitForRequests returns, bg.totalRequests accurately represents the total number of requests in the BucketGroup.
// ATTENTION: All Buckets must be LOCKED when calling this method.
//            May release and re-acquire the bucket locks before returning.
func (bg *BucketGroup) waitForRequestsLocked(numRequests int, timeout time.Duration) {

	// Count all requests in all buckets in the group.
	// (A normal assignment suffices, as all buckets are locked.)
	bg.totalRequests = int32(bg.CountRequests())

	// If there are enough requests in the bucket, return immediately.
	if int(bg.totalRequests) >= numRequests || (timeout == 0) {
		return
	}

	// Initialize data structures for waiting for requests.
	bg.cutThreshold = numRequests
	bg.timer = time.AfterFunc(timeout, func() { bg.batchTrigger <- struct{}{} })

	// Register this group with all buckets (for notifications).
	for _, b := range bg.buckets {
		b.Group = bg
	}

	// Release locks (so that Add() can add requests to the buckets) and wait for the trigger
	// (released by the timeout or by bg.RequestAdded() called by a bucket's Add() method).
	bg.unlockBuckets()
	<-bg.batchTrigger
	bg.lockBuckets()

	// Clean up.
	for _, b := range bg.buckets {
		b.Group = nil
	}
	bg.timer = nil
	bg.cutThreshold = -1
}

func (bg *BucketGroup) waitMinimum() int64 {
	// Note that if nextBatchTimestamp is 0, this function returns immediately as expected.
	dur := atomic.LoadInt64(&bg.nextBatchTimestamp) - time.Now().UnixNano()
	if dur > 0 {
		time.Sleep(time.Duration(dur) * time.Nanosecond)
	} else {
		dur = 0
	}

	return dur // Return the number of nanoseconds waited.
}

// Notifies the BucketGroup that is waiting to cut a batch about a request being added in one of its buckets.
// Can be called concurrently from many Bucket.Add() methods (while the Bucket is locked).
func (bg *BucketGroup) RequestAdded() {

	// Atomically increment and fetch the number of requests in the BucketGroup.
	totalRequests := atomic.AddInt32(&bg.totalRequests, 1)

	// If CutBatch() is waiting and the required threshold of requests has been reached
	// It is important to use == (and not >= ) when comparing the request count, s.t. the body of the condition
	// is only executed once.
	if bg.timer != nil && int(totalRequests) == bg.cutThreshold {

		// Stop the timer
		if bg.timer.Stop() {

			// And release WaitForReuests().
			// Note that stopping the timer might fail if the timeout triggers concurrently with RequestAdded().
			// In such a case, this line is not executed, as the timeout does the job.
			bg.batchTrigger <- struct{}{}
		}
	}
}

// Counts all requests in all buckets.
// Only makes sense if the buckets are locked.
func (bg *BucketGroup) CountRequests() int {
	n := 0
	for _, b := range bg.buckets {
		n += b.Len()
	}
	return n
}

// Returns a list with the bucket IDs  in the Bucket Group.
func (bg *BucketGroup) GetBucketIDs() []int {
	ids := make([]int, len(bg.buckets))
	for i, b := range bg.buckets {
		ids[i] = b.id
	}
	return ids
}

// Locks all buckets in the group.
func (bg *BucketGroup) lockBuckets() {
	for _, b := range bg.buckets {
		b.Lock()
	}
}

// Unlocks all buckets in the group.
func (bg *BucketGroup) unlockBuckets() {
	for _, b := range bg.buckets {
		b.Unlock()
	}
}
