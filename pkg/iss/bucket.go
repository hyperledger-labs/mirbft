/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"container/list"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
)

// requestBucket represents a subset of received requests (called a Bucket in ISS)
// that retains the order in which the requests have been added.
// Each request deterministically maps to a single bucket.
// A hash function (bucketGroup.RequestBucket()) decides which bucket a request falls into.
//
// The implementation of a bucket must support efficient additions (new requests),
// removals of the n oldest requests (cutting a batch), as well as removals of random requests (committing requests).
type requestBucket struct {

	// Numeric ID of the bucket.
	// A hash function maps each request to a bucket ID.
	ID int

	// Doubly linked list of requests.
	// New requests are added to the "back" of this list, new batches are cut from the "front".
	reqList list.List

	// Map index of the list elements, index by (a string representation of) request references.
	// This is required for efficiently removing requests from the list.
	// On removal, the list element corresponding to the request being removed
	// is first looked up in the list (constant time) and then unlinked from the list (constant time)
	//
	// This map also serves as an add-only set of requests. This means that even on removal from the list,
	// An entry under the request's key stays in this map. The Add() method only adds new requests to the bucket
	// if such an entry does not exist. When a request is removed from the bucket,
	// the value of the corresponding map entry is set to nil (instead of the pointer to a list element)
	// to signify that the request is not in the bucket anymore.
	// Thus, the reqMap always contains a superset of the requests in reqList.
	// Map entries are only removed when client watermarks are moved, in which case entries falling below the low
	// watermark can safely be deleted.
	// TODO: Implement garbage collection. It might be helpful
	//       to make the hash function only take client ID and request Nr as arguments,
	//       instead of the whole request reference.
	reqMap map[string]*list.Element

	// TODO: Make sure the system works well even if a malicious client tries to submit conflicting requests.
	//       If any conflicting requests end up in a bucket, make sure to garbage-collect them.

	// Logger for outputting debugging messages.
	logger logging.Logger
}

// newRequestBucket returns a new initialized request bucket with ID `id`.
func newRequestBucket(id int, logger logging.Logger) *requestBucket {
	return &requestBucket{
		ID:      id,
		reqMap:  make(map[string]*list.Element),
		reqList: list.List{},
		logger:  logger,
	}
}

// Len returns the number of requests in the bucket.
func (b *requestBucket) Len() int {
	// Only requests that are actually present, i.e., those added and not removed, count.
	// Even though removed requests might still linger around in the reqMap (to prevent re-adding them),
	// those do not count towards the number of requests in the bucket.
	return b.reqList.Len()
}

// Add adds a new request to the bucket if that request has not yet been added.
// Note that even requests that have been added and removed cannot be added again
// (except for the case of request resurrection, but this is done using the Resurrect() method).
// Returns true if the request was not in the bucket and has just been added,
// false if the request already has been added.
func (b *requestBucket) Add(reqRef *requestpb.RequestRef) bool {

	// Compute map key of request.
	key := reqStrKey(reqRef)

	// If request has already been added to the bucket, do not add it again.
	// It is important to check for the presence of the entry in reqMap (using the second return value)
	// rather than just comparing the first return value to nil, since a nil value (but present in the map)
	// signifies that the request has been added and removed, in which chase it must not be added again.
	if _, ok := b.reqMap[key]; ok {
		return false
	} else {
		// Add request to the bucket.
		e := b.reqList.PushBack(reqRef)
		b.reqMap[key] = e
		return true
	}
}

// Remove removes a request from the bucket.
// Note that even after removal, the request cannot be added again using the Add() method.
// It can be, however, returned to the bucket during request resurrection, see Resurrect().
func (b *requestBucket) Remove(reqRef *requestpb.RequestRef) {

	// Look up the corresponding element in the reqMap.
	reqKey := reqStrKey(reqRef)
	element, ok := b.reqMap[reqKey]

	if !ok {
		// Request has never been added or has been garbage-collected.
		b.logger.Log(logging.LevelWarn, "Request to remove not found.", "reqKey", reqKey)
	} else if element == nil {
		// Request has already been removed.
		b.logger.Log(logging.LevelWarn, "Request to remove already removed.", "reqKey", reqKey)
	} else {
		// Request present.
		// Remove it from the list and set its map entry to nil
		// (important: keep the nil map entry until client watermarks move past this request)
		b.reqList.Remove(b.reqMap[reqKey])
		b.reqMap[reqKey] = nil
	}
}

// Contains returns true if the given request is in the bucket, false otherwise.
// Only requests that have been added but not removed count as contained in the bucket,
// as well as resurrected requests that have not been removed since resurrection.
func (b *requestBucket) Contains(reqRef *requestpb.RequestRef) bool {
	// We check against nil on purpose, as we are not interested in removed requests here.
	return b.reqMap[reqStrKey(reqRef)] != nil
}

// RemoveFirst removes the first up to n requests from the bucket and appends them to the accumulator acc.
// Returns the resulting slice obtained by appending the Requests to acc.
func (b *requestBucket) RemoveFirst(n int, acc []*requestpb.RequestRef) []*requestpb.RequestRef {

	for ; b.Len() > 0 && n > 0; n-- {
		acc = append(acc, b.reqList.Remove(b.reqList.Front()).(*requestpb.RequestRef))
	}

	return acc
}

// Resurrect re-adds a previously removed request to the bucket, effectively undoing the removal of the request.
// The request is added to the "front" of the bucket, i.e., it will be the first request to be removed by RemoveFirst().
// Request resurrection is performed when a leader proposes a batch from this bucket,
// but, for some reason, the batch is not committed in the same epoch.
// In such a case, the requests contained in the batch need to be resurrected
// (i.e., put back in their respective buckets) so they can be committed in a future epoch.
func (b *requestBucket) Resurrect(reqRef *requestpb.RequestRef) {

	// Compute map key of request.
	key := reqStrKey(reqRef)

	// If request is not in the reqMap or request already is in the bucket, panic. This must never happen.
	if element, ok := b.reqMap[key]; !ok || element == nil {
		panic(fmt.Errorf("resurrecting request that has been garbage-collected or never added"))
	}

	// Add request to the front of the list.
	// Note that this differs from Add(), which adds the request to the back of the list.
	// In this case, since the resurrected request already has been proposed,
	// it should be the first one to be re-proposed again.
	e := b.reqList.PushFront(reqRef)
	b.reqMap[key] = e
}
