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

package manager

import (
	"sync"

	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/request"
)

// A Manager orchestrates the interaction between the different modules (Log, Orderer, Checkpointer, ...).
// The main task of the Manager is to split the log up into Segments that can be ordered independently and in parallel.
// A Segment groups a subset of (not necessarily contiguous) sequence numbers of the log and lists the nodes that
// are responsible for agreeing on an order of Entries for these sequence numbers. (See the segment.go file itself.)
// The Manager informs the corresponding nodes about Segments they are involved in.
// The Manager also triggers the checkpointing protocol when appropriate.
// The decisions of the Manager about when to issue a new Segment or when to trigger the checkpointing protocol are
// mainly based on observing the state of the log.
type Manager interface {

	// Returns a channel to which the Manager pushes all segments in the ordering of which this node is involved.
	// This function is called by the Orderer in order to know when a new instance of the ordering protocol needs
	// to be started.
	SubscribeOrderer() chan Segment

	// Returns a channel to which the Manager pushes sequence numbers at which a checkpoint should occur.
	// This function is called by the Checkpointer module to know when a new instance of the checkpoint protocol needs
	// to be started.
	SubscribeCheckpointer() chan int32

	// Starts the Manager, making it start issuing Segments and sequence numbers for checkpointing.
	// The Orderer and the Checkpointer need to have subscribed by the time Start() is called.
	// Meant to be run as a separate goroutine, and thus no critical initialization must be performed here
	// (e.g. subscribing to log events such as Entries or Checkpoints), as these actions may be delayed arbitrarily.
	// Decrements the provided wait group when done.
	Start(group *sync.WaitGroup)
}

// Calculates the assignment of buckets to leaders in epoch e.
// The leaders parameter is a list of peerIDs that are leaders in epoch e.
// Returns a list of BucketGroups where the BucketGroup[i] at contains buckets assigned to leaders[i].
//
// OPT: Most of the variables used in this function can be allocated statically and simply re-initialized,
//      instead of allocating new copies on each call. However, as this function is not called very often,
//      it probably has negligible impact on performance.
func assignBuckets(e int32, leaders []int32) []*request.BucketGroup {

	// Convenience variables
	numPeers := membership.NumNodes()
	numLeaders := len(leaders)
	numBuckets := len(request.Buckets)
	isLeader := make(map[int32]bool, numPeers)
	for _, l := range leaders {
		isLeader[l] = true
	}

	// Create a list of bucket lists (one for each peer)
	assignedBuckets := make([][]int, numPeers)

	// Initialize an empty list of bucket IDs for each peer, even for non-leaders.
	for i := 0; i < numPeers; i++ {
		// The capacity is set to be sufficient for the leader peers.
		// +1 in the capacity is a rough compensation for a potential rounding error
		assignedBuckets[i] = make([]int, 0, numBuckets/numLeaders+1)
	}

	// Initially, assign buckets to all peers in a round-robin way, shifted by epoch number.
	for b := 0; b < numBuckets; b++ {
		peerID := (b + int(e)) % numPeers
		assignedBuckets[peerID] = append(assignedBuckets[peerID], b)
	}

	// Re-assign buckets of non-leaders to leaders.
	// Go through all bucket assignments.
	for i, bucketIDs := range assignedBuckets {

		// If the assignment is to a non-leader,
		if !isLeader[int32(i)] {

			// Re-distribute buckets to leaders
			for b := range bucketIDs {
				leader := leaders[(b+int(e))%numLeaders] // leader ID to which the bucket is assigned
				assignedBuckets[leader] = append(assignedBuckets[leader], b)
			}
		}
	}

	// For each bucket assignment to a leader, create a BucketGroup
	bucketGroups := make([]*request.BucketGroup, numLeaders)
	for i, l := range leaders {
		bucketGroups[i] = request.NewBucketGroup(assignedBuckets[l])
	}

	// Return the resulting bucketGroups
	return bucketGroups
}
