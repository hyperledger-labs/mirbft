/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// The Config type defines all the ISS configuration parameters.
// Note that some fields specify delays in ticks of the logical clock.
// To obtain real time delays, these need to be multiplied by the period of the ticker provided to the Node at runtime.
type Config struct {

	// The IDs of all nodes that execute the protocol.
	// Must not be empty.
	Membership []t.NodeID

	// The length of an ISS segment, in sequence numbers.
	// This is the number of commitLog entries each orderer needs to output in an epoch.
	// Depending on the number of leaders (and thus orderers), this will result in epoch of different lengths.
	// If set to 0, the EpochLength parameter must be non-zero and will be used to calculate the length of the segments
	// such that their lengths sum up to EpochLength.
	// Must not be negative.
	SegmentLength int

	// The length of an ISS epoch, in sequence numbers.
	// If EpochLength is non-zero, the epoch will always have a fixed
	// length, regardless of the number of leaders.
	// In each epoch, the corresponding segment lengths will be calculated to sum up to EpochLength,
	// potentially resulting in different segment length across epochs as well as within an epoch.
	// If set to zero, SegmentLength must be non-zero and will be used directly to set the length of each segment.
	// Must not be negative.
	// TODO: That EpochLength is not implemented now. SegmentLength has to be used.
	EpochLength int

	// The maximal number of requests in a proposed request batch.
	// As soon as the buckets assigned to an orderer contain MaxBatchSize requests,
	// the orderer may decide to immediately propose a new request batch.
	// This is meaningful, for example, for the PBFT orderer.
	// Setting MaxBatchSize to zero signifies no limit on batch size.
	// TODO: Consider making this orderer-specific.
	MaxBatchSize t.NumRequests

	// The maximum number of logical time ticks between two proposals of an orderer, where applicable.
	// For orderers that wait for a request batch to fill before proposing it (e.g. PBFT),
	// This parameter caps the waiting time in order to bound latency.
	// When MaxProposeDelay ticks have elapsed since the last proposal made by an orderer,
	// the orderer proposes a new request batch, even if the batch is not full (or even completely empty).
	// Must not be negative.
	MaxProposeDelay int

	// Total number of buckets used by ISS.
	// In each epoch, these buckets are re-distributed evenly among the orderers.
	// Must be greater than 0.
	NumBuckets int

	// The logic for selecting leader nodes in each epoch.
	// For details see the documentation of the LeaderSelectionPolicy type.
	// ATTENTION: The leader selection policy is stateful!
	// Must not be nil.
	LeaderPolicy LeaderSelectionPolicy

	// Number of logical time ticks to wait until demanding retransmission of missing requests.
	// If a node receives a proposal containing requests that are not in the node's buckets,
	// it cannot accept the proposal.
	// In such a case, the node will wait for RequestNAckTimeout ticks
	// before trying to fetch those requests from other nodes.
	// Must be positive.
	RequestNAckTimeout int
}

// CheckConfig checks whether the given configuration satisfies all necessary constraints.
func CheckConfig(c *Config) error {

	// The membership must not be empty.
	if len(c.Membership) == 0 {
		return fmt.Errorf("empty membership")
	}

	// Segment length must not be negative.
	if c.SegmentLength < 0 {
		return fmt.Errorf("negative SegmentLength: %d", c.SegmentLength)
	}

	// Epoch length must not be negative.
	if c.EpochLength < 0 {
		return fmt.Errorf("negative EpochLength: %d", c.EpochLength)
	}

	// Exactly one of SegmentLength and EpochLength must be zero.
	// (The one that is zero is computed based on the non-zero one.)
	if (c.EpochLength != 0 && c.SegmentLength != 0) || (c.EpochLength == 0 && c.SegmentLength == 0) {
		return fmt.Errorf("conflicting EpochLength (%d) and SegmentLength (%d) (exactly one must be zero)",
			c.EpochLength, c.SegmentLength)
	}

	// MaxBatchSize must not be negative (it can be zero though, signifying infinite batch size).
	if c.MaxBatchSize < 0 {
		return fmt.Errorf("negative MaxBatchSize: %d", c.MaxBatchSize)
	}

	// MaxProposeDelay must not be negative.
	if c.MaxProposeDelay < 0 {
		return fmt.Errorf("negative MaxProposeDelay: %d", c.MaxProposeDelay)
	}

	// There must be at least one bucket.
	if c.NumBuckets <= 0 {
		return fmt.Errorf("non-positive number of buckets: %d", c.NumBuckets)
	}

	// There must be a leader selection policy.
	if c.LeaderPolicy == nil {
		return fmt.Errorf("missing leader selection policy")
	}

	// RequestNackTimeout must be positive
	if c.RequestNAckTimeout <= 0 {
		return fmt.Errorf("non-positive RequestNAckTimeout: %d", c.RequestNAckTimeout)
	}

	// If all checks passed, return nil error.
	return nil
}

// DefaultConfig returns the default configuration for a given membership.
// There is no guarantee that this configuration ensures good performance, but it will pass the CheckConfig test.
// DefaultConfig is intended for use during testing and hello-world examples.
// A proper deployment is expected to craft a custom configuration,
// for which DefaultConfig can serve as a starting point.
func DefaultConfig(membership []t.NodeID) *Config {
	return &Config{
		Membership:         membership,
		SegmentLength:      10,
		MaxBatchSize:       4,
		MaxProposeDelay:    2,
		NumBuckets:         len(membership),
		LeaderPolicy:       &SimpleLeaderPolicy{Membership: membership},
		RequestNAckTimeout: 16,
	}
}
