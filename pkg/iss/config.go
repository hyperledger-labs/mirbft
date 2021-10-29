/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type Config struct {
	Membership    []t.NodeID
	SegmentLength int // In sequence numbers
	// TODO: That EpochLength is not implemented now.
	EpochLength        int           // In sequence numbers
	MaxBatchSize       t.NumRequests // In requests
	MaxProposeDelay    int           // In ticks
	NumBuckets         int
	LeaderPolicy       LeaderSelectionPolicy // ATTENTION: The leader selection policy is stateful!
	RequestNAckTimeout int                   // In ticks
}

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
