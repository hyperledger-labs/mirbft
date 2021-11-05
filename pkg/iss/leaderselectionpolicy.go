/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import t "github.com/hyperledger-labs/mirbft/pkg/types"

// A LeaderSelectionPolicy implements the algorithm for selecting a set of leaders in each ISS epoch.
// In a nutshell, it gathers information about suspected leaders in the past epochs
// and uses it to calculate the set of leaders for future epochs.
// Its state can be updated using Suspect() and the leader set for an epoch is queried using Leaders().
// A leader set policy must be deterministic, i.e., calling Leaders() after the same sequence of Suspect() invocations
// always returns the same set of leaders at every Node.
type LeaderSelectionPolicy interface {

	// Leaders returns the (ordered) list of leaders based on the given epoch e and on the state of this policy object.
	Leaders(e t.EpochNr) []t.NodeID

	// Suspect updates the state of the policy object by announcing it that node `node` has been suspected in epoch `e`.
	Suspect(e t.EpochNr, node t.NodeID)
}

// The SimpleLeaderPolicy is a trivial leader selection policy.
// It must be initialized with a set of node IDs and always returns that full set as leaders,
// regardless of which nodes have been suspected. In other words, each node is leader each epoch with this policy.
type SimpleLeaderPolicy struct {
	Membership []t.NodeID
}

// Leaders always returns the whole membership for the SimpleLeaderPolicy. All nodes are always leaders.
func (simple *SimpleLeaderPolicy) Leaders(e t.EpochNr) []t.NodeID {
	// All nodes are always leaders.
	return simple.Membership
}

// Suspect does nothing for the SimpleLeaderPolicy.
func (simple *SimpleLeaderPolicy) Suspect(e t.EpochNr, node t.NodeID) {
	// Do nothing.
}
