/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import t "github.com/hyperledger-labs/mirbft/pkg/types"

type LeaderSelectionPolicy interface {
	Leaders(e t.EpochNr) []t.NodeID
	Suspect(e t.EpochNr, node t.NodeID)
}

type SimpleLeaderPolicy struct {
	Membership []t.NodeID
}

func (simple *SimpleLeaderPolicy) Leaders(e t.EpochNr) []t.NodeID {
	// All nodes are always leaders.
	return simple.Membership
}

func (simple *SimpleLeaderPolicy) Suspect(e t.EpochNr, node t.NodeID) {
	// Do nothing.
}
