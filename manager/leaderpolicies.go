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
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/membership"
)

type leaderPolicy interface {
	// GetLeaders returns the set of leaders in epoch e
	GetLeaders(e int32) []int32
	// Updates information about the leaders accoring to the specific policy.
	// The method receives a list of suspected nodes for epoch e.
	Update(e int32, suspect int32)
}

func NewLeaderPolicy(policyName string) leaderPolicy {
	switch policyName {
	case "Simple":
		return newSimpleLeaderPolicy()
	case "Single":
		return newSingleLeaderPolicy()
	case "Backoff":
		return newBackoffLeaderPolicy()
	case "Blacklist":
		return newBlacklistLeaderPolicy()
	case "Combined":
		return newCombinedLeaderPolicy()
	case "SimulatedRandomFailures":
		return newSimulatedRandomFailuresLeaderPolicy(config.Config.Failures, config.Config.RandomSeed)
	default:
		logger.Fatal().Msgf("Unsupported leader policy %s", config.Config.LeaderPolicy)
	}
	return nil
}

//============================================================
// Simple
//============================================================

//The SIMPLE leader selection policy always selects all nodes to be leaders in each epoch.
type simpleLeaderPolicy struct{}

func newSimpleLeaderPolicy() *simpleLeaderPolicy {
	return &simpleLeaderPolicy{}
}

func (sp *simpleLeaderPolicy) GetLeaders(e int32) []int32 {
	allNodeIDs := membership.AllNodeIDs()
	leadersCount := len(allNodeIDs) / config.Config.NodeToLeaderRatio
	leaders := make([]int32, 0, 0)
	leaders = append(leaders, allNodeIDs[:leadersCount]...)
	return leaders
}

func (sp *simpleLeaderPolicy) Update(e int32, suspect int32) {
	// Nothing needs to be done for the simple policy
}

//============================================================
// Single
//============================================================

type singleLeaderPolicy struct{}

func newSingleLeaderPolicy() *singleLeaderPolicy {
	return &singleLeaderPolicy{}
}

func (sp *singleLeaderPolicy) GetLeaders(e int32) []int32 {
	allNodeIDs := membership.AllNodeIDs()
	return []int32{allNodeIDs[int(e)%len(allNodeIDs)]}
}

func (sp *singleLeaderPolicy) Update(e int32, suspect int32) {
	// Nothing needs to be done for the single policy
}

//============================================================
// Backoff
//============================================================

// BACKOFF leader selection policy bans a suspected leader from the leader set for a certain number of epochs.
type backoffLeaderPolicy struct {
	// A map from each node id to their ban period. Initially the ban period is the default for all nodes.
	// The ban period doubles each time a node is suspected.
	// TODO: Should we reduce ban period after "good" behavior?
	ban map[int32]int32
	// For each peer, the epoch starting from which the peer can be leader again.
	bannedUntil map[int32]int32
}

func newBackoffLeaderPolicy() *backoffLeaderPolicy {
	return &backoffLeaderPolicy{
		ban:         make(map[int32]int32),
		bannedUntil: make(map[int32]int32),
	}
}

func (bp *backoffLeaderPolicy) Update(e int32, suspect int32) {
	if _, ok := bp.ban[suspect]; !ok {
		// Use default ban if the peer has not been suspected before.
		bp.ban[suspect] = int32(config.Config.DefaultLeaderBan)
	} else {
		// Double the penalty for nodes that have alrady been suspected.
		bp.ban[suspect] = bp.ban[suspect] * 2
	}
	bp.bannedUntil[suspect] = e + bp.ban[suspect]
	logger.Info().Int32("epoch", e).
		Int32("id", suspect).
		Int32("until", bp.bannedUntil[suspect]).
		Msg("Banning suspect.")

	// Make sure that the next epoch has at least one leader by decreasing everybody's bans if necessary.
	// This is equivalent to skipping epochs with 0 leaders.
	for len(bp.GetLeaders(e+1)) == 0 {
		for leader, until := range bp.bannedUntil {
			bp.bannedUntil[leader] = until - 1
		}
	}
}

func (bp *backoffLeaderPolicy) GetLeaders(e int32) []int32 {
	leaders := make([]int32, 0, 0)
	allNodeIDs := membership.AllNodeIDs()

	for _, id := range allNodeIDs {
		// Select leaders that are 1) not banned (!ok) or 2) their ban expired (bannedUntil < e).
		if bannedUntil, ok := bp.bannedUntil[id]; !ok || bannedUntil < e {
			leaders = append(leaders, id)
		}
	}

	return leaders
}

//============================================================
// Blacklist
//============================================================

// BLACKLIST leader selection policy never excludes more than f nodes from the leader set.
type blacklistLeaderPolicy struct {
	bannedList []int32
	bannedMap  map[int32]bool
}

func newBlacklistLeaderPolicy() *blacklistLeaderPolicy {
	return &blacklistLeaderPolicy{
		bannedList: make([]int32, 0),
		bannedMap:  make(map[int32]bool),
	}
}

func (bp *blacklistLeaderPolicy) Update(e int32, suspect int32) {
	bp.bannedList = append(bp.bannedList, suspect)

	if len(bp.bannedList) > membership.Faults() {
		bp.bannedList = bp.bannedList[len(bp.bannedList)-membership.Faults():]
	}

	bp.bannedMap = make(map[int32]bool)
	for _, leader := range bp.bannedList {
		bp.bannedMap[leader] = true
	}
}

func (bp *blacklistLeaderPolicy) GetLeaders(e int32) []int32 {
	leaders := make([]int32, 0, 0)

	for _, leader := range membership.AllNodeIDs() {
		if !bp.bannedMap[leader] {
			leaders = append(leaders, leader)
		}
	}

	return leaders
}

//============================================================
// Combined
//============================================================

// COMBINED leader selection policy combines the BACKOFF and the BLACKLIST policies
// by including a node in the leader set iff either of these two policies includes it.
type combinedLeaderPolicy struct {
	backoff   *backoffLeaderPolicy
	blacklist *blacklistLeaderPolicy
}

func newCombinedLeaderPolicy() leaderPolicy {
	return &combinedLeaderPolicy{
		backoff:   newBackoffLeaderPolicy(),
		blacklist: newBlacklistLeaderPolicy(),
	}
}

func (cp *combinedLeaderPolicy) Update(e int32, suspect int32) {
	cp.backoff.Update(e, suspect)
	cp.blacklist.Update(e, suspect)
}

func (cp *combinedLeaderPolicy) GetLeaders(e int32) []int32 {

	// Create map of leaders from both policies.
	leaderMap := make(map[int32]bool)
	for _, leader := range cp.backoff.GetLeaders(e) {
		leaderMap[leader] = true
	}
	for _, leader := range cp.blacklist.GetLeaders(e) {
		leaderMap[leader] = true
	}

	// Create a list of leaders based on the map.
	leaders := make([]int32, 0, len(leaderMap))
	for leader, _ := range leaderMap {
		leaders = append(leaders, leader)
	}

	return leaders
}

//============================================================
// SimulatedRandomFailures
//============================================================

// SIMULATEDRANDOMFAILURES leader selection policy simulates the state of the system after the crash of some peers
// and their eviction from the leader set.
type simulatedRandomFailuresLeaderPolicy struct {
}

func newSimulatedRandomFailuresLeaderPolicy(failures int, seed int64) *simulatedRandomFailuresLeaderPolicy {
	return &simulatedRandomFailuresLeaderPolicy{}
}

func (srfp *simulatedRandomFailuresLeaderPolicy) Update(e int32, suspect int32) {
	// Do nothing.
}

func (srfp *simulatedRandomFailuresLeaderPolicy) GetLeaders(e int32) []int32 {
	return membership.CorrectPeers()
}
