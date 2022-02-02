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

package membership

import (
	"math/rand"
	"sort"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

var (
	// TODO: Implement proper client public keys, instead of this hard-coded one.
	clientPubKey interface{} = nil

	// ID of this peer. Initialized by the main program.
	// OwnID is only used by peers, not clients.
	// There is only one global OwnID, but potentially several clients that can run in one process.
	OwnID int32 = -1

	// Peer own private key
	// TODO convert this to interface{} so it is decode form bytes only once on peer registration
	// TODO and not all the time
	OwnPrivKey []byte

	// Peer own private threshold BLS key share
	TBLSPrivKeyShare *crypto.TBLSPrivKeyShare

	// Public key of the BLS threshold cryptosystem
	TBLSPublicKey *crypto.TBLSPubKey

	// All known node identities, indexed by node ID.
	nodeIdentities map[int32]*pb.NodeIdentity

	// Ordered list of node IDs. All nodes must have the same view of this.
	nodeIDs []int32

	SimulatedCrashes map[int32]*pb.NodeIdentity
)

// Initializes the Client key (to be changed at some point).
// Cannot be part of the init() function, as the configuration file is not yet loaded when init() is executed.
func Init() {

	// TODO: Here we just use a pre-shared client key for verification of requests.
	//       Implement each client sending its own key during client handshake.
	if config.Config.ClientPubKeyFile != "" {
		if pk, err := crypto.PublicKeyFromFile(config.Config.ClientPubKeyFile); err == nil {
			clientPubKey = pk
		} else {
			logger.Error().
				Err(err).
				Str("keyFile", config.Config.ClientPubKeyFile).
				Msg("Could not load client public key.")
		}
	}

	SimulatedCrashes = make(map[int32]*pb.NodeIdentity)
}

// Initializes the known node identities.
func InitNodeIdentities(identities []*pb.NodeIdentity) {

	// Allocate memory for data structures
	nodeIdentities = make(map[int32]*pb.NodeIdentity, len(identities))
	nodeIDs = make([]int32, 0, len(identities))

	// Create indexes
	for _, identity := range identities {
		nodeIdentities[identity.NodeId] = identity
		nodeIDs = append(nodeIDs, identity.NodeId)
	}

	// Sort node IDs, such that each peer has a consistent view of them
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	// Initialize the map of peers that simulate a crash.
	// This is only used for benchmarking purposes.
	// Shuffle the order of all peer IDs.
	allNodeIDs := AllNodeIDs()
	r := rand.New(rand.NewSource(config.Config.RandomSeed))
	r.Shuffle(len(allNodeIDs), func(i, j int) {
		allNodeIDs[i], allNodeIDs[j] = allNodeIDs[j], allNodeIDs[i]
	})
	for _, p := range allNodeIDs[:config.Config.Failures] {
		SimulatedCrashes[p] = nodeIdentities[p]
	}
}

// Return full node identity
func NodeIdentity(nodeID int32) *pb.NodeIdentity {
	return nodeIdentities[nodeID]
}

func SimulatesCrash(peerID int32) bool {
	_, ok := SimulatedCrashes[peerID]
	return ok
}

func CorrectPeers() []int32 {
	correct := make([]int32, 0, 0)
	for _, p := range AllNodeIDs() {
		if !SimulatesCrash(p) {
			correct = append(correct, p)
		}
	}
	return correct
}

// Return an ordered list of IDs of all known nodes.
// Every node has a consistent view of this.
// (At least for now. This might change if we start experimenting with dynamic membership.)
func AllNodeIDs() []int32 {
	// Return a copy of the data, so the caller cannot change the ordering
	c := make([]int32, len(nodeIDs), len(nodeIDs))
	copy(c, nodeIDs)
	return c
}

// Returns the total number of nodes.
func NumNodes() int {
	return len(nodeIdentities)
}

// Returns the the maximum number of faults
func Faults() int {
	return (NumNodes() - 1) / 3
}

// TODO: Implement proper client public keys, instead of this hard-coded one.
func ClientPubKey(clID int32) interface{} {
	return clientPubKey
}

func WeakQuorum() int {
	return Faults() + 1
}

func Quorum() int {
	return 2*Faults() + 1
}
