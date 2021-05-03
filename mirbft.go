/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package mirbft is a consensus library, implementing the Mir BFT consensus protocol.
//
// This library can be used by applications which desire distributed, Byzantine fault
// tolerant consensus on the order of requests.
// Unlike many traditional consensus algorithms, Mir BFT is a multi-leader protocol,
// allowing throughput to scale with the number of nodes even over WANs,
// where the performance of traditional consensus algorithms begins to degrade.
package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

func StandardInitialNetworkState(nodeCount int, clientCount int) *msgs.NetworkState {
	nodes := []uint64{}
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, uint64(i))
	}

	numberOfBuckets := int32(nodeCount)
	checkpointInterval := numberOfBuckets * 5
	maxEpochLength := checkpointInterval * 10

	clients := make([]*msgs.NetworkState_Client, clientCount)
	for i := range clients {
		clients[i] = &msgs.NetworkState_Client{
			Id:           uint64(i),
			Width:        100,
			LowWatermark: 0,
		}
	}

	return &msgs.NetworkState{
		Config: &msgs.NetworkState_Config{
			Nodes:              nodes,
			F:                  int32((nodeCount - 1) / 3),
			NumberOfBuckets:    numberOfBuckets,
			CheckpointInterval: checkpointInterval,
			MaxEpochLength:     uint64(maxEpochLength),
		},
		Clients: clients,
	}
}
