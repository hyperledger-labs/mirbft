/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type checkpoint struct {
	start         uint64
	end           uint64
	myConfig      *Config
	networkConfig *pb.NetworkConfig

	values         map[string][]NodeID
	committedValue []byte
	myValue        []byte
	stable         bool
	obsolete       bool
}

func newCheckpoint(start, end uint64, config *pb.NetworkConfig, myConfig *Config) *checkpoint {
	return &checkpoint{
		start:         start,
		end:           end,
		networkConfig: config,
		myConfig:      myConfig,
		values:        map[string][]NodeID{},
	}
}

func (cw *checkpoint) applyCheckpointMsg(source NodeID, value []byte) *Actions {
	checkpointValueNodes := append(cw.values[string(value)], source)
	cw.values[string(value)] = checkpointValueNodes

	agreements := len(checkpointValueNodes)

	if agreements == someCorrectQuorum(cw.networkConfig) {
		cw.committedValue = value
	}

	if source == NodeID(cw.myConfig.ID) {
		cw.myValue = value
	}

	// If I have completed this checkpoint, along with a quorum of the network, and I've not already run this path
	if cw.myValue != nil && cw.committedValue != nil && !cw.stable {
		if !bytes.Equal(value, cw.committedValue) {
			// TODO optionally handle this more gracefully, with state transfer (though this
			// indicates a violation of the byzantine assumptions)
			panic("my checkpoint disagrees with the committed network view of this checkpoint")
		}

		// This checkpoint has enough agreements, including my own, it may now be garbage collectable
		// Note, this must be >= (not ==) because my agreement could come after 2f+1 from the network.
		if agreements >= intersectionQuorum(cw.networkConfig) {
			cw.stable = true
		}
	}

	if len(checkpointValueNodes) == len(cw.networkConfig.Nodes) {
		cw.obsolete = true
	}

	return &Actions{}
}

func (cw *checkpoint) applyCheckpointResult(value []byte) *Actions {
	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Checkpoint{
					Checkpoint: &pb.Checkpoint{
						SeqNo: uint64(cw.end),
						Value: value,
					},
				},
			},
		},
	}
}

type CheckpointStatus struct {
	SeqNo          uint64
	PendingCommits int
	NetQuorum      bool
	LocalAgreement bool
}

func (cw *checkpoint) status() *CheckpointStatus {
	return &CheckpointStatus{
		SeqNo: cw.end,
		// XXX, populate pending commits
		NetQuorum:      cw.committedValue != nil,
		LocalAgreement: cw.committedValue != nil && bytes.Equal(cw.committedValue, cw.myValue),
	}
}
