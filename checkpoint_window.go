/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type checkpointWindow struct {
	start       uint64
	end         uint64
	myConfig    *Config
	epochConfig *epochConfig

	values             map[string][]NodeID
	committedValue     []byte
	myValue            []byte
	garbageCollectible bool // TODO, probably rename this to 'stable'
	obsolete           bool
}

func newCheckpointWindow(start, end uint64, config *epochConfig, myConfig *Config) *checkpointWindow {
	return &checkpointWindow{
		start:       start,
		end:         end,
		epochConfig: config,
		myConfig:    myConfig,
		values:      map[string][]NodeID{},
	}
}

func (cw *checkpointWindow) applyCheckpointMsg(source NodeID, value []byte) *Actions {
	checkpointValueNodes := append(cw.values[string(value)], source)
	cw.values[string(value)] = checkpointValueNodes

	agreements := len(checkpointValueNodes)

	if agreements == cw.epochConfig.someCorrectQuorum() {
		cw.committedValue = value
	}

	if source == NodeID(cw.myConfig.ID) {
		cw.myValue = value
	}

	// If I have completed this checkpoint, along with a quorum of the network, and I've not already run this path
	if cw.myValue != nil && cw.committedValue != nil && !cw.garbageCollectible {
		if !bytes.Equal(value, cw.committedValue) {
			// TODO optionally handle this more gracefully, with state transfer (though this
			// indicates a violation of the byzantine assumptions)
			panic("my checkpoint disagrees with the committed network view of this checkpoint")
		}

		// This checkpoint has enough agreements, including my own, it may now be garbage collectable
		// Note, this must be >= (not ==) because my agreement could come after 2f+1 from the network.
		if agreements >= cw.epochConfig.intersectionQuorum() {
			cw.garbageCollectible = true
		}

		// TODO, eventually, we should return the checkpoint value and set of attestations
		// to the caller, as they may want to do something with the set of attestations to preserve them.
	}

	if cw.garbageCollectible {
		if len(checkpointValueNodes) == len(cw.epochConfig.networkConfig.Nodes) {
			cw.obsolete = true
		}
	}

	return &Actions{}
}

func (cw *checkpointWindow) applyCheckpointResult(value []byte) *Actions {
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

func (cw *checkpointWindow) status() *CheckpointStatus {
	return &CheckpointStatus{
		SeqNo: cw.end,
		// XXX, populate pending commits
		NetQuorum:      cw.committedValue != nil,
		LocalAgreement: cw.committedValue != nil && bytes.Equal(cw.committedValue, cw.myValue),
	}
}
