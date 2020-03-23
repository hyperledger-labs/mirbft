/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type checkpointTracker struct {
	highestCheckpoint map[NodeID]*checkpoint

	// checkpoints is a map of bounded size.  The map maintains a checkpoint
	// so long as it is the highest checkpoint for some node, or, is currently
	// within the watermarks.
	checkpoints map[uint64]*checkpoint

	networkConfig *pb.NetworkConfig
	myConfig      *Config
}

func newCheckpointTracker(networkConfig *pb.NetworkConfig, myConfig *Config) *checkpointTracker {
	return &checkpointTracker{
		highestCheckpoint: map[NodeID]*checkpoint{}, // TODO, implement
		checkpoints:       map[uint64]*checkpoint{},
		networkConfig:     networkConfig,
		myConfig:          myConfig,
	}
}

func (ct *checkpointTracker) checkpoint(seqNo uint64) *checkpoint {
	cp, ok := ct.checkpoints[seqNo]
	if !ok {
		cp = newCheckpoint(seqNo-uint64(ct.networkConfig.CheckpointInterval), seqNo, ct.networkConfig, ct.myConfig)
		ct.checkpoints[seqNo] = cp
	}

	return cp
}

func (ct *checkpointTracker) applyCheckpointMsg(source NodeID, seqNo uint64, value []byte) bool {
	cp := ct.checkpoint(seqNo)

	return cp.applyCheckpointMsg(source, value)
}

func (ct *checkpointTracker) applyCheckpointResult(seqNo uint64, value []byte) *Actions {
	return ct.checkpoints[seqNo].applyCheckpointResult(value)
}

func (ct *checkpointTracker) release(cp *checkpoint) {
	delete(ct.checkpoints, cp.end)
}

func (ct *checkpointTracker) status() []*CheckpointStatus {
	result := make([]*CheckpointStatus, len(ct.checkpoints))
	i := 0
	for _, cp := range ct.checkpoints {
		result[i] = cp.status()
		i++
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SeqNo < result[j].SeqNo
	})

	return result
}

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

func (cw *checkpoint) applyCheckpointMsg(source NodeID, value []byte) bool {
	stateChange := false

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
			stateChange = true
		}
	}

	if len(checkpointValueNodes) == len(cw.networkConfig.Nodes) {
		cw.obsolete = true
		stateChange = true
	}

	return stateChange
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

func (cw *checkpoint) status() *CheckpointStatus {
	maxAgreements := 0
	for _, nodes := range cw.values {
		if len(nodes) > maxAgreements {
			maxAgreements = len(nodes)
		}
	}
	return &CheckpointStatus{
		SeqNo:         cw.end,
		MaxAgreements: maxAgreements,
		NetQuorum:     cw.committedValue != nil,
		LocalDecision: cw.myValue != nil,
	}
}
