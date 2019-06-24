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
	number      SeqNo
	epochConfig *epochConfig

	pendingCommits     map[BucketID]struct{}
	values             map[string][]nodeAttestation
	committedValue     []byte
	myValue            []byte
	garbageCollectible bool
	obsolete           bool
}

type nodeAttestation struct {
	node        NodeID
	attestation []byte
}

func newCheckpointWindow(number SeqNo, config *epochConfig) *checkpointWindow {
	pendingCommits := map[BucketID]struct{}{}
	for bucketID := range config.buckets {
		pendingCommits[bucketID] = struct{}{}
	}

	return &checkpointWindow{
		number:         number,
		epochConfig:    config,
		pendingCommits: pendingCommits,
		values:         map[string][]nodeAttestation{},
	}
}

func (cw *checkpointWindow) Committed(bucket BucketID) *Actions {
	delete(cw.pendingCommits, bucket)
	if len(cw.pendingCommits) > 0 {
		return &Actions{}
	}
	return &Actions{
		Checkpoint: []uint64{uint64(cw.number)},
	}
}

func (cw *checkpointWindow) applyCheckpointMsg(source NodeID, value, attestation []byte) *Actions {
	checkpointValueNodes := append(cw.values[string(value)], nodeAttestation{
		node:        source,
		attestation: attestation,
	})
	cw.values[string(value)] = checkpointValueNodes
	if len(checkpointValueNodes) == 2*cw.epochConfig.f+1 {
		cw.committedValue = value
	}

	if source == NodeID(cw.epochConfig.myConfig.ID) {
		cw.myValue = value
	}

	// If I have completed this checkpoint, along with a quorum of the network, and I've not already run this path
	if cw.myValue != nil && cw.committedValue != nil && !cw.garbageCollectible {
		if !bytes.Equal(value, cw.committedValue) {
			// TODO optionally handle this more gracefully, with state transfer (though this
			// indicates a violation of the byzantine assumptions)
			panic("my checkpoint disagrees with the committed network view of this checkpoint")
		}

		// This checkpoint has 2f+1 agreements, including my own, it may now be garbage collectable
		cw.garbageCollectible = true

		// TODO, eventually, we should return the checkpoint value and set of attestations
		// to the caller, as they may want to do something with the set of attestations to preserve them.
	}

	if cw.garbageCollectible {
		if len(checkpointValueNodes) == len(cw.epochConfig.nodes) {
			cw.obsolete = true
		}
	}

	return &Actions{}
}

func (cw *checkpointWindow) applyCheckpointResult(value, attestation []byte) *Actions {
	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Checkpoint{
					Checkpoint: &pb.Checkpoint{
						SeqNo:       uint64(cw.number),
						Value:       value,
						Attestation: attestation,
					},
				},
			},
		},
	}
}

type CheckpointStatus struct {
	SeqNo          uint64
	pendingCommits int
	NetQuorum      bool
	LocalAgreement bool
}

func (cw *checkpointWindow) status() *CheckpointStatus {
	return &CheckpointStatus{
		SeqNo:          uint64(cw.number),
		pendingCommits: len(cw.pendingCommits),
		NetQuorum:      cw.committedValue != nil,
		LocalAgreement: cw.committedValue != nil && bytes.Equal(cw.committedValue, cw.myValue),
	}
}
