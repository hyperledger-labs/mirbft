/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"bytes"

	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type CheckpointWindow struct {
	Number      SeqNo
	EpochConfig *EpochConfig

	PendingCommits     map[BucketID]struct{}
	Values             map[string][]NodeAttestation
	CommittedValue     []byte
	MyValue            []byte
	GarbageCollectible bool
}

type NodeAttestation struct {
	Node        NodeID
	Attestation []byte
}

func NewCheckpointWindow(number SeqNo, config *EpochConfig) *CheckpointWindow {
	pendingCommits := map[BucketID]struct{}{}
	for bucketID := range config.Buckets {
		pendingCommits[bucketID] = struct{}{}
	}

	return &CheckpointWindow{
		Number:         number,
		EpochConfig:    config,
		PendingCommits: pendingCommits,
		Values:         map[string][]NodeAttestation{},
	}
}

func (cw *CheckpointWindow) Committed(bucket BucketID) *consumer.Actions {
	delete(cw.PendingCommits, bucket)
	if len(cw.PendingCommits) > 0 {
		return &consumer.Actions{}
	}
	return &consumer.Actions{
		Checkpoint: []uint64{uint64(cw.Number)},
	}
}

func (cw *CheckpointWindow) ApplyCheckpointMsg(source NodeID, value, attestation []byte) *consumer.Actions {
	checkpointValueNodes := append(cw.Values[string(value)], NodeAttestation{
		Node:        source,
		Attestation: attestation,
	})
	cw.Values[string(value)] = checkpointValueNodes
	if len(checkpointValueNodes) == 2*cw.EpochConfig.F+1 {
		cw.CommittedValue = value
	}

	if source == NodeID(cw.EpochConfig.MyConfig.ID) {
		cw.MyValue = value
	}

	// If I have completed this checkpoint, along with a quorum of the network, and I've not already run this path
	if cw.MyValue != nil && cw.CommittedValue != nil && !cw.GarbageCollectible {
		if !bytes.Equal(value, cw.CommittedValue) {
			// TODO optionally handle this more gracefully, with state transfer (though this
			// indicates a violation of the byzantine assumptions)
			panic("my checkpoint disagrees with the committed network view of this checkpoint")
		}

		// This checkpoint has 2f+1 agreements, including my own, it may now be garbage collectable
		cw.GarbageCollectible = true

		// TODO, eventually, we should return the checkpoint value and set of attestations
		// to the caller, as they may want to do something with the set of attestations to preserve them.
	}

	return &consumer.Actions{}
}

func (cw *CheckpointWindow) ApplyCheckpointResult(value, attestation []byte) *consumer.Actions {
	return &consumer.Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Checkpoint{
					Checkpoint: &pb.Checkpoint{
						SeqNo:       uint64(cw.Number),
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
	PendingCommits int
	NetQuorum      bool
	LocalAgreement bool
}

func (cw *CheckpointWindow) Status() *CheckpointStatus {
	defer func() {
		if r := recover(); r != nil {
			panic(cw.Number)
		}
	}()
	return &CheckpointStatus{
		SeqNo:          uint64(cw.Number),
		PendingCommits: len(cw.PendingCommits),
		NetQuorum:      cw.CommittedValue != nil,
		LocalAgreement: cw.CommittedValue != nil && bytes.Equal(cw.CommittedValue, cw.MyValue),
	}
}
