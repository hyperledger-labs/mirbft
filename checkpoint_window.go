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
	start       SeqNo
	end         SeqNo
	epochConfig *epochConfig

	buckets map[BucketID]*bucket

	outstandingBuckets map[BucketID]struct{}
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

func newCheckpointWindow(start, end SeqNo, config *epochConfig) *checkpointWindow {
	outstandingBuckets := map[BucketID]struct{}{}

	buckets := map[BucketID]*bucket{}
	for bucketID := range config.buckets {
		outstandingBuckets[bucketID] = struct{}{}
		buckets[bucketID] = newBucket(start, end, config, bucketID)
	}

	return &checkpointWindow{
		start:              start,
		end:                end,
		epochConfig:        config,
		outstandingBuckets: outstandingBuckets,
		buckets:            buckets,
		values:             map[string][]nodeAttestation{},
	}
}

func (cw *checkpointWindow) preprepare(source NodeID, seqNo SeqNo, bucket BucketID, batch [][]byte) *Actions {
	return cw.buckets[bucket].applyPreprepare(seqNo, batch)
}

func (cw *checkpointWindow) prepare(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return cw.buckets[bucket].applyPrepare(source, seqNo, digest)
}

func (cw *checkpointWindow) commit(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return cw.buckets[bucket].applyCommit(source, seqNo, digest)
}

func (cw *checkpointWindow) digest(seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return cw.buckets[bucket].applyDigestResult(seqNo, digest)
}

func (cw *checkpointWindow) validate(seqNo SeqNo, bucket BucketID, valid bool) *Actions {
	return cw.buckets[bucket].applyValidateResult(seqNo, valid)
}

func (cw *checkpointWindow) committed(bucket BucketID) *Actions {
	delete(cw.outstandingBuckets, bucket)
	if len(cw.outstandingBuckets) > 0 {
		return &Actions{}
	}
	return &Actions{
		Checkpoint: []uint64{uint64(cw.end)},
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
						SeqNo:       uint64(cw.end),
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

func (cw *checkpointWindow) status() *CheckpointStatus {
	return &CheckpointStatus{
		SeqNo:          uint64(cw.end),
		PendingCommits: len(cw.outstandingBuckets),
		NetQuorum:      cw.committedValue != nil,
		LocalAgreement: cw.committedValue != nil && bytes.Equal(cw.committedValue, cw.myValue),
	}
}
