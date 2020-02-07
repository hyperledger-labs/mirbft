/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import pb "github.com/IBM/mirbft/mirbftpb"

type bucket struct {
	myConfig *Config

	start uint64
	end   uint64

	epochConfig *epochConfig

	leader NodeID

	id BucketID

	ticksSinceProgress int

	// sequences are the current active sequence numbers in this bucket
	// it is indexed by column, where column is derived from sequence
	// and bucketID
	sequences map[uint64]*sequence
}

func newBucket(start, end uint64, config *epochConfig, myConfig *Config, bucketID BucketID) *bucket {
	sequences := map[uint64]*sequence{}
	for seqNo := start; seqNo <= end; seqNo++ {
		sequences[seqNo] = newSequence(config, myConfig, &Entry{
			SeqNo: seqNo,
			Epoch: config.number,
		})
	}
	return &bucket{
		myConfig:    myConfig,
		start:       start,
		end:         end,
		sequences:   sequences,
		id:          bucketID,
		epochConfig: config,
		leader:      config.buckets[bucketID],
	}
}

func (b *bucket) iAmLeader() bool {
	return b.leader == NodeID(b.myConfig.ID)
}

func (b *bucket) applyPreprepareMsg(column uint64, batch [][]byte) *Actions {
	b.ticksSinceProgress = 0
	return b.sequences[column].applyPreprepareMsg(batch)
}

func (b *bucket) applyDigestResult(column uint64, digest []byte) *Actions {
	s := b.sequences[column]
	actions := s.applyDigestResult(digest)
	if b.iAmLeader() {
		// We are the leader, no need to check ourselves for byzantine behavior
		// And no need to send the resulting prepare
		_ = s.applyValidateResult(true)
		return s.applyPrepareMsg(b.leader, digest)
	}
	return actions
}

func (b *bucket) applyValidateResult(column uint64, valid bool) *Actions {
	s := b.sequences[column]
	actions := s.applyValidateResult(valid)
	if !b.iAmLeader() {
		// We are not the leader, so let's apply a virtual prepare from
		// the leader that will not be sent, as there is no need to prepare
		actions.Append(s.applyPrepareMsg(b.leader, s.digest))
	}
	return actions
}

func (b *bucket) applyPrepareMsg(source NodeID, column uint64, digest []byte) *Actions {
	b.ticksSinceProgress = 0
	return b.sequences[column].applyPrepareMsg(source, digest)
}

func (b *bucket) applyCommitMsg(source NodeID, column uint64, digest []byte) *Actions {
	b.ticksSinceProgress = 0
	return b.sequences[column].applyCommitMsg(source, digest)
}

func (b *bucket) tick() *Actions {
	b.ticksSinceProgress++
	if b.ticksSinceProgress > b.myConfig.SuspectTicks {
		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_Suspect{
						Suspect: &pb.Suspect{
							Epoch: b.epochConfig.number,
						},
					},
				},
			},
		}
	}
	return &Actions{}
}

// BucketStatus represents the current
type BucketStatus struct {
	ID        uint64
	Leader    bool
	Sequences []SequenceState
}

func (b *bucket) status() *BucketStatus {
	sequences := make([]SequenceState, int(b.end-b.start+1))
	for i := range sequences {
		sequences[i] = b.sequences[uint64(i)+b.start].state
	}
	return &BucketStatus{
		ID:        uint64(b.id),
		Leader:    b.iAmLeader(),
		Sequences: sequences,
	}
}
