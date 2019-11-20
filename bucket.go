/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type bucket struct {
	start SeqNo
	end   SeqNo

	epochConfig *epochConfig

	leader NodeID

	id BucketID

	// sequences are the current active sequence numbers in this bucket
	sequences map[SeqNo]*sequence
}

func newBucket(start, end SeqNo, config *epochConfig, bucketID BucketID) *bucket {
	sequences := map[SeqNo]*sequence{}
	for seqNo := start; seqNo <= end; seqNo++ {
		sequences[seqNo] = newSequence(config, seqNo, bucketID)
	}
	return &bucket{
		start:       start,
		end:         end,
		sequences:   sequences,
		id:          bucketID,
		epochConfig: config,
		leader:      config.buckets[bucketID],
	}
}

func (b *bucket) iAmLeader() bool {
	return b.leader == NodeID(b.epochConfig.myConfig.ID)
}

func (b *bucket) applyPreprepareMsg(seqNo SeqNo, batch [][]byte) *Actions {
	return b.sequences[seqNo].applyPreprepareMsg(batch)
}

func (b *bucket) applyDigestResult(seqNo SeqNo, digest []byte) *Actions {
	s := b.sequences[seqNo]
	actions := s.applyDigestResult(digest)
	if b.iAmLeader() {
		// We are the leader, no need to check ourselves for byzantine behavior
		// And no need to send the resulting prepare
		_ = s.applyValidateResult(true)
		return s.applyPrepareMsg(b.leader, digest)
	}
	return actions
}

func (b *bucket) applyValidateResult(seqNo SeqNo, valid bool) *Actions {
	s := b.sequences[seqNo]
	actions := s.applyValidateResult(valid)
	if !b.iAmLeader() {
		// We are not the leader, so let's apply a virtual prepare from
		// the leader that will not be sent, as there is no need to prepare
		actions.Append(s.applyPrepareMsg(b.leader, s.digest))
	}
	return actions
}

func (b *bucket) applyPrepareMsg(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.sequences[seqNo].applyPrepareMsg(source, digest)
}

func (b *bucket) applyCommitMsg(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.sequences[seqNo].applyCommitMsg(source, digest)
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
		sequences[i] = b.sequences[SeqNo(i)+b.start].state
	}
	return &BucketStatus{
		ID:        uint64(b.id),
		Leader:    b.iAmLeader(),
		Sequences: sequences,
	}
}
