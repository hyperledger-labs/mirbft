/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type bucket struct {
	epochConfig *epochConfig

	leader NodeID

	id BucketID

	// sequences are the current active sequence numbers in this bucket
	sequences map[SeqNo]*sequence
}

func newBucket(config *epochConfig, bucketID BucketID) *bucket {
	sequences := map[SeqNo]*sequence{}
	b := &bucket{
		leader:      config.buckets[bucketID],
		id:          bucketID,
		epochConfig: config,
		sequences:   sequences,
	}
	b.moveWatermarks()
	return b
}

func (b *bucket) moveWatermarks() {
	// XXX this is a pretty obviously suboptimal way of moving watermarks,
	// we know they're in order, so iterating through all sequences twice
	// is wasteful, but it's easy to show it's correct, so implementing naively for now

	for seqNo := range b.sequences {
		if seqNo < b.epochConfig.lowWatermark {
			delete(b.sequences, seqNo)
		}
	}

	for i := b.epochConfig.lowWatermark; i <= b.epochConfig.highWatermark; i++ {
		if _, ok := b.sequences[i]; !ok {
			b.sequences[i] = newSequence(b.epochConfig, i, b.id)
		}
	}
}

func (b *bucket) iAmLeader() bool {
	return b.leader == NodeID(b.epochConfig.myConfig.ID)
}

func (b *bucket) applyPreprepare(seqNo SeqNo, batch [][]byte) *Actions {
	return b.sequences[seqNo].applyPreprepare(batch)
}

func (b *bucket) applyDigestResult(seqNo SeqNo, digest []byte) *Actions {
	s := b.sequences[seqNo]
	actions := s.applyDigestResult(digest)
	if b.iAmLeader() {
		// We are the leader, no need to check ourselves for byzantine behavior
		// And no need to send the resulting prepare
		_ = s.applyValidateResult(true)
		return s.applyPrepare(b.leader, digest)
	}
	return actions
}

func (b *bucket) applyValidateResult(seqNo SeqNo, valid bool) *Actions {
	s := b.sequences[seqNo]
	actions := s.applyValidateResult(valid)
	if !b.iAmLeader() {
		// We are not the leader, so let's apply a virtual prepare from
		// the leader that will not be sent, as there is no need to prepare
		actions.Append(s.applyPrepare(b.leader, s.digest))
	}
	return actions
}

func (b *bucket) applyPrepare(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.sequences[seqNo].applyPrepare(source, digest)
}

func (b *bucket) applyCommit(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.sequences[seqNo].applyCommit(source, digest)
}

// BucketStatus represents the current
type BucketStatus struct {
	ID        uint64
	Leader    bool
	Sequences []SequenceState
}

func (b *bucket) status() *BucketStatus {
	sequences := make([]SequenceState, int(b.epochConfig.highWatermark-b.epochConfig.lowWatermark)+1)
	for i := range sequences {
		sequences[i] = b.sequences[SeqNo(i)+b.epochConfig.lowWatermark].state
	}
	return &BucketStatus{
		ID:        uint64(b.id),
		Leader:    b.iAmLeader(),
		Sequences: sequences,
	}
}
