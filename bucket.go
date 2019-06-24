/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type BucketID uint64
type SeqNo uint64
type NodeID uint64

type Bucket struct {
	EpochConfig *EpochConfig

	Leader NodeID
	ID     BucketID

	// Sequences are the current active sequence numbers in this bucket
	Sequences map[SeqNo]*Sequence

	NextAssigned   SeqNo
	NextPreprepare SeqNo

	// The variables below are only set if this Bucket is led locally
	Queue     [][]byte
	SizeBytes int
	Pending   [][][]byte
}

func NewBucket(config *EpochConfig, bucketID BucketID) *Bucket {
	sequences := map[SeqNo]*Sequence{}
	b := &Bucket{
		Leader:       config.Buckets[bucketID],
		ID:           bucketID,
		EpochConfig:  config,
		Sequences:    sequences,
		NextAssigned: config.LowWatermark + 1,
	}
	b.MoveWatermarks()
	return b
}

func (b *Bucket) MoveWatermarks() {
	// XXX this is a pretty obviously suboptimal way of moving watermarks,
	// we know they're in order, so iterating through all sequences twice
	// is wasteful, but it's easy to show it's correct, so implementing naively for now

	for seqNo := range b.Sequences {
		if seqNo < b.EpochConfig.LowWatermark {
			delete(b.Sequences, seqNo)
		}
	}

	for i := b.EpochConfig.LowWatermark; i <= b.EpochConfig.HighWatermark; i++ {
		if _, ok := b.Sequences[i]; !ok {
			b.Sequences[i] = NewSequence(b.EpochConfig, i, b.ID)
		}
	}
}

func (b *Bucket) IAmLeader() bool {
	return b.Leader == NodeID(b.EpochConfig.MyConfig.ID)
}

func (b *Bucket) ApplyPreprepare(seqNo SeqNo, batch [][]byte) *Actions {
	b.NextPreprepare = seqNo + 1
	return b.Sequences[seqNo].ApplyPreprepare(batch)
}

func (b *Bucket) ApplyDigestResult(seqNo SeqNo, digest []byte) *Actions {
	s := b.Sequences[seqNo]
	actions := s.ApplyDigestResult(digest)
	if b.IAmLeader() {
		// We are the leader, no need to check ourselves for byzantine behavior
		// And no need to send the resulting prepare
		_ = s.ApplyValidateResult(true)
		return s.ApplyPrepare(b.Leader, digest)
	}
	return actions
}

func (b *Bucket) ApplyValidateResult(seqNo SeqNo, valid bool) *Actions {
	s := b.Sequences[seqNo]
	actions := s.ApplyValidateResult(valid)
	if !b.IAmLeader() {
		// We are not the leader, so let's apply a virtual prepare from
		// the leader that will not be sent, as there is no need to prepare
		actions.Append(s.ApplyPrepare(b.Leader, s.Digest))
	}
	return actions
}

func (b *Bucket) ApplyPrepare(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.Sequences[seqNo].ApplyPrepare(source, digest)
}

func (b *Bucket) ApplyCommit(source NodeID, seqNo SeqNo, digest []byte) *Actions {
	return b.Sequences[seqNo].ApplyCommit(source, digest)
}

type BucketStatus struct {
	ID             uint64
	Leader         bool
	NextAssigned   SeqNo
	BatchesPending int
	Sequences      []SequenceState
}

func (b *Bucket) Status() *BucketStatus {
	sequences := make([]SequenceState, int(b.EpochConfig.HighWatermark-b.EpochConfig.LowWatermark)+1)
	for i := range sequences {
		sequences[i] = b.Sequences[SeqNo(i)+b.EpochConfig.LowWatermark].State
	}
	return &BucketStatus{
		ID:             uint64(b.ID),
		Leader:         b.IAmLeader(),
		NextAssigned:   b.NextAssigned,
		BatchesPending: len(b.Pending),
		Sequences:      sequences,
	}
}
