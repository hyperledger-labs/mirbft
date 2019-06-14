/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type SequenceState int

const (
	Uninitialized SequenceState = iota
	Preprepared
	Digested
	InvalidBatch
	Validated
	Prepared
	Committed
)

type Sequence struct {
	EpochConfig *EpochConfig

	State SequenceState

	// Entry's Batch field is unset until after state >= Preprepared
	Entry *consumer.Entry

	// Digest is not set until after state >= Digested
	Digest []byte

	Prepares map[string]map[NodeID]struct{}
	Commits  map[string]map[NodeID]struct{}
}

func NewSequence(epochConfig *EpochConfig, number SeqNo, bucket BucketID) *Sequence {
	return &Sequence{
		EpochConfig: epochConfig,
		Entry: &consumer.Entry{
			Epoch:    epochConfig.Number,
			SeqNo:    uint64(number),
			BucketID: uint64(bucket),
		},
		State:    Uninitialized,
		Prepares: map[string]map[NodeID]struct{}{},
		Commits:  map[string]map[NodeID]struct{}{},
	}
}

// ApplyPreprepare attempts to apply a batch from a preprepare message to the state machine.
// If the state machine is not in the Uninitialized state, it returns an error.  Otherwise,
// It transitions to Preprepared and returns a ValidationRequest message.
func (s *Sequence) ApplyPreprepare(batch [][]byte) *consumer.Actions {
	if s.State != Uninitialized {
		s.EpochConfig.MyConfig.Logger.Panic("illegal state for preprepare", zap.Uint64(SeqNoLog, s.Entry.SeqNo), zap.Uint64(BucketIDLog, s.Entry.BucketID), zap.Uint64(EpochLog, s.EpochConfig.Number), zap.Int("CurrentState", int(s.State)), zap.Int("Expected", int(Uninitialized)))
	}

	s.State = Preprepared
	s.Entry.Batch = batch

	return &consumer.Actions{
		Digest: []*consumer.Entry{s.Entry},
	}
}

func (s *Sequence) ApplyDigestResult(digest []byte) *consumer.Actions {
	if s.State != Preprepared {
		s.EpochConfig.MyConfig.Logger.Panic("illegal state for digest result", zap.Uint64(SeqNoLog, s.Entry.SeqNo), zap.Uint64(BucketIDLog, s.Entry.BucketID), zap.Uint64(EpochLog, s.EpochConfig.Number), zap.Int("CurrentState", int(s.State)), zap.Int("Expected", int(Preprepared)))
	}

	s.State = Digested
	s.Digest = digest

	return &consumer.Actions{
		Validate: []*consumer.Entry{s.Entry},
	}
}

func (s *Sequence) ApplyValidateResult(valid bool) *consumer.Actions {
	if s.State != Digested {
		s.EpochConfig.MyConfig.Logger.Panic("illegal state for validate result", zap.Uint64(SeqNoLog, s.Entry.SeqNo), zap.Uint64(BucketIDLog, s.Entry.BucketID), zap.Uint64(EpochLog, s.EpochConfig.Number), zap.Int("CurrentState", int(s.State)), zap.Int("Expected", int(Digested)))
	}

	if !valid {
		s.State = InvalidBatch
		// TODO return a view change / suspect message
		return &consumer.Actions{}
	}

	s.State = Validated

	return &consumer.Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Prepare{
					Prepare: &pb.Prepare{
						SeqNo:  s.Entry.SeqNo,
						Epoch:  s.Entry.Epoch,
						Bucket: s.Entry.BucketID,
						Digest: s.Digest,
					},
				},
			},
		},
	}
}

func (s *Sequence) ApplyPrepare(source NodeID, digest []byte) *consumer.Actions {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.Prepares[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.Prepares[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	if s.State != Validated {
		return &consumer.Actions{}
	}

	// Do not prepare unless we have sent our prepare as well
	if _, ok := agreements[NodeID(s.EpochConfig.MyConfig.ID)]; !ok {
		return &consumer.Actions{}
	}

	// We do require 2*F+1 prepares, a prepare is implicitly added for the leader
	requiredPrepares := 2*s.EpochConfig.F + 1

	if len(agreements) < requiredPrepares {
		return &consumer.Actions{}
	}

	s.State = Prepared

	return &consumer.Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Commit{
					Commit: &pb.Commit{
						SeqNo:  s.Entry.SeqNo,
						Epoch:  s.Entry.Epoch,
						Bucket: s.Entry.BucketID,
						Digest: s.Digest,
					},
				},
			},
		},
	}
}

func (s *Sequence) ApplyCommit(source NodeID, digest []byte) *consumer.Actions {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.Commits[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.Commits[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	if s.State != Prepared {
		return &consumer.Actions{}
	}

	// Do not commit unless we have sent a commit
	if _, ok := agreements[NodeID(s.EpochConfig.MyConfig.ID)]; !ok {
		return &consumer.Actions{}
	}

	requiredCommits := 2*s.EpochConfig.F + 1

	if len(agreements) < requiredCommits {
		return &consumer.Actions{}
	}

	s.State = Committed

	return &consumer.Actions{
		Commit: []*consumer.Entry{s.Entry},
	}
}
