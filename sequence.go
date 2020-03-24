/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
	"go.uber.org/zap"
)

type SequenceState int

const (
	Uninitialized SequenceState = iota
	Invalid
	Allocated
	Ready
	Preprepared
	Prepared
	Committed
)

type sequence struct {
	owner NodeID
	seqNo uint64
	epoch uint64

	myConfig      *Config
	networkConfig *pb.NetworkConfig

	state SequenceState

	// qEntry is unset until after state >= Preprepared
	qEntry *pb.QEntry

	// batch is not set until after state >= Allocated
	batch []*request

	// digest is not set until after state >= Digested
	digest []byte

	prepares map[string]map[NodeID]struct{}
	commits  map[string]map[NodeID]struct{}
}

func newSequence(owner NodeID, epoch, seqNo uint64, networkConfig *pb.NetworkConfig, myConfig *Config) *sequence {
	return &sequence{
		owner:         owner,
		seqNo:         seqNo,
		epoch:         epoch,
		myConfig:      myConfig,
		networkConfig: networkConfig,
		state:         Uninitialized,
		prepares:      map[string]map[NodeID]struct{}{},
		commits:       map[string]map[NodeID]struct{}{},
	}
}

func (s *sequence) allocateInvalid(batch []*request) *Actions {
	// TODO, handle this case by optionally allowing the transition from
	// Invalid to Prepared if the network agrees that this batch is valid.
	panic("TODO unhandled")
}

// allocate reserves this sequence in this epoch for a set of requests.
// If the state machine is not in the Uninitialized state, it returns an error.  Otherwise,
// It transitions to Preprepared and returns a ValidationRequest message.
func (s *sequence) allocate(batch []*request) *Actions {
	if s.state != Uninitialized {
		s.myConfig.Logger.Panic("illegal state for allocate", zap.Int("State", int(s.state)), zap.Uint64("SeqNo", s.seqNo), zap.Uint64("Epoch", s.epoch))
	}

	s.state = Allocated
	s.batch = batch

	requests := make([]*PreprocessResult, len(batch))
	for i, request := range batch {
		requests[i] = &PreprocessResult{
			RequestData: request.requestData,
			Digest:      request.digest,
		}
		request.state = Allocated
		request.seqNo = s.seqNo
	}

	return &Actions{
		Process: []*Batch{
			{
				Source:   uint64(s.owner),
				SeqNo:    s.seqNo,
				Epoch:    s.epoch,
				Requests: requests,
			},
		},
	}
}

func (s *sequence) applyProcessResult(digest []byte) *Actions {
	if s.state != Allocated {
		s.myConfig.Logger.Panic("illegal state for digest result")
	}

	s.digest = digest

	requests := make([]*pb.Request, len(s.batch))
	for i, req := range s.batch {
		requests[i] = &pb.Request{
			ClientId: req.requestData.ClientId,
			ReqNo:    req.requestData.ReqNo,
			Digest:   req.digest,
		}
	}

	s.qEntry = &pb.QEntry{
		SeqNo:    s.seqNo,
		Epoch:    s.epoch,
		Digest:   digest,
		Requests: requests,
	}

	for _, request := range s.batch {
		request.state = Preprepared
	}

	s.state = Preprepared

	var msg *pb.Msg
	if uint64(s.owner) == s.myConfig.ID {
		msg = &pb.Msg{
			Type: &pb.Msg_Preprepare{
				Preprepare: &pb.Preprepare{
					SeqNo: s.seqNo,
					Epoch: s.epoch,
					Batch: requests, // TODO, do we want to share this with the qEntry? Concurrent marshaling is not threadsafe I think
				},
			},
		}
	} else {
		msg = &pb.Msg{
			Type: &pb.Msg_Prepare{
				Prepare: &pb.Prepare{
					SeqNo:  s.seqNo,
					Epoch:  s.epoch,
					Digest: s.digest,
				},
			},
		}
	}

	return &Actions{
		Broadcast: []*pb.Msg{msg},
		QEntries:  []*pb.QEntry{s.qEntry},
	}
}

func (s *sequence) applyPrepareMsg(source NodeID, digest []byte) *Actions {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.prepares[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.prepares[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	if s.state != Preprepared {
		return &Actions{}
	}

	// Do not prepare unless we have sent our prepare as well
	if _, ok := agreements[NodeID(s.myConfig.ID)]; !ok {
		return &Actions{}
	}

	// We do require 2f+1 prepares (instead of 2f), as the preprepare
	// for the leader will be applied as a prepare here
	requiredPrepares := intersectionQuorum(s.networkConfig)

	if len(agreements) < requiredPrepares {
		return &Actions{}
	}

	s.state = Prepared
	for _, request := range s.batch {
		request.state = Prepared
	}

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Commit{
					Commit: &pb.Commit{
						SeqNo:  s.seqNo,
						Epoch:  s.epoch,
						Digest: s.digest,
					},
				},
			},
		},
		PEntries: []*pb.PEntry{
			{
				Epoch:  s.epoch,
				SeqNo:  s.seqNo,
				Digest: s.digest,
			},
		},
	}
}

func (s *sequence) applyCommitMsg(source NodeID, digest []byte) *Actions {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.commits[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.commits[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	if s.state != Prepared {
		return &Actions{}
	}

	// Do not commit unless we have sent a commit
	if _, ok := agreements[NodeID(s.myConfig.ID)]; !ok {
		return &Actions{}
	}

	requiredCommits := intersectionQuorum(s.networkConfig)

	if len(agreements) < requiredCommits {
		return &Actions{}
	}

	s.state = Committed
	for _, request := range s.batch {
		request.state = Committed
	}

	return &Actions{
		Commits: []*Commit{
			{
				QEntry:     s.qEntry,
				Checkpoint: s.seqNo%uint64(s.networkConfig.CheckpointInterval) == 0,
			},
		},
	}
}
