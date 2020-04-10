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

	persisted *persisted

	// qEntry is unset until after state >= Preprepared
	qEntry *pb.QEntry

	// batch is not set until after state >= Allocated
	batch []*clientRequest

	// digest is not set until after state >= Digested
	digest []byte

	prepares map[string]map[NodeID]struct{}
	commits  map[string]map[NodeID]struct{}
}

func newSequence(owner NodeID, epoch, seqNo uint64, persisted *persisted, networkConfig *pb.NetworkConfig, myConfig *Config) *sequence {
	return &sequence{
		owner:         owner,
		seqNo:         seqNo,
		epoch:         epoch,
		myConfig:      myConfig,
		networkConfig: networkConfig,
		persisted:     persisted,
		state:         Uninitialized,
		prepares:      map[string]map[NodeID]struct{}{},
		commits:       map[string]map[NodeID]struct{}{},
	}
}

func (s *sequence) allocateInvalid(batch []*clientRequest) *Actions {
	// TODO, handle this case by optionally allowing the transition from
	// Invalid to Prepared if the network agrees that this batch is valid.
	panic("TODO unhandled")
}

// allocate reserves this sequence in this epoch for a set of requests.
// If the state machine is not in the Uninitialized state, it returns an error.  Otherwise,
// It transitions to Preprepared and returns a ValidationRequest message.
func (s *sequence) allocate(batch []*clientRequest) *Actions {
	if s.state != Uninitialized {
		s.myConfig.Logger.Panic("illegal state for allocate", zap.Int("State", int(s.state)), zap.Uint64("SeqNo", s.seqNo), zap.Uint64("Epoch", s.epoch))
	}

	s.state = Allocated
	s.batch = batch

	requestAcks := make([]*pb.RequestAck, len(batch))
	data := make([][]byte, len(batch))
	for i, request := range batch {
		requestAcks[i] = &pb.RequestAck{
			ClientId: request.data.ClientId,
			ReqNo:    request.data.ReqNo,
			Digest:   request.digest,
		}
		data[i] = request.digest
	}

	if len(requestAcks) == 0 {
		// This is a no-op batch, no need to compute a digest
		return s.applyProcessResult(nil)
	}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: data,

				Batch: &Batch{
					Source:      uint64(s.owner),
					SeqNo:       s.seqNo,
					Epoch:       s.epoch,
					RequestAcks: requestAcks,
				},
			},
		},
	}
}

func (s *sequence) applyProcessResult(digest []byte) *Actions {
	if s.state != Allocated {
		s.myConfig.Logger.Panic("illegal state for digest result", zap.Any("State", s.state), zap.Uint64("SeqNo", s.seqNo), zap.Uint64("Epoch", s.epoch))
	}

	s.digest = digest

	requests := make([]*pb.ForwardRequest, len(s.batch))
	requestAcks := make([]*pb.RequestAck, len(s.batch))
	for i, req := range s.batch {
		requests[i] = &pb.ForwardRequest{
			Request: req.data,
			Digest:  req.digest,
		}
		requestAcks[i] = &pb.RequestAck{
			ClientId: req.data.ClientId,
			ReqNo:    req.data.ReqNo,
			Digest:   req.digest,
		}
	}

	s.qEntry = &pb.QEntry{
		SeqNo:    s.seqNo,
		Epoch:    s.epoch,
		Digest:   digest,
		Requests: requests,
	}

	s.state = Preprepared

	var msgs []*pb.Msg
	if uint64(s.owner) == s.myConfig.ID {
		msgs = make([]*pb.Msg, len(s.batch)+1)
		for i, request := range requests {
			msgs[i] = &pb.Msg{
				Type: &pb.Msg_ForwardRequest{
					ForwardRequest: request, // TODO, do we want to share this with QEntry? It causes concurrent marshalling concerns.
				},
			}
		}
		msgs[len(s.batch)] = &pb.Msg{
			Type: &pb.Msg_Preprepare{
				Preprepare: &pb.Preprepare{
					SeqNo: s.seqNo,
					Epoch: s.epoch,
					Batch: requestAcks,
				},
			},
		}
	} else {
		msgs = []*pb.Msg{
			{
				Type: &pb.Msg_Prepare{
					Prepare: &pb.Prepare{
						SeqNo:  s.seqNo,
						Epoch:  s.epoch,
						Digest: s.digest,
					},
				},
			},
		}
	}

	s.persisted.addQEntry(s.qEntry)

	actions := &Actions{
		Broadcast: msgs,
		QEntries:  []*pb.QEntry{s.qEntry},
	}

	if s.owner != NodeID(s.myConfig.ID) {
		actions.Append(s.applyPrepareMsg(s.owner, digest))
	}

	return actions
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

	pEntry := &pb.PEntry{
		Epoch:  s.epoch,
		SeqNo:  s.seqNo,
		Digest: s.digest,
	}

	s.persisted.addPEntry(pEntry)

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
			pEntry,
		},
	}
}

func (s *sequence) applyCommitMsg(source NodeID, digest []byte) {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.commits[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.commits[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	if s.state != Prepared {
		return
	}

	// Do not commit unless we have sent a commit
	if _, ok := agreements[NodeID(s.myConfig.ID)]; !ok {
		return
	}

	requiredCommits := intersectionQuorum(s.networkConfig)

	if len(agreements) < requiredCommits {
		return
	}

	s.state = Committed
}
