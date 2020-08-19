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
	Allocated
	PendingRequests
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
	networkConfig *pb.NetworkState_Config

	state SequenceState

	persisted *persisted

	// qEntry is unset until after state >= Preprepared
	qEntry *pb.QEntry

	// batch is not set until after state >= Allocated
	batch []*pb.RequestAck

	// outstandingReqs is not set until after state >= Allocated and may never be set
	// it is a map from request digest to index in the forwardReqs slice
	outstandingReqs map[string]int

	// forwardReqs is corresponding Requests reference in the batch, not available until state >= Ready
	forwardReqs []*pb.ForwardRequest

	// digest is the computed digest of the batch, may not be set until state > Ready
	digest []byte

	prepares map[string]map[NodeID]struct{}
	commits  map[string]map[NodeID]struct{}
}

func newSequence(owner NodeID, epoch, seqNo uint64, persisted *persisted, networkConfig *pb.NetworkState_Config, myConfig *Config) *sequence {
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

func (s *sequence) advanceState() *Actions {
	actions := &Actions{}
	for {
		oldState := s.state
		switch s.state {
		case Uninitialized:
		case Allocated:
		case PendingRequests:
			s.checkRequests()
		case Ready:
			if s.digest != nil || len(s.batch) == 0 {
				actions.Append(s.prepare())
			}
		case Preprepared:
			actions.Append(s.checkPrepareQuorum())
		case Prepared:
			s.checkCommitQuorum()
		case Committed:
		}
		if s.state == oldState {
			return actions
		}
	}
}

func (s *sequence) allocateAsOwner(clientRequests []*clientRequest) *Actions {
	requestAcks := make([]*pb.RequestAck, len(clientRequests))
	forwardReqs := make([]*pb.ForwardRequest, len(clientRequests))
	for i, clientRequest := range clientRequests {
		requestAcks[i] = &pb.RequestAck{
			ClientId: clientRequest.data.ClientId,
			ReqNo:    clientRequest.data.ReqNo,
			Digest:   clientRequest.digest,
		}

		forwardReqs[i] = &pb.ForwardRequest{
			Request: clientRequest.data,
			Digest:  clientRequest.digest,
		}
	}

	// TODO, hold onto the clientRequests so that we know who to forward to

	return s.allocate(requestAcks, forwardReqs, nil)
}

// allocate reserves this sequence in this epoch for a set of requests.
// If the state machine is not in the Uninitialized state, it returns an error.  Otherwise,
// It transitions to Preprepared and returns a ValidationRequest message.
func (s *sequence) allocate(requestAcks []*pb.RequestAck, forwardReqs []*pb.ForwardRequest, outstandingReqs map[string]int) *Actions {
	if s.state != Uninitialized {
		s.myConfig.Logger.Panic("illegal state for allocate", zap.Int("State", int(s.state)), zap.Uint64("SeqNo", s.seqNo), zap.Uint64("Epoch", s.epoch))
	}

	s.state = Allocated
	s.batch = requestAcks
	s.forwardReqs = forwardReqs
	s.outstandingReqs = outstandingReqs

	if len(requestAcks) == 0 {
		// This is a no-op batch, no need to compute a digest
		s.state = Ready
		return s.applyProcessResult(nil)
	}

	data := make([][]byte, len(requestAcks))
	for i, ack := range requestAcks {
		data[i] = ack.Digest
	}

	actions := &Actions{
		Hash: []*HashRequest{
			{
				Data: data,

				Origin: &pb.HashResult{
					Type: &pb.HashResult_Batch_{
						Batch: &pb.HashResult_Batch{
							Source:      uint64(s.owner),
							SeqNo:       s.seqNo,
							Epoch:       s.epoch,
							RequestAcks: requestAcks,
						},
					},
				},
			},
		},
	}

	s.state = PendingRequests

	actions.Append(s.advanceState())

	return actions
}

func (s *sequence) satisfyOutstanding(fr *pb.ForwardRequest) *Actions {
	i, ok := s.outstandingReqs[string(fr.Digest)]
	if !ok {
		panic("dev sanity check")
	}

	s.forwardReqs[i] = fr

	return s.advanceState()
}

func (s *sequence) checkRequests() {
	if len(s.outstandingReqs) > 0 {
		return
	}

	s.state = Ready
}

func (s *sequence) applyProcessResult(digest []byte) *Actions {

	s.digest = digest

	return s.applyPrepareMsg(s.owner, digest)
}

func (s *sequence) prepare() *Actions {
	s.qEntry = &pb.QEntry{
		SeqNo:    s.seqNo,
		Digest:   s.digest,
		Requests: s.forwardReqs,
	}

	s.state = Preprepared

	var actions *Actions

	if uint64(s.owner) == s.myConfig.ID {
		bcast := make([]*pb.Msg, len(s.forwardReqs)+1)
		for i, fr := range s.forwardReqs {
			bcast[i] = &pb.Msg{ // TODO, unicast only to those who need it
				Type: &pb.Msg_ForwardRequest{
					ForwardRequest: fr,
				},
			}
		}
		bcast[len(s.forwardReqs)] = &pb.Msg{
			Type: &pb.Msg_Preprepare{
				Preprepare: &pb.Preprepare{
					SeqNo: s.seqNo,
					Epoch: s.epoch,
					Batch: s.batch,
				},
			},
		}
		actions = &Actions{
			Broadcast: bcast,
		}
	} else {
		actions = &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_Prepare{
						Prepare: &pb.Prepare{
							SeqNo:  s.seqNo,
							Epoch:  s.epoch,
							Digest: s.digest,
						},
					},
				},
			},
		}
	}

	actions.Append(s.persisted.addQEntry(s.qEntry))

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
	s.prepares[string(digest)] = agreements

	return s.advanceState()
}

func (s *sequence) checkPrepareQuorum() *Actions {
	agreements := s.prepares[string(s.digest)]
	// Do not prepare unless we have sent our prepare as well
	// as this ensures we've persisted our qSet
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
		SeqNo:  s.seqNo,
		Digest: s.digest,
	}

	actions := &Actions{
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
	}
	actions.Append(s.persisted.addPEntry(pEntry))
	return actions
}

func (s *sequence) applyCommitMsg(source NodeID, digest []byte) *Actions {
	// TODO, if the digest is known, mark a mismatch as oddity
	agreements := s.commits[string(digest)]
	if agreements == nil {
		agreements = map[NodeID]struct{}{}
		s.commits[string(digest)] = agreements
	}
	agreements[source] = struct{}{}

	return s.advanceState()
}

func (s *sequence) checkCommitQuorum() {
	agreements := s.commits[string(s.digest)]
	// Do not commit unless we have sent a commit
	// and therefore already have persisted our pSet and qSet
	if _, ok := agreements[NodeID(s.myConfig.ID)]; !ok {
		return
	}

	requiredCommits := intersectionQuorum(s.networkConfig)

	if len(agreements) < requiredCommits {
		return
	}

	s.state = Committed
}
