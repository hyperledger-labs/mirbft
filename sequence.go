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
	networkConfig *pb.NetworkConfig
	clientWindows *clientWindows

	state SequenceState

	persisted *persisted

	// qEntry is unset until after state >= Preprepared
	qEntry *pb.QEntry

	// batch is not set until after state >= Allocated
	batch []*pb.RequestAck

	// requestData is corresponding Requests reference in the batch, not available until state >= Ready
	requestData []*pb.Request

	// digest is the computed digest of the batch, may not be set until state > Ready
	digest []byte

	prepares map[string]map[NodeID]struct{}
	commits  map[string]map[NodeID]struct{}
}

func newSequence(owner NodeID, epoch, seqNo uint64, clientWindows *clientWindows, persisted *persisted, networkConfig *pb.NetworkConfig, myConfig *Config) *sequence {
	return &sequence{
		owner:         owner,
		seqNo:         seqNo,
		epoch:         epoch,
		myConfig:      myConfig,
		networkConfig: networkConfig,
		clientWindows: clientWindows,
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

// allocate reserves this sequence in this epoch for a set of requests.
// If the state machine is not in the Uninitialized state, it returns an error.  Otherwise,
// It transitions to Preprepared and returns a ValidationRequest message.
func (s *sequence) allocate(requestAcks []*pb.RequestAck) *Actions {
	if s.state != Uninitialized {
		s.myConfig.Logger.Panic("illegal state for allocate", zap.Int("State", int(s.state)), zap.Uint64("SeqNo", s.seqNo), zap.Uint64("Epoch", s.epoch))
	}

	s.state = Allocated
	s.batch = requestAcks

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

				Batch: &Batch{
					Source:      uint64(s.owner),
					SeqNo:       s.seqNo,
					Epoch:       s.epoch,
					RequestAcks: requestAcks,
				},
			},
		},
	}

	s.state = PendingRequests

	actions.Append(s.advanceState())

	return actions
}

func (s *sequence) checkRequests() {
	requestData := make([]*pb.Request, len(s.batch))
	someMissing := false
	for i, requestAck := range s.batch {
		cw, ok := s.clientWindows.clientWindow(requestAck.ClientId)
		if !ok {
			someMissing = true
			continue
		}

		cr := cw.request(requestAck.ReqNo)
		if cr == nil {
			panic("this shouldn't happen, we should have already had a request forwarded")
		}

		clientRequest, ok := cr.digests[string(requestAck.Digest)]
		if !ok {
			panic("we should already have at least one ack by this point")
		}

		if len(clientRequest.agreements) < someCorrectQuorum(s.networkConfig) {
			someMissing = true
			continue
		}

		if clientRequest.data == nil {
			someMissing = true
			continue
		}

		requestData[i] = clientRequest.data
	}

	if someMissing {
		return
	}

	s.state = Ready
	s.requestData = requestData
}

func (s *sequence) applyProcessResult(digest []byte) *Actions {

	s.digest = digest

	return s.advanceState()
}

func (s *sequence) prepare() *Actions {
	forwardRequests := make([]*pb.ForwardRequest, len(s.batch))
	for i, req := range s.batch {
		forwardRequests[i] = &pb.ForwardRequest{
			Request: s.requestData[i],
			Digest:  req.Digest,
		}
	}

	s.qEntry = &pb.QEntry{
		SeqNo:    s.seqNo,
		Epoch:    s.epoch,
		Digest:   s.digest,
		Requests: forwardRequests,
	}

	s.state = Preprepared

	var msgs []*pb.Msg
	if uint64(s.owner) == s.myConfig.ID {
		msgs = make([]*pb.Msg, len(s.batch)+1)
		for i, forwardRequest := range forwardRequests {
			msgs[i] = &pb.Msg{
				Type: &pb.Msg_ForwardRequest{
					ForwardRequest: forwardRequest,
				},
			}
		}
		msgs[len(s.batch)] = &pb.Msg{
			Type: &pb.Msg_Preprepare{
				Preprepare: &pb.Preprepare{
					SeqNo: s.seqNo,
					Epoch: s.epoch,
					Batch: s.batch,
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

	actions := &Actions{Broadcast: msgs}
	actions.Append(s.persisted.addQEntry(s.qEntry))

	if s.owner != NodeID(s.myConfig.ID) {
		s.applyPrepareMsg(s.owner, s.digest)
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
		Epoch:  s.epoch,
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
