/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mir

import (
	"time"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
)

func (s *SBFT) sendViewChange() {
	log.Criticalf("replica %d: starting send view change", s.id)
	s.view = s.nextView()

	s.activeView = false
	s.haltAllInstances()

	s.viewChangeTimer.Cancel()

	log.Debugf("replica %d: halted instances", s.id)

	for _, b := range s.waitingToBeDelivered {
		b.maybeCancelTimer()
	}
	s.setRecovery()
	s.epochConfig[s.view] = &epochConfig{
		epoch:  s.view,
		valid:  false,
		echo:   make(map[uint64]*pb.Subject),
		accept: make(map[uint64]*pb.Subject),
	}

	for _, batch := range s.cur {
		batch.maybeCancelTimer()
	}
	for src := range s.replicaState {
		state := &s.replicaState[src]
		if state.viewchange != nil && state.viewchange.View < s.view {
			state.viewchange = nil
		}
	}
	log.Noticef("replica %d: sending viewchange for view %d", s.id, s.view)

	var q, p []*pb.Subject
	for _, batch := range s.cur {
		if batch.subject.Seq.Seq < s.lastCheckpoint.subject.Seq.Seq && s.lastCheckpoint.checkpointDone {
			log.Warningf("replica %d: batch %d should not be in current, last checkpoint is %d", s.id, batch.subject.Seq.Seq, s.lastCheckpoint.subject.Seq.Seq)
			continue
		}
		if batch.prepared {
			p = append(p, batch.subject)
		}
		if batch.preprep != nil {
			q = append(q, batch.subject)
		}
	}
	vc := &pb.ViewChange{
		View: s.view,
		Qset: q,
		Pset: p,
		Checkpoint: &pb.Batch{
			Header: s.lastCheckpoint.preprep.Batch.Header,
		},
	}
	svc := s.sign(vc)

	//s.sys.Persist(s.chainId, viewchange, svc)
	s.broadcast(&pb.Msg{Type: &pb.Msg_ViewChange{svc}})
}

func (s *SBFT) cancelViewChangeTimer() {
	log.Infof("replica %d: cancelling view-change timeout", s.id)
	s.viewChangeTimer.Cancel()
	s.viewChangeTimeout = time.Duration(config.Config.EpochTimeoutNsec) * 5
}

func (s *SBFT) handleViewChange(svc *pb.Signed, src uint64) {
	vc := &pb.ViewChange{}
	err := s.checkSig(svc, src, vc)
	if err != nil {
		log.Noticef("replica %d: invalid viewchange: %s", s.id, err)
		return
	}
	//TODO do we need to checkBatch???
	if vc.View < s.view {
		log.Debugf("replica %d: old view change from %d for view %d, we are in view %d", s.id, src, vc.View, s.view)
		return
	}
	if vc.View == s.view && s.activeView {
		log.Debugf("replica %d: old view change from %d for view %d, we are already active in view %d", s.id, src, vc.View, s.view)
		return
	}
	if ovc := s.replicaState[src].viewchange; ovc != nil && vc.View <= ovc.View {
		log.Noticef("replica %d: duplicate view change for %d from %d", s.id, vc.View, src)
		return
	}

	//s.haltAllInstances()
	log.Errorf("replica %d: viewchange from %d: %v", s.id, src, vc.View)
	s.replicaState[src].viewchange = vc
	s.replicaState[src].signedViewchange = svc

	min := vc.View

	quorum := 0
	for _, state := range s.replicaState {
		if state.viewchange != nil {
			quorum++
			if state.viewchange.View < min {
				min = state.viewchange.View
			}
		}
	}

	if quorum == s.oneCorrectQuorum() {
		// catch up to the minimum view
		if s.view < min {
			log.Errorf("replica %d: we are behind on view change, resending for newer view", s.id)
			s.view = min - 1
			s.sendViewChange()
			return
		}
	}

	if quorum == s.viewChangeQuorum() {
		s.viewChangeTimer = s.managerDispatcher.TimerForRequestHandler(s.viewChangeTimeout, func() {
			s.viewChangeTimeout *= 2
			log.Errorf("replica %d: view change timed out, sending next", s.id)
			s.sendViewChange()
		})
	}

	if s.primaryIDView(vc.View) == s.id {
		s.maybeSendNewEpoch()
	}
}
