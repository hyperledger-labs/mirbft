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
	pb "github.com/IBM/mirbft/protos"
)

func (s *SBFT) maybeSendEpochCfgAccept(epoch uint64) {
	config := s.epochConfig[epoch]
	if config.echoed {
		return
	}
	if config.newview == nil || len(s.epochConfig[epoch].echo) < s.commonCaseQuorum()-1 {
		return
	}
	s.sendEpochCfgAccept(epoch)
}

func (s *SBFT) sendEpochCfgAccept(epoch uint64) {
	s.epochConfig[epoch].echoed = true
	subject := s.epochConfig[epoch].subject
	log.Debugf("replica %d: %+v", s.id, s.epochConfig[epoch].subject)
	log.Infof("replica %d: ECHOED %d", s.id, s.epochConfig[epoch].subject.Seq.View)
	s.broadcast(&pb.Msg{Type: &pb.Msg_EpochCfgAccept{subject}})
}

func (s *SBFT) handleEpochCfgAccept(nva *pb.Subject, src uint64) {
	log.Infof("received epoch config accept for % from %d", nva.Seq.View, src)
	var config *epochConfig
	if c, ok := s.epochConfig[nva.Seq.View]; ok {
		config = c
		c.subject = nva
		if _, ok := config.accept[src]; ok {
			log.Infof("replica %d: duplicate epoch config accept for %+v from %d", s.id, nva.Seq, src)
			return
		}
	} else {
		config = &epochConfig{
			subject:  nva,
			epoch:    nva.Seq.View,
			echo:     make(map[uint64]*pb.Subject),
			accept:   make(map[uint64]*pb.Subject),
			echoed:   false,
			accepted: false,
		}
		s.epochConfig[nva.Seq.View] = config
		log.Debugf("replica %d: inserting new epoch config for epoch %d: %v", s.id, config.epoch, config)
	}
	config.accept[src] = nva

	//maybe mark as committed
	if config.accepted {
		return
	}

	if len(config.accept) < s.commonCaseQuorum() {
		return
	}
	config.accepted = true

	log.Infof("replica %d: CONFIG %d", s.id, config.subject.Seq.View)
	log.Noticef("replica %d: accepting new epoch configuration %d", s.id, nva.Seq.View)

	if config.newview == nil {
		return
	}

	s.applyNewView(config.newview, config.src)
}
