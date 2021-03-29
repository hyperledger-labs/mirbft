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
	"github.com/golang/protobuf/proto"
)

func (s *SBFT) sendEpochCfgEcho(nv *pb.NewView, src uint64) {
	log.Errorf("replica %d: sendEpochCfgEcho, src is %d", s.id, src)
	if nv.Config == nil {
		log.Criticalf("replica %d: did not receive a config with the new view", s.id)
	}
	if _, ok := s.epochConfig[nv.Config.Epoch]; !ok {
		s.epochConfig[nv.Config.Epoch] = &epochConfig{}
		log.Debugf("replica %d: inserting new epoch config", s.id)

	}
	applyNV := false
	config := s.epochConfig[nv.Config.Epoch]
	if config.accepted && config.newview == nil {
		applyNV = true
	}
	config.src = src
	config.epoch = nv.Config.Epoch
	config.primary = nv.Config.Primary
	config.leaders = nv.Config.Leaderset
	config.blacklist = nv.Config.Blacklist
	config.length = nv.Config.Length
	config.first = nv.Config.First
	config.last = nv.Config.First + nv.Config.Length
	config.nextActiveBucket = nv.Config.RotationOffset
	config.newview = nv
	if config.echo == nil {
		config.echo = make(map[uint64]*pb.Subject)
	}
	if config.accept == nil {
		config.accept = make(map[uint64]*pb.Subject)
	}

	if s.activeView {
		err := s.validateEpochConfig(config, s.view)

		if err != nil {
			log.Errorf("replica %d: invalid epoch configuration while active, sending viewchange: %s", s.id, err.Error())
			s.sendViewChange()
			return
		}
	} else {
		err := s.validateEpochConfig(config, s.view-1)

		if err != nil {
			log.Errorf("replica %d: invalid epoch configuration while not active, sending viewchange: %s", s.id, err.Error())
			s.sendViewChange()
			return
		}
	}
	config.valid = true

	subject := &pb.Subject{Seq: &pb.SeqView{View: nv.Config.Epoch}}
	configRaw, err := proto.Marshal(nv.Config)
	subject.Digest = hash(configRaw)
	if err != nil {
		log.Warningf("replica %d: could not marshal epoch configuration", s.id)
	}
	config.subject = subject
	s.epochConfig[nv.Config.Epoch] = config
	log.Errorf("replica %d: inserting new epoch config for epoch %d: %v", s.id, config.epoch, config)
	if !s.isPrimary() {
		log.Errorf("replica %d: sending echo for new epoch %d configuration", s.id, nv.Config.Epoch)
		s.broadcast(&pb.Msg{Type: &pb.Msg_EpochCfgEcho{subject}})
	}
	if applyNV {
		s.applyNewView(config.newview, config.src)
	}
}

func (s *SBFT) handleEpochCfgEcho(nve *pb.Subject, src uint64) {
	log.Errorf("received epoch config echo for %d from %d", nve.Seq.View, src)
	if config, ok := s.epochConfig[nve.Seq.View]; ok {
		if _, ok := config.echo[src]; ok {
			log.Infof("replica %d: duplicate epoch config echo for %v from %d", s.id, nve.Seq, src)
			return
		}
		if config.subject == nil {
			config.subject = nve
		}
		config.echo[src] = nve
	} else {
		config = &epochConfig{
			subject:  nve,
			epoch:    nve.Seq.View,
			echo:     make(map[uint64]*pb.Subject),
			accept:   make(map[uint64]*pb.Subject),
			echoed:   false,
			accepted: false,
		}
		s.epochConfig[nve.Seq.View] = config
		log.Debugf("replica %d: inserting new epoch config for epoch %d: %v", s.id, config.epoch, config)
		config.echo[src] = nve
	}
	s.maybeSendEpochCfgAccept(nve.Seq.View)
}

func (s *SBFT) sendEpochCfgEchoNoNV(newconfig *pb.EpochConfig, src uint64) {
	log.Debugf("replica %d: sendEpochCfgEchoNoNV, src is %d", s.id, src)
	if newconfig == nil {
		log.Criticalf("replica %d: did not receive a config", s.id)
	}
	if _, ok := s.epochConfig[newconfig.Epoch]; !ok {
		s.epochConfig[newconfig.Epoch] = &epochConfig{}

		log.Debugf("replica %d: inserting new epoch config", s.id)

	}
	config := s.epochConfig[newconfig.Epoch]
	config.src = src
	config.epoch = newconfig.Epoch
	config.primary = newconfig.Primary
	config.leaders = newconfig.Leaderset
	config.blacklist = newconfig.Blacklist
	config.length = newconfig.Length
	config.first = newconfig.First
	config.last = newconfig.First + newconfig.Length
	config.nextActiveBucket = newconfig.RotationOffset
	config.newview = nil
	if config.echo == nil {
		config.echo = make(map[uint64]*pb.Subject)
	}
	if config.accept == nil {
		config.accept = make(map[uint64]*pb.Subject)
	}

	err := s.validateEpochConfig(config, config.epoch-1)
	if err != nil {
		log.Errorf("replica %d: invalid epoch configuration, sending viewchange: %s", s.id, err.Error())
		s.sendViewChange()
		return
	}
	//if s.activeView {
	//	err := s.validateEpochConfig(config, s.view)
	//	if err != nil {
	//		log.Errorf("replica %d: invalid epoch configuration while active, sending viewchange: %s", s.id, err.Error())
	//		s.sendViewChange()
	//		return
	//	}
	//} else {
	//	err := s.validateEpochConfig(config, s.view-1)
	//
	//	if err != nil {
	//		log.Errorf("replica %d: invalid epoch configuration while not active, sending viewchange: %s", s.id, err.Error())
	//		s.sendViewChange()
	//		return
	//	}
	//}

	config.valid = true

	subject := &pb.Subject{Seq: &pb.SeqView{View: newconfig.Epoch}}
	configRaw, err := proto.Marshal(newconfig)
	subject.Digest = hash(configRaw)
	if err != nil {
		log.Warningf("replica %d: could not marshal epoch configuration", s.id)
	}
	config.subject = subject
	s.epochConfig[newconfig.Epoch] = config
	log.Debugf("replica %d: inserting new epoch config for epoch %d: %v", s.id, config.epoch, config)
	if s.primaryIDView(newconfig.Epoch) != s.id {
		log.Debugf("replica %d: sending echo for new epoch %d configuration", s.id, newconfig.Epoch)
		s.broadcast(&pb.Msg{Type: &pb.Msg_EpochCfgEcho{subject}})
	}
}
