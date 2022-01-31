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
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
	"github.com/golang/protobuf/proto"
)

/*
Epoch configuration is sent as special configuration request that has payload the
proposed epochleaderset and the size of the epoch.
It is sent in a preprepare message by the new epoch leader after the new epoch has been established.
Optimization: piggyback this special request on the last preprepare before the epoch change.
In an untimely epoch change the optimization is of course not possible.
*/

func (s *SBFT) maybeNewEpochConfig(epoch uint64) (bool, error) {
	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	log.Infof("maybe new epoch: current view %d, potential view %d, current config last %d, last delivered %d", s.view, epoch, s.epochConfig[s.view].last, lastDelivered.subject.Seq.Seq)
	if s.view < epoch && s.epochConfig[s.view].last <= lastDelivered.subject.Seq.Seq {
		if s.activeView == false {
			log.Errorf("want to change epoch but view not active")
			return false, nil
		}
		if config, ok := s.epochConfig[epoch]; ok {
			log.Errorf("replica %d: applying new epoch %d after delivery", s.id, epoch)
			s.applyNewEpochConfig(config)
			return true, nil
		}
		log.Errorf("replica %d - I do not have the epoch confing for epoch %d", s.id, epoch)

		if s.isInRecovery() {
			log.Errorf("Want to change to epoch %d but don't have epoch config primary responsible %d", epoch, s.primaryIDView(epoch))
			//s.blacklistedLeaders[s.primaryIDView(epoch)] = 1
			return false, errors.New("Want to change epoch but don't have epoch config")
		}
	} else {
		log.Errorf("replica %d - not applying new epoch", s.id)
	}
	return false, nil
}

func (s *SBFT) applyNewEpochConfig(epochConfig *epochConfig) {
	log.Criticalf("replica %d: applying new epoch config %d with %d leaders %v", s.id, epochConfig.epoch, len(epochConfig.leaders), epochConfig.leaders)

	view := s.view
	if s.activeView {
		log.Debugf("replica %d: active view, setting view to %d", s.id, epochConfig.epoch)
		view = epochConfig.epoch
	}
	if !epochConfig.valid {
		err := s.validateEpochConfig(epochConfig, view-1)
		if err != nil {
			log.Errorf("replica %d invalid epoch config: %s", s.id, err.Error())
			s.sendViewChange()
		}
	}

	epochConfig.valid = true
	if _, ok := s.epochConfig[epochConfig.epoch]; !ok {
		log.Debugf("replica %d: inserting new epoch config for epoch %d: %v", s.id, epochConfig.epoch, epochConfig)
		s.epochConfig[epochConfig.epoch] = epochConfig
	}

	s.leaders = make(map[uint64]uint64)

	iAmLeader := false
	offset := uint64(0)

	for i, leader := range epochConfig.leaders {

		s.leaders[uint64(i)] = leader
		//s.bucketLocks[leader].Lock()
		//s.pending[s.leaders[uint64(i)]] = make(map[string]*Request)
		//s.bucketLocks[leader].Unlock()
		//s.nextSeqNo[leader]= config.first+uint64(i)
		if leader == s.id {
			iAmLeader = true
			offset = uint64(i)
		}
	}
	//s.blacklistedLeaders = make(map[uint64]uint64)
	//for _, suspect := range config.blacklist{
	//	s.blacklistedLeaders[suspect]=1
	//}
	log.Criticalf("blacklist: %v", s.blacklistedLeaders)

	if iAmLeader {
		s.nextProposalToSchedule = epochConfig.first + offset
	} else {
		s.nextProposalToSchedule = epochConfig.last + 1
	}

	//if s.epochConfig[s.view].length == s.config.N {
	//	s.inRecovery = false
	//} else {
	//	s.inRecovery = true
	//}
	s.nextActiveBucket[epochConfig.src] = epochConfig.nextActiveBucket

	s.assignBuckets()
	//s.leaderPendingSize = 0
	//for _, bucket := range s.activeBuckets[s.id] {
	//	s.leaderPendingSize += uint64(len(s.pending[bucket]))
	//}

	// Attention: exit recovery AFTER assigning buckets for the first time
	// At this point we do not have update rotation offsets for other nodes except for the epoch primary
	if uint64(len(s.epochConfig[view].leaders)) == uint64(config.Config.MaxLeaders) {
		//|| uint64(len(s.epochConfig[view].leaders)) >= s.config.N - uint64(len(s.blacklistedLeaders)) {
		//we need to set last rotation such that the next rotation should not happen "in the past"
		lastDelivered := s.lastDelivered.Load().(*batchInfo)
		for s.lastRotation+uint64(config.Config.BucketRotationPeriod) <= lastDelivered.subject.Seq.Seq {
			s.lastRotation += uint64(config.Config.BucketRotationPeriod)
		}
		s.unsetRecovery()
		log.Errorf("replica %d: back to stable mode", s.id)
	} else {
		s.setRecovery()
	}

	s.assignPayloadBuckets(s.isInRecovery())

	//s.sys.Persist(s.chainId, epochconfig, config.newview.Config)

	if s.activeView {
		fmt.Println("active")
		atomic.StoreUint64(&s.view, view)
	}
	epochConfig.applied = true
	atomic.StorePointer(&s.currentEpochConfig, unsafe.Pointer(epochConfig))

	log.Debugf("replica %d: deleting epoch config for view %d", s.id, s.view-1)
	delete(s.epochConfig, s.view-1)
	s.maybeAllowMoreProposingInstances()

	if s.isInRecovery() && (s.primaryIDView(s.getView()+1) == s.id) && !s.isLeader(s.id) {
		log.Errorf("replica %d: primary for next epoch but not in the leader set; sending epoch config asynchronously", s.id)
		ec, err := s.makeEpochConfig(epochConfig.epoch+1, true, s.epochConfig[epochConfig.epoch].last+1)
		if err != nil {
			log.Errorf("replica %d: could not create epoch", s.id)
		} else {
			s.sendEpochCfg(ec, s.id)
		}
	}

}

func (s *SBFT) handleEpochConfig(config *pb.EpochConfig, src uint64) {
	log.Debugf("handling epoch config")
	// create new epoch configuration entry
	// at this point we cannot validate the configuration entry, information from previous epochs could be missing
	// we just know that all the nodes agreed on the same configuration
	// upon the epoch change if the configuration is found invalid an utimely epoch change should be triggered (view change)
	s.epochConfig[config.Epoch] = &epochConfig{
		src:              src,
		epoch:            config.Epoch,
		primary:          config.Primary,
		leaders:          config.Leaderset,
		blacklist:        config.Blacklist,
		length:           config.Length,
		first:            config.First,
		last:             config.First + config.Length,
		nextActiveBucket: config.RotationOffset,
		newview:          &pb.NewView{Config: config},
	}
	log.Debugf("replica %d: inserting new epoch config for epoch %d: %v", s.id, config.Epoch, config)
	//s.lock.Lock()
	//s.maybeNewEpoch(config.Epoch)
	//s.lock.Unlock()
}

func (s *SBFT) validateEpochConfig(config *epochConfig, prevEpoch uint64) error {
	return nil

	// TODO
	log.Errorf("replica %d: validating epoch configuration %d against epoch %d: %+v", s.id, config.epoch, prevEpoch, s.epochConfig[prevEpoch])

	if _, ok := s.epochConfig[prevEpoch]; !ok {
		return errors.New("previous epoch configuration not found")
	}
	log.Debugf("replica  %d%d: previous epoch configuration %+v", s.id, s.epochConfig[prevEpoch])
	if config.primary != config.src {
		return errors.New(fmt.Sprintf("Recieved epoch in batch from invalid leader %d", s.id))
	}
	if config.primary != s.primaryIDView(config.epoch) {
		return errors.New(fmt.Sprintf("Recieved epoch configuration for %d from invalid epoch primary %d", config.epoch, config.primary))
	}
	increase := false
	if s.activeView { // last epoch change completed successfully
		increase = true
	}
	if s.newEpochLength(s.epochConfig[prevEpoch], increase) != config.length {
		return errors.New(fmt.Sprintf("Invalid epoch length %d. Expected: %d", config.length, s.newEpochLength(s.epochConfig[prevEpoch], increase)))
	}
	if s.newLeaderSetSize(s.epochConfig[prevEpoch], increase) != uint64(len(config.leaders)) {
		return errors.New(fmt.Sprintf("Invalid leader size length %d. Expected: %d", uint64(len(config.leaders)), s.newLeaderSetSize(s.epochConfig[prevEpoch], increase)))
	}
	return nil
}

func (s *SBFT) checkForEpochConfig(pp *pb.Preprepare) {
	header := pp.Batch.DecodeHeader()
	if header.IsConfig == 1 {
		config := &pb.EpochConfig{}
		err := proto.Unmarshal(pp.Batch.Payloads[len(pp.Batch.Payloads)-1], config)
		if err != nil {
			log.Warningf("replica %d: Configuration batch %d does not contain a configuration request", s.id, header.Seq)
		} else {
			s.handleEpochConfig(config, pp.Leader)
		}
	}
}

//with the current epoch as parameter
func (s *SBFT) makeNextEpochConfig(epoch uint64, increase bool, first uint64, current *epochConfig) (*pb.EpochConfig, error) {
	log.Debugf("replica %d: creating epoch configuration for epoch %d, starting from %d", s.id, epoch, first)

	blacklist := make([]uint64, 0, 0)
	for leader, _ := range s.blacklistedLeaders {
		blacklist = append(blacklist, leader)
	}

	s.nextActiveBucketLock.Lock()
	next := s.getNextActiveBucket()
	s.nextActiveBucketLock.Unlock()
	log.Debugf("replica %d: previous epoch configuration %+v", s.id, current)
	epochConfig := &pb.EpochConfig{
		Epoch:          epoch,
		Primary:        s.id,
		First:          current.last + 1,
		Length:         uint64(config.Config.BucketRotationPeriod),
		Leaderset:      s.newLeaderSet(s.newLeaderSetSize(current, increase)),
		RotationOffset: next,
		Blacklist:      blacklist,
	}
	if !increase {
		epochConfig.First = first
	}
	log.Infof("replica %d: new epoch configuration %+v", s.id, epochConfig)
	return epochConfig, nil
}

func (s *SBFT) makeEpochConfig(epoch uint64, increase bool, first uint64) (*pb.EpochConfig, error) {
	log.Debugf("replica %d: creating epoch configuration for epoch %d, starting from %d", s.id, epoch, first)
	if _, ok := s.epochConfig[epoch-1]; !ok {
		return nil, errors.New("previous configuration not found")
	}

	blacklist := make([]uint64, 0, 0)
	for leader, _ := range s.blacklistedLeaders {
		blacklist = append(blacklist, leader)
	}
	s.nextActiveBucketLock.Lock()
	next := s.getNextActiveBucket()
	s.nextActiveBucketLock.Unlock()
	log.Debugf("replica %d: previous epoch configuration %+v", s.id, s.epochConfig[epoch-1])
	config := &pb.EpochConfig{
		Epoch:          epoch,
		Primary:        s.id,
		First:          s.epochConfig[epoch-1].last + 1,
		Length:         uint64(config.Config.BucketRotationPeriod),
		Leaderset:      s.newLeaderSet(s.newLeaderSetSize(s.epochConfig[epoch-1], increase)),
		Blacklist:      blacklist,
		RotationOffset: next,
	}
	if !increase {
		config.First = first
	}
	log.Errorf("replica %d: new epoch configuration %+v", s.id, config)
	return config, nil
}

func (s *SBFT) newEpochLength(config *epochConfig, increase bool) uint64 {
	if increase {
		return config.length + 1
	}
	next := config
	if !next.applied {
		next = s.getCurrentEpochConfig()
	}
	if !next.applied {
		log.Fatal()
	}
	//for {
	//	if next.applied {
	//		break
	//	}
	//	if c, ok := s.epochConfig[next.epoch-1]; ok {
	//		next = c
	//	} else {
	//		log.Fatal()
	//	}
	//}
	length := next.length / (2 * (1 + config.epoch - next.epoch))
	if length <= 0 {
		return 1
	}
	return length

}

func (s *SBFT) newLeaderSetSize(epochConfig *epochConfig, increase bool) uint64 {
	if increase {
		return min(uint64(len(epochConfig.leaders)+1), uint64(config.Config.N-len(s.blacklistedLeaders)))
	}
	next := epochConfig
	if !next.applied {
		next = s.getCurrentEpochConfig()
	}
	if !next.applied {
		log.Fatal()
	}
	//for {
	//	if next.applied {
	//		break
	//	}
	//	if c, ok := s.epochConfig[next.epoch-1]; ok {
	//		next = c
	//	} else {
	//		log.Fatal()
	//	}
	//}
	if uint64(len(next.leaders)) < (1 + epochConfig.epoch - next.epoch) {
		return 1
	}
	size := uint64(len(next.leaders)) - (1 + epochConfig.epoch - next.epoch)
	if size == 0 {
		return 1
	}
	log.Criticalf("new leader set size: %d %d %d", size, epochConfig.epoch, next.epoch)
	return size
}

func (s *SBFT) newLeaderSet(size uint64) []uint64 {
	// TODO take size into account for cascading epoch changes
	log.Criticalf("Creating leader set with size %d", size)
	oldLeaders := s.leaders
	leaders := make([]uint64, 0, 0)
	leaders = append(leaders, s.id)
	count := uint64(0)
	for _, leader := range oldLeaders {
		if count >= size {
			break
		}
		if leader == s.id {
			size = size + 1
			continue
		}
		_, ok := s.blacklistedLeaders[leader]
		if ok {
			log.Criticalf("Replica %d: removing node %d from leaderset because it is blacklisted", s.id, leader)
			s.blacklistedLeaders[leader] = 0
			continue
		}
		leaders = append(leaders, leader)
		count++
	}
	log.Criticalf("replica %d: new leaderset size %d", s.id, len(leaders))
	return leaders
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func minInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func maxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
