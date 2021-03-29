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
	"encoding/binary"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
	"github.com/golang/protobuf/proto"
)

func (instance *BFTInstance) sendPreprepare(batch []*pb.Request, seq pb.SeqView) {
	//	seq := s.nextSeq(s.id)
	seqNos := make([]uint64, len(batch))
	data := make([][]byte, len(batch))
	sig := make([][]byte, len(batch))
	pk := make([][]byte, len(batch))
	for i, req := range batch {
		seqNos[i] = req.Seq
		data[i] = req.Payload
		sig[i] = req.Signature
		pk[i] = req.PubKey
	}

	s := instance.sbft

	var isConfig uint64 = 0
	// Piggybacking the next epoch configuration at the last batch of the current epoch
	if s.isInRecovery() {
		if s.primaryIDView(s.getView()+1) == s.id {
			//next:=seq.Seq + uint64(len(s.epochConfig[seq.View].leaders))
			next := seq.Seq + uint64(len(instance.epoch.leaders))
			if next > instance.epoch.last { //this must be the last proposal I make in this view
				log.Infof("replica %d: sending configuration for epoch %d", s.id, s.view+1)
				isConfig = 1
				epochConfig, err := s.makeNextEpochConfig(instance.epoch.epoch+1, true, 0, instance.epoch)
				if err != nil {
					log.Errorf("replica %d: Failed to create new epoch configuration: %s", s.id, err.Error())
				}
				configRaw, err := proto.Marshal(epochConfig)
				if err != nil {
					log.Warningf("replica %d: Failed to create new epoch configuration: %s", s.id, err.Error())
				}
				data = append(data, configRaw)
				sig = append(sig, nil)
				pk = append(pk, nil)
				seqNos = append(seqNos, 0)
				if config.Config.SignatureVerification {
					sig[len(sig)-1] = instance.backend.Sign(configRaw)
				}
			}
		}
	}

	// Piggybacking the highest covered point of the hash space in the first block when entering stable mode
	if !s.isInRecovery() {
		if seq.Seq >= instance.epoch.first && seq.Seq < instance.epoch.first+uint64(config.Config.N-len(s.blacklistedLeaders)) {
			isConfig = 2
			buffer := make([]byte, 8)
			s.nextActiveBucketLock.Lock()
			nextActiveBucket := s.getNextActiveBucket()
			binary.LittleEndian.PutUint64(buffer, nextActiveBucket)
			s.nextActiveBucketLock.Unlock()
			var signature []byte
			if config.Config.SignatureVerification {
				signature = s.sys.Sign(buffer)
			}
			data = append(data, buffer)
			sig = append(sig, signature)
			pk = append(pk, nil)
			seqNos = append(seqNos, 0)
			if config.Config.SignatureVerification {
				sig[len(sig)-1] = s.sys.Sign(buffer)
			}
		}
	}

	payload, lightPayload, batchHeader := s.makeBatch(seq.Seq, seqNos, data, sig, pk, isConfig)
	batchHash := batchHeader.DataHash
	reqHashes := lightPayload.Payloads

	m := &pb.Preprepare{
		Leader: s.id,
		Seq:    &seq,
		Batch:  payload,
	}

	tracing.MainTrace.Event(tracing.PROPOSE, int64(seq.Seq), 0)

	if config.Config.PayloadSharding {
		mLight := &pb.Preprepare{
			Leader: s.id,
			Seq:    &seq,
			Batch:  lightPayload,
		}
		for i := uint64(0); i < uint64(config.Config.N); i++ {
			if i == s.id {
				continue
			}
			if s.getsPayload(i, batchHash) {
				log.Infof("replica %d: PREPREPARE %d to %d", s.id, seq.Seq, i)
				instance.backend.Send(s.chainId, &pb.Msg{Type: &pb.Msg_Preprepare{m}}, i)
			} else {
				log.Infof("replica %d: LIGHT PREPREPARE %d to %d", s.id, seq.Seq, i)
				instance.backend.Send(s.chainId, &pb.Msg{Type: &pb.Msg_Preprepare{mLight}}, i)
			}
		}
	} else {
		s.broadcastToRest(&pb.Msg{Type: &pb.Msg_Preprepare{m}}, instance.backend)
		log.Infof("replica %d: PREPREPARE %d", s.id, seq.Seq)
	}
	instance.handleCheckedPreprepare(m, batchHeader, reqHashes)
}

func (instance *BFTInstance) handlePreprepare(pp *pb.Preprepare, src uint64) {
	s := instance.sbft

	log.Infof("replica %d: Received PREPREPARE %d", instance.sbft.id, pp.Seq.Seq)
	//TODO if preprepare from self, call directly handle checked preprepare

	if src == s.id {
		log.Infof("replica %d: ignoring preprepare from self: %d", s.id, src)
		return
	}

	if !s.leads(src, pp.Seq.Seq, instance.epoch) {
		log.Infof("replica %d: replica %d does not lead %d", s.id, src, pp.Seq.Seq)
		return
	}

	if src != pp.Leader {
		log.Infof("replica %d: leader %d does not match source %d", s.id, pp.Leader, src)
		return
	}
	//if !sbft.inWatermarkRange(pp.Seq.Seq) {
	//	log.Infof("replica %d: preprepare outside of watermarks [%v,%v) , got %v", sbft.id, sbft.lowWatermark, sbft.highWatermark, pp.Seq.Seq)
	//	return
	//}

	//TODO perform the following check before spawining the thread
	//if batch, ok := s.cur[pp.Seq.Seq]; ok {
	//	if batch.preprep != nil {
	//	log.Infof("replica %d: duplicate preprepare for %v", s.id, pp.Seq.Seq) return
	//	}
	//}

	batchHeader, err := s.checkBatch(pp.Seq, pp.Batch, true, false)

	if err != nil {
		log.Criticalf("replica %d: %s in %d", s.id, err.Error(), pp.Seq.Seq)
		instance.requestViewChange()
		return
	}

	if !s.isInRecovery() {
		if pp.Seq.Seq >= instance.epoch.first && pp.Seq.Seq < instance.epoch.first+uint64(config.Config.N-len(s.blacklistedLeaders)) {
			if !(batchHeader.IsConfig == 2) {
				log.Infof("replica %d: bucket rotation offset not found at seq no %d", s.id, pp.Seq.Seq)
				return
			}
		}
	}

	batchHash := batchHeader.DataHash
	var requestHashes [][]byte
	var blockOK bool
	if config.Config.PayloadSharding {
		if s.getsPayload(s.id, batchHash) {
			blockOK, requestHashes = s.revalidateBatch(pp.Leader, src, pp.Seq.Seq, pp.Batch, batchHeader)
			if !blockOK {
				log.Debugf("Replica %d found Byzantine block in preprepare, Seq: %d View: %d", s.id, pp.Seq.Seq, pp.Seq.View)
				instance.requestViewChange()
				return
			}
		} else {
			requestHashes = make([][]byte, len(pp.Batch.Payloads))
			for i, payload := range pp.Batch.Payloads {
				requestHashes[i] = payload
			}
		}

	} else {
		blockOK, requestHashes = s.revalidateBatch(pp.Leader, src, pp.Seq.Seq, pp.Batch, batchHeader)
		if !blockOK {
			log.Debugf("Replica %d found Byzantine block in preprepare, Seq: %d View: %d", s.id, pp.Seq.Seq, pp.Seq.View)
			instance.requestViewChange()
			return
		}
	}
	log.Infof("replica %d: handlePreprepare from %d", s.id, pp.Leader)
	instance.handleCheckedPreprepare(pp, batchHeader, requestHashes)
}

func (instance *BFTInstance) acceptPreprepare(pp *pb.Preprepare, batchHeader *pb.BatchHeader, requestHashes [][]byte) {
	sub := &pb.Subject{Seq: pp.Seq, Digest: batchHeader.DataHash}

	sbft := instance.sbft

	log.Infof("replica %d: accepting preprepare for %v, %x, with %d reqs", sbft.id, sub.Seq, sub.Digest, len(pp.Batch.Payloads))
	//instance.backend.Persist(s.chainId, preprepared + strconv.Itoa(int(sub.Seq.Seq % s.config.WatermarkDist)), pp)

	instance.batch = &batchInfo{
		subject:       sub,
		timeout:       nil,
		preprep:       pp,
		requestHashes: requestHashes,
		prep:          make(map[uint64]*pb.Subject),
		commit:        make(map[uint64]*pb.Commit),
		checkpoint:    make(map[uint64]*pb.Checkpoint),
		verifiers:     make([]uint64, 0, 0),
	}

	sbft.curLock.Lock()
	sbft.cur[pp.Seq.Seq] = instance.batch
	sbft.curLock.Unlock()
	log.Infof("ACCEPTED %d", sub.Seq.Seq)
}

func (s *SBFT) createPreprepareEntry(pp *pb.Preprepare, batchHeader *pb.BatchHeader, requestHashes [][]byte) {
	sub := &pb.Subject{Seq: pp.Seq, Digest: batchHeader.DataHash}

	log.Infof("replica %d: accepting preprepare for %v, %x, with %d reqs", s.id, sub.Seq, sub.Digest, len(pp.Batch.Payloads))
	//instance.backend.Persist(s.chainId, preprepared + strconv.Itoa(int(sub.Seq.Seq % s.config.WatermarkDist)), pp)

	batch := &batchInfo{
		subject:       sub,
		timeout:       nil,
		preprep:       pp,
		requestHashes: requestHashes,
		prep:          make(map[uint64]*pb.Subject),
		commit:        make(map[uint64]*pb.Commit),
		checkpoint:    make(map[uint64]*pb.Checkpoint),
		verifiers:     make([]uint64, 0, 0),
	}

	//TODO do we nees reqHashes here???

	s.curLock.Lock()
	s.cur[pp.Seq.Seq] = batch
	s.curLock.Unlock()

	log.Infof("CREATED %d", sub.Seq.Seq)
}

func (instance *BFTInstance) handleCheckedPreprepare(pp *pb.Preprepare, batchHeader *pb.BatchHeader, requestHashes [][]byte) {
	sbft := instance.sbft
	//if pp.Leader == sbft.id {
	//	sbft.bucketLocks[sbft.id].Lock()
	//	for i, reqPayload := range pp.Batch.Payloads {
	//		key := hash2str(requestHash(reqPayload, pp.Batch.SeqNos[i], pp.Batch.Pub[i]))
	//		delete(sbft.pending[sbft.id], key)
	//	}
	//	sbft.bucketLocks[sbft.id].Unlock()
	//}
	instance.acceptPreprepare(pp, batchHeader, requestHashes)
	if !(pp.Leader == sbft.id) {
		instance.sendPrepare(pp.Seq.Seq)
		//s.processBacklog()
	}
	instance.maybeSendCommit() //TODO this shouldn't really happen; (unless it's just one replica)
}

////////////////////////////////////////////////
