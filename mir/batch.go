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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
	"github.com/golang/protobuf/proto"
)

func (s *SBFT) makeBatch(seq uint64, seqNos []uint64, data [][]byte, sig [][]byte, pk [][]byte, isConfig uint64) (*pb.Batch, *pb.Batch, *pb.BatchHeader) {
	batchHash, reqHashes := makeBatchHash(seqNos, data, pk)
	batchhead := &pb.BatchHeader{
		Seq:      seq,
		IsConfig: isConfig,
		DataHash: batchHash,
	}
	rawHeader, err := proto.Marshal(batchhead)
	if err != nil {
		panic(err)
	}
	batch := &pb.Batch{
		Header:      rawHeader,
		SeqNos:      seqNos,
		Payloads:    data,
		PayloadSigs: sig,
		Pub:         pk,
	}

	lightBatch := &pb.Batch{
		Header:      rawHeader,
		SeqNos:      seqNos,
		Payloads:    reqHashes,
		PayloadSigs: nil,
		Pub:         pk,
	}
	return batch, lightBatch, batchhead
}

func makeBatchHash(seqNos []uint64, data [][]byte, pk [][]byte) (batchHash []byte, reqHashes [][]byte) {
	metadata := make([]byte, 0, 0)
	for i, c := range pk {
		t := make([]byte, 8)
		binary.LittleEndian.PutUint64(t, seqNos[i])
		metadata = append(metadata, t...)
		metadata = append(metadata, c...)
	}
	rh := make([][]byte, len(data), len(data))
	for i, data := range data {
		reqHash := requestHash(data, seqNos[i], pk[i])
		rh[i] = reqHash
	}
	return hashDataArray(append(rh, hash(metadata))), rh
}

func makeLightBatchHash(seqNos []uint64, data [][]byte, pk [][]byte) (batchHash []byte, reqHashes [][]byte) {
	metadata := make([]byte, 0, 0)
	for i, c := range pk {
		t := make([]byte, 8)
		binary.LittleEndian.PutUint64(t, seqNos[i])
		metadata = append(metadata, t...)
		metadata = append(metadata, c...)
	}
	return hashDataArray(append(data, hash(metadata))), data
}

func (s *SBFT) checkBatch(seq *pb.SeqView, b *pb.Batch, checkData bool, needSigs bool) (*pb.BatchHeader, error) {
	batchheader := &pb.BatchHeader{}
	err := proto.Unmarshal(b.Header, batchheader)
	if err != nil {
		return nil, err
	}
	var batchHash []byte
	if s.getsPayload(s.id, batchheader.DataHash) {
		batchHash, _ = makeBatchHash(b.SeqNos, b.Payloads, b.Pub)
	} else {
		batchHash, _ = makeLightBatchHash(b.SeqNos, b.Payloads, b.Pub)
	}

	if checkData {
		if len(b.Payloads) > 0 {
			if !reflect.DeepEqual(batchHash, batchheader.DataHash) {
				return nil, fmt.Errorf("malformed batches: invalid hash")
			}
		}
	}

	if batchheader.Seq != seq.Seq {
		return nil, fmt.Errorf("malformed batch header: invalid sequence number")
	}

	if needSigs {
		if len(b.Signatures) < s.oneCorrectQuorum() {
			return nil, fmt.Errorf("insufficient number of signatures on batches: need %d, got %d", s.oneCorrectQuorum(), len(b.Signatures))
		}
		bh := hash(b.Header)
		for r, sig := range b.Signatures {
			err = s.sys.CheckSig(bh, r, sig)
			if err != nil {
				return nil, err
			}
		}
	}

	return batchheader, nil
}

func (s *SBFT) revalidateBatch(leader uint64, src uint64, seq uint64, batch *pb.Batch, batchHeader *pb.BatchHeader) (bool, [][]byte) {
	requestHashes := make([][]byte, len(batch.Payloads), len(batch.Payloads))
	batchHash := batchHeader.DataHash
	var wg sync.WaitGroup
	wg.Add(len(batch.Payloads))
	valid := true
	for i := 0; i < len(batch.Payloads); i++ {

		go func(i int, valid *bool) {
			defer wg.Done()

			digest, bucket, err := s.revalidateRequest(batch.SeqNos[i], batch.Payloads[i], batch.Pub[i])
			requestHashes[i] = digest
			if err != nil {
				log.Criticalf(err.Error())
				*valid = false
				return
			}

			if config.Config.SignatureVerification && (s.verifies(s.id, batchHash) || s.isInRecovery()) {
				if batchHeader.IsConfig != 0 {
					if i == len(batch.Payloads)-1 {
						log.Debugf("replica %d: configuration request signature validation, src %d", s.id, src)
						err := s.sys.CheckSig(batch.Payloads[i], src, batch.PayloadSigs[i])
						if err != nil {
							log.Criticalf("replica %d: invalid configuration request signature %s", s.id, err.Error())
							*valid = false
							return
						}
					}
				} else {
					log.Debugf("replica %d: request signature validation, src %d", s.id, src)
					err := verifyRequestSignature(requestHashes[i], batch.Pub[i], batch.PayloadSigs[i])
					if err != nil {
						log.Criticalf("replica %d: invalid request signature (%x,%d): %s", s.id, batch.Pub[i], batch.SeqNos[i], err.Error())
						*valid = false
						return
					}
				}
			}
			if batchHeader.IsConfig != 0 {
				if i == len(batch.Payloads)-1 {
					requestHashes[i] = requestHash(batch.Payloads[i], batch.SeqNos[i], batch.Pub[i])
					return
				}
			}

			if !s.inActiveBucket(src, seq, digest) {
				log.Criticalf("replica %d: request with sequence %d is out of proposers range; pp.leader %d, src %d", s.id, seq, leader, src)
				*valid = false
				return
			}
			s.bucketLocks[bucket].RLock()
			if _, ok := s.preprepared[bucket][hash2str(digest)]; ok {
				log.Criticalf("replica %d: duplicate request %s with seq %d belonging to bucket %d, leader is %d, src is %d, seq is %d", s.id, hash2str(digest), batch.SeqNos[i], s.getBucket(digest), leader, src, seq)
				*valid = false
				s.bucketLocks[bucket].RUnlock()
				return
			}
			s.bucketLocks[bucket].RUnlock()
		}(i, &valid)
	}
	wg.Wait()
	if !valid {
		return false, nil
	}
	return true, requestHashes
}

func (s *SBFT) cutBatch() []*pb.Request {
	log.Errorf("replica %d: cutting batch", s.id)
	batch := make([]*pb.Request, 0)
	size := uint64(0)
	deleted := uint64(0)
	for _, bucket := range s.activeBuckets[s.id] {
		s.bucketLocks[bucket].Lock()

		key, req := s.popFromPending(bucket)

		for key != "" {
			log.Infof("PREPREPARING %d from %d", req.Seq, req.Nonce)
			sz := uint64(reqSize(req))
			err := s.validateRequestNoHashing(req.Seq, req.Payload, req.PubKey)
			if err == nil {
				batch = append(batch, req)
				s.preprepared[bucket][key] = &clientRequest{client: hash2str(req.PubKey), seq: req.Seq}
				size += sz
			}

			s.deleteFromPending(bucket, key)
			deleted += sz

			if size >= uint64(config.Config.BatchSizeBytes) {
				s.bucketLocks[bucket].Unlock()
				s.subtractFromLeaderPendingSize(int64(deleted))
				return batch
			}
			key, req = s.popFromPending(bucket)
		}
		s.bucketLocks[bucket].Unlock()
	}
	s.subtractFromLeaderPendingSize(int64(deleted))

	invalid := uint64(0)
	var wg sync.WaitGroup
	wg.Add(len(batch))
	for i, req := range batch {
		go func(i int, req *pb.Request, invalid *uint64) {
			defer wg.Done()
			if config.Config.SignatureVerification {
				hash := requestHash(req.Payload, req.Seq, req.PubKey)
				err := verifyRequestSignature(hash, req.PubKey, req.Signature)
				if err != nil {
					log.Criticalf("replica %d: invalid request signature (%x,%d): %s", s.id, req.PubKey, req.Seq, err.Error())
					batch[i].Nonce = 42 //TODO we do no use nonce anywhere but if we do we must use something else there
					atomic.AddUint64(invalid, 1)
				}
			}
		}(i, req, &invalid)
	}
	wg.Wait()
	if invalid > 0 {
		for i := 0; i < len(batch); i++ {
			if batch[i].Nonce == 42 {
				batch[i] = batch[len(batch)-1]
				batch = batch[:len(batch)-1]
				i--
			}
		}
	}

	return batch
}

func reqSize(req *pb.Request) int {
	return len(req.Payload)
}

func (s *SBFT) getBatches(batchSeq uint64) [][]*pb.Request {

	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	if config.Config.ByzantineDelay > 0 && (lastDelivered.subject.Seq.Seq >= uint64(config.Config.ByzantineAfter) && lastDelivered.subject.Seq.Seq < uint64(config.Config.ByzantineUntil)) {
		return nil
	}

	batches := make([][]*pb.Request, 0)
	batch := make([]*pb.Request, 0)
	size := uint64(0)

	if s.getLeaderPendingSize() >= int64(config.Config.BatchSizeBytes) {
		numLeaders := uint64(len(s.epochConfig[s.view].leaders))
		numBuckets := uint64(len(s.activeBuckets[s.id]))

		myProposalNum := batchSeq / numLeaders

		var i uint64
		deletedKeys := make([][]byte, 0, 0)

		for i = 0; i < numBuckets; i++ {
			bucket := s.activeBuckets[s.id][(myProposalNum+i)%numBuckets]
			//for _, bucket := range s.activeBuckets[s.id] {
			s.bucketLocks[bucket].Lock()

			key, req := s.popFromPending(bucket)

			for key != "" {
				log.Info("PREPREPARING %d from %d", req.Seq, req.Nonce)
				err := s.validateRequestNoHashing(req.Seq, req.Payload, req.PubKey)
				if err == nil {
					batch = append(batch, req)
					size += uint64(reqSize(req))
					s.preprepared[bucket][key] = &clientRequest{client: hash2str(req.PubKey), seq: req.Seq}
				}
				s.deleteFromPending(bucket, key)
				s.subtractFromLeaderPendingSize(int64(reqSize(req)))

				//delete(s.pending[bucket],key)

				if size >= uint64(config.Config.BatchSizeBytes) {
					deleted := uint64(0)
					var wg sync.WaitGroup
					wg.Add(len(batch))
					for i, req := range batch {
						go func(i int, req *pb.Request, deleted *uint64) {
							defer wg.Done()
							if config.Config.SignatureVerification {
								hash := requestHash(req.Payload, req.Seq, req.PubKey)
								err := verifyRequestSignature(hash, req.PubKey, req.Signature)
								if err != nil {
									log.Criticalf("replica %d: invalid request signature (%x,%d): %s", s.id, req.PubKey, req.Seq, err.Error())
									batch[i].Nonce = 42 //TODO we do no use nonce anywhere but if we do we must use something else there
									atomic.AddUint64(deleted, uint64(len(req.Payload)))
								}
							}
						}(i, req, &deleted)
					}
					wg.Wait()
					// There exist requests with invalid signature so we must remove them from the batch
					// We do this post signature verification to allow efficient parallel signature verification
					if deleted > 0 {
						size -= deleted
						for i := 0; i < len(batch); i++ {
							if batch[i].Nonce == 42 { // the request signature was invalid
								deletedKeys = append(deletedKeys, requestHash(batch[i].Payload, batch[i].Seq, batch[i].PubKey))
								batch[i] = batch[len(batch)-1]
								batch = batch[:len(batch)-1]
								i--
							}
						}
					}
					if size >= uint64(config.Config.BatchSizeBytes) {
						batches = append(batches, batch)
						s.bucketLocks[bucket].Unlock()
						return batches
					}
				}
				key, req = s.popFromPending(bucket)
			}
			s.bucketLocks[bucket].Unlock()
		}

		for _, key := range deletedKeys {
			bucket := s.getBucket(key)
			s.bucketLocks[bucket].Lock()
			delete(s.preprepared[bucket], hash2str(key))
			s.bucketLocks[bucket].Unlock()
		}

	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

func (s *SBFT) maybeSendNextBatch() {
	log.Infof("Maybe send next batch")
	if s.batchTimer != nil {
		s.batchTimer.Cancel()
		s.batchTimer = nil
	}

	if s.nextProposalInfo == nil {
		return
	}

	if !s.isLeaderInCrtEpoch(s.id) {
		log.Debugf("replica %d: not leader, should not send batch", s.id)
		return
	}

	//if!s.activeView {
	//	log.Debugf("replica %d: not active in view %d", s.id, s.getView())
	//	return
	//}

	//if !s.inWatermarkRange(s.nextSeqNo[s.id]) {
	//	log.Debugf("replica %d: batch %d not in watermark range [%d, %d), should not send batch", s.id, s.nextSeqNo[s.id], s.lowWatermark, s.highWatermark)
	//	return
	//}

	//if !s.inRecovery {
	//	if s.rotates(s.nextSeqNo[s.id]) { //TODO this check should not occur here
	//		return
	//		//log.Debugf("replica %d: HERE last rotation :%d, lastDelivered:%d", s.id, s.lastRotation, s.lastDelivered.subject.Seq.Seq)
	//		//if s.lastDelivered.subject.Seq.Seq + uint64(1) == s.lastRotation + s.config.BucketRotationPeriod  {
	//		//	s.rotateBuckets()
	//		//} else {
	//		//	return
	//		//}
	//	}
	//}

	if len(s.batches) == 0 {
		if s.getLeaderPendingSize() != 0 {
			bSeq := s.nextProposalInfo.seq
			batches := s.getBatches(bSeq.Seq)
			if len(batches) == 0 {
				s.startBatchTimer()
				return
			}
			s.batches = append(s.batches, batches...)
			log.Error("Appending batch")
		}
		if len(s.batches) == 0 {
			s.startBatchTimer()
			return
		}
	}

	//	batch := s.batches[0]
	//	s.batches = s.batches[1:]
	//s.sendPreprepare(batch)
	//	s.batchQueue<-batch

	adapter := s.nextProposalInfo.adapter
	seq := s.nextProposalInfo.seq
	s.wg.Add(1) //proposal in flight
	//TODO check for stop???
	batch := s.batches[0]
	s.batches = s.batches[1:]
	adapter.proposeBatch(batch, seq)
	s.nextProposalInfo = nil

}

func (s *SBFT) MakeBatch(seq uint64, seqNos []uint64, data [][]byte, sig [][]byte, pk [][]byte, isConfig uint64) *pb.Batch {
	batch, _, _ := s.makeBatch(seq, seqNos, data, sig, pk, isConfig)
	return batch
}
