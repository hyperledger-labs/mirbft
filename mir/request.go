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
	"errors"
	"fmt"
	"time"

	"github.com/IBM/mirbft/config"
	"github.com/IBM/mirbft/crypto"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
)

func (s *SBFT) processNewRequest(request *pb.Request, digest []byte, bucket uint64) (uint64, uint64, error) {
	if digest == nil {
		digest = requestHash(request.Payload, request.Seq, request.PubKey)
		bucket = s.getBucket(digest)
	}
	key := hash2str(digest)
	tracing.MainTrace.Event(tracing.REQ_RECEIVE, int64(request.Client), int64(request.Client))
	log.Debugf("replica %d: PROCESSING %s", s.id, key)
	err := s.validateRequestNoHashing(request.Seq, request.Payload, request.PubKey)
	if err != nil {
		log.Warningf("replica %d: %s", s.id, err.Error())
		return 0, 0, fmt.Errorf("replica %d: validateRequest error %s", s.id, err.Error())
	}

	s.bucketLocks[bucket].Lock()
	if !config.Config.ByzantineDuplication {
		if clientRequest, ok := s.preprepared[bucket][hash2str(digest)]; ok {
			log.Warningf("replica %d: duplicate request %d", s.id, clientRequest.seq)
			s.bucketLocks[bucket].Unlock()
			return 0, 0, fmt.Errorf("replica %d: duplicate request %d", s.id, clientRequest.seq)
		}
	}

	if len(s.pending[bucket]) == 0 {
		s.bucketPriorityLock.Lock()
		s.bucketPriority = append(s.bucketPriority, bucket)
		s.bucketPriorityLock.Unlock()
	}

	if s.insertInPending(bucket, key, request) {
		//s.pending[bucket][key] = request

		s.bucketToLeaderLock.RLock()
		if !config.Config.ByzantineDuplication {
			if s.bucketToLeader[bucket] == s.id {
				s.addToLeaderPendingSize(int64(reqSize(request)))
				//s.leaderPendingSize = s.leaderPendingSize + reqSize
			}
		} else {
			s.addToLeaderPendingSize(int64(reqSize(request)))
		}

		log.Debugf("Added request to pending %d. New size: %d", bucket, len(s.pending[bucket]))
		s.bucketToLeaderLock.RUnlock()
		s.bucketLocks[bucket].Unlock()
		log.Infof("replica %d: inserting %d into bucket %d", s.id, request.Seq, bucket)
		return bucket, uint64(reqSize(request)), nil
	} else {
		log.Warningf("replica %d: duplicate request %d", s.id, request.Seq)
		s.bucketLocks[bucket].Unlock()
		return 0, 0, fmt.Errorf("replica %d: duplicate request %d", s.id, request.Seq)
	}

}

func (s *SBFT) returnRequestToPending(request *pb.Request) (uint64, uint64, error) {
	digest := requestHash(request.Payload, request.Seq, request.PubKey)
	bucket := s.getBucket(digest)
	log.Errorf("replica %d: Returning client request to pending %d in bucket %d", s.id, request.Seq, bucket)
	s.bucketLocks[bucket].Lock()
	delete(s.preprepared[bucket], hash2str(digest))
	s.bucketLocks[bucket].Unlock()
	s.addRequestToPending(bucket, hash2str(digest), request)
	return bucket, uint64(reqSize(request)), nil
}

func (s *SBFT) processRequestAdditionNotification(bucket uint64, reqSize uint64) {
	log.Infof("processing client request")

	if s.nextProposalInfo == nil {
		log.Warningf("No proposal info, returning")
		return
	}

	if s.isLeaderInCrtEpoch(s.id) {
		log.Debugf("replica %d: preparing to cut Batch.", s.id)
		//s.maybeSendNextBatch()
		batches := s.getBatches(s.nextProposalInfo.seq.Seq)
		if len(batches) == 0 {
			log.Debugf("replica %d: not enough in pending to cut a batch, start timer", s.id)
			s.startBatchTimer()
		} else {
			log.Debugf("replica %d: enough in pernding to cut a batch, so maybe send next", s.id)
			s.batches = append(s.batches, batches...)
			s.maybeSendNextBatch()
		}
	} else {
		log.Infof("not leader, nothing to do with requests")
	}
}

////////////////////////////////////////////////

func (s *SBFT) startBatchTimer() {
	var duration uint64
	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	if lastDelivered.subject.Seq.Seq >= uint64(config.Config.ByzantineAfter) && lastDelivered.subject.Seq.Seq < uint64(config.Config.ByzantineUntil) {
		duration = uint64(config.Config.BatchDurationNsec + config.Config.ByzantineDelay)
	} else {
		duration = uint64(config.Config.BatchDurationNsec)
	}

	if s.batchTimer == nil {
		s.batchTimer = s.requestHandlingDispatcher.TimerForRequestHandler(time.Duration(duration), s.cutAndMaybeSend)
	}
}

func (s *SBFT) cutAndMaybeSend() {
	log.Infof("replica %d: batch timer expired: must cut a batch", s.id)
	if s.nextProposalInfo == nil {
		s.startBatchTimer()
		return
	}
	batch := s.cutBatch()
	s.batches = append(s.batches, batch)
	log.Criticalf("replica %d: batch length %d", s.id, len(batch))
	s.maybeSendNextBatch()
}

func (s *SBFT) batchSize() uint64 {
	size := uint64(0)
	if len(s.batches) == 0 {
		return size
	}
	for _, req := range s.batches[0] {
		size += uint64(reqSize(req))
	}
	return size
}

func requestHash(payload []byte, seq uint64, pk []byte) []byte {
	buffer := make([]byte, 0, 0)
	buffer = append(buffer, payload...)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, seq)
	buffer = append(buffer, b...)
	buffer = append(buffer, pk...)
	return hash(buffer)
}

func RequestHash(payload []byte, seq uint64, pk []byte) []byte {
	return requestHash(payload, seq, pk)
}

func (s *SBFT) validateRequestNoHashing(seq uint64, payload []byte, pubKey []byte) error {

	if uint64(len(payload)) > uint64(config.Config.BatchSizeBytes) {
		return fmt.Errorf(fmt.Sprintf("request too big %s", seq))
	}

	id := hash2str(pubKey)
	s.clientsLock.RLock()
	client, ok := s.clients[id]
	s.clientsLock.RUnlock()
	if !ok {
		s.clientsLock.Lock()
		c, ok2 := s.clients[id]
		if !ok2 {
			client = &clientInfo{
				lowWatermark:  0,
				highWatermark: uint64(config.Config.ClientWatermarkDist),
				delivered:     make(map[uint64]*deliveredRequest),
			}
			s.clients[id] = client
		} else {
			client = c
		}
		s.clientsLock.Unlock()
	}

	lw := client.getLowWatermark()
	hw := client.getHighWatermark()

	if seq >= hw || seq < lw {
		return fmt.Errorf(fmt.Sprintf("request timestamp %d out of client's window [%d,%d)", seq, lw, hw))
	}
	return nil
}

func (s *SBFT) revalidateRequest(seq uint64, payload []byte, pubKey []byte) ([]byte, uint64, error) {

	digest := requestHash(payload, seq, pubKey)

	if uint64(len(payload)) > uint64(config.Config.BatchSizeBytes) {
		return nil, 0, errors.New(fmt.Sprintf("replica %d: request too big %s", s.id, digest))
	}

	id := hash2str(pubKey)
	s.clientsLock.RLock()
	client, ok := s.clients[id]
	s.clientsLock.RUnlock()
	if !ok {
		s.clientsLock.Lock()
		c, ok2 := s.clients[id]
		if !ok2 {
			client = &clientInfo{
				lowWatermark:  0,
				highWatermark: uint64(config.Config.ClientWatermarkDist),
				delivered:     make(map[uint64]*deliveredRequest),
			}
			s.clients[id] = client
		} else {
			client = c
		}
		s.clientsLock.Unlock()
	}

	lw := client.getLowWatermark()
	hw := client.getHighWatermark()

	if !config.Config.ByzantineDuplication {
		if seq >= hw || seq < lw {
			return nil, 0, errors.New(fmt.Sprintf("replica %d: request timestamp %d out of client's window [%d,%d)", s.id, seq, lw, hw))
		}
	}

	bucket := s.getBucket(digest)

	return digest, bucket, nil
}

func verifyRequestSignature(hash []byte, pkRaw []byte, signature []byte) error {
	log.Debug("Verifying signature")
	pk, err := crypto.PublicKeyFromBytes(pkRaw)
	if err != nil {
		return err
	}
	err = crypto.CheckSig(hash, pk, signature)
	if err != nil {
		return err
	}
	return nil
}

func (s *SBFT) addRequestToPending(bucket uint64, key string, req *pb.Request) {
	if key == "" {
		digest := requestHash(req.Payload, req.Seq, req.PubKey)
		key = hash2str(digest)
		bucket = s.getBucket(digest)
	}
	//s.lock.Lock()
	//defer s.lock.Unlock()

	s.bucketLocks[bucket].Lock()

	if len(s.pending[bucket]) == 0 {
		s.bucketPriorityLock.Lock()
		s.bucketPriority = append(s.bucketPriority, bucket)
		s.bucketPriorityLock.Unlock()
	}

	if s.insertInPending(bucket, key, req) {
		//s.pending[bucket][key] = req

		s.bucketToLeaderLock.RLock()
		if !config.Config.ByzantineDuplication {
			if s.bucketToLeader[bucket] == s.id {
				s.addToLeaderPendingSize(int64(reqSize(req)))
				//s.leaderPendingSize = s.leaderPendingSize + reqSize
			}
		} else {
			s.addToLeaderPendingSize(int64(reqSize(req)))
		}

		log.Debugf("Added request to pending %d. New size: %d", bucket, len(s.pending[bucket]))
		s.bucketToLeaderLock.RUnlock()

		log.Debugf("replica %d: leaderPendingSize is %d", s.id, s.leaderPendingSize)
		log.Infof("replica %d: inserting %d into bucket %d", s.id, req.Seq, bucket)

	} else {
		log.Debugf("replica %d: request %s already in pending", s.id, key)
	}
	s.bucketLocks[bucket].Unlock()

}
