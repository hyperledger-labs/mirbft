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
	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
)

func (s *SBFT) makeCheckpoint() *pb.Checkpoint {
	lastSeq := s.lastCheckpoint.subject.Seq.Seq
	maxSeq := lastSeq + uint64(config.Config.CheckpointDist)
	digests := make([][]byte, 0, 0)
	s.curLock.RLock()
	for seq := lastSeq + 1; seq <= maxSeq; seq++ {
		_, ok := s.cur[seq]
		if !ok {
			log.Errorf("replica %d cannot find entry in cur with sequence %d", s.id, seq)
		}
		digests = append(digests, s.cur[seq].subject.Digest)
	}

	s.lastCheckpoint = s.cur[maxSeq]

	s.curLock.RUnlock()
	digest := hashDataArray(digests)
	log.Debugf("replica %d: Digest for checkpoint %x", s.id, digest)
	signature := s.sys.Sign(digest)
	c := &pb.Checkpoint{
		Seq:       maxSeq,
		Digest:    digest,
		Signature: signature,
	}
	s.lastCheckpoint.checkpointDone = false
	log.Infof("Sending CHECKPOINT for %d", maxSeq)
	return c
}

func (s *SBFT) maybeSendCheckpoint(seq uint64) {
	if seq == s.lastCheckpoint.subject.Seq.Seq+uint64(config.Config.CheckpointDist) {
		s.sendCheckpoint()
		s.managerDispatcher.processBacklog()
	}
}

func (s *SBFT) sendCheckpoint() {
	s.broadcast(&pb.Msg{Type: &pb.Msg_Checkpoint{s.makeCheckpoint()}})
}

func (s *SBFT) handleCheckpoint(c *pb.Checkpoint, src uint64) {
	log.Infof("replica %d: got checkpoint from: %d for %d", s.id, src, c.Seq)

	if c.Seq == s.lastCheckpoint.subject.Seq.Seq && s.lastCheckpoint.checkpointDone {
		log.Debugf("replica %d: checkpoint for: %d already done", s.id, c.Seq)
		return
	}

	if c.Seq < s.lastCheckpoint.subject.Seq.Seq {
		log.Debugf("replica %d: checkpoint from %d for: %d old", s.id, src, c.Seq)
		return
	}

	if c.Seq != s.lastCheckpoint.subject.Seq.Seq {
		log.Errorf("replica %d: checkpoint does not match expected subject %v, got %v", s.id, s.lastCheckpoint.subject, c)
		return
	}
	if _, ok := s.lastCheckpoint.checkpoint[src]; ok {
		log.Errorf("replica %d: duplicate checkpoint for %d from %d", s.id, c.Seq, src)
	}
	s.lastCheckpoint.checkpoint[src] = c
	log.Debugf("replica %d: %+v", s.id, s.lastCheckpoint.checkpoint)

	max := "_"
	sums := make(map[string][]uint64)
	for csrc, c := range s.lastCheckpoint.checkpoint {
		sum := hash2str(c.Digest)
		sums[sum] = append(sums[sum], csrc)

		if len(sums[sum]) >= s.oneCorrectQuorum() {
			max = sum
		}
	}

	replicas, ok := sums[max]
	if !ok {
		log.Debugf("replica %d: incorrect sum for checkpoint: %+v", s.id, sums)
		return
	}

	log.Debugf("replica %d: one correct quorum  for checkpoint: %d", s.id, s.lastCheckpoint.subject.Seq.Seq)
	log.Infof("replica %d: Checkpoint %d", s.id, s.lastCheckpoint.subject.Seq.Seq)

	// got a weak checkpoint
	c = s.lastCheckpoint.checkpoint[replicas[0]]

	//TODO fix his condition, it is not the lastCheckpoint only we should compare to but all digests up to this point
	//if !reflect.DeepEqual(c.Digest, hash2str(s.lastCheckpoint.subject.Digest)) {
	//	log.Warningf("replica %d: weak checkpoint %x does not match our state %x --- primary %d of view %d is probably Byzantine, sending view change",
	//		s.id, c.Digest, s.lastCheckpoint.subject.Digest, s.primaryID(), s.view)
	//	s.sendViewChange()
	//	return
	//}

	// garbage collect
	s.curLock.Lock()
	for seq, batch := range s.cur {
		if seq <= s.lastCheckpoint.subject.Seq.Seq {
			if batch.timeout != nil {
				batch.timeout.Cancel()
			}
			delete(s.cur, seq)
		}
	}
	s.curLock.Unlock()

	log.Infof("replica %d: Garbage collecting current done", s.id)

	//some statistics

	approxPendingSize := 0
	approxPrepreparedSize := 0
	approxPrepreparedSizeAfter := 0
	approxClientsDeliveredSize := 0
	clientDeleted := 0
	prepreparedDeleted := 0

	//toBeDeleted := make(map[uint64][]string)
	for b, _ := range s.bucketLocks {
		s.toBeDeleted[b] = s.toBeDeleted[b][:0]
	}

	s.clientsLock.RLock()
	//oldLowWatermarks:= make(map[string]uint64, len(s.clients))

	for id, client := range s.clients {
		log.Infof("replica %d: before checkpoint client %s: low=%d, high=%d", s.id, id, client.lowWatermark, client.highWatermark)
		lw := client.getLowWatermark()
		hw := client.getHighWatermark()
		//oldLowWatermarks[id] = lw
		maxW := lw
		approxClientsDeliveredSize += len(client.delivered)

		for i := lw; i < hw; i++ {
			if request, ok := client.delivered[i]; ok {
				if request.seq <= s.lastCheckpoint.subject.Seq.Seq {
					maxW++
					bucket := s.getBucket(request.digest)
					s.toBeDeleted[bucket] = append(s.toBeDeleted[bucket], hash2str(request.digest))
					delete(client.delivered, i)
					clientDeleted++
				} else {
					maxW = i
					break
				}
			} else {
				maxW = i
				break
			}
		}
		client.setLowWatermark(maxW)
		client.setHighWatermark(maxW + uint64(config.Config.ClientWatermarkDist))
		log.Infof("replica %d: after checkpoint client %s: low=%d, high=%d", s.id, id, client.lowWatermark, client.highWatermark)
	}

	s.clientsLock.RUnlock()

	for b, keys := range s.toBeDeleted {
		//for b, lock := range s.bucketLocks {
		s.bucketLocks[b].Lock()

		approxPrepreparedSize += len(s.preprepared[b])
		approxPendingSize += len(s.pending[b])

		for _, key := range keys {
			prepreparedDeleted++
			//TODO remove the next check; for now it's just to verify the correctness of the approach
			_, ok = s.preprepared[b][key]
			if !ok {
				log.Errorf("Delivered request not found in preprepared")
			}
			delete(s.preprepared[b], key)
			log.Infof("replica %d: deleting from preprepared %s", s.id, key)

		}

		approxPrepreparedSizeAfter += len(s.preprepared[b])
		s.bucketLocks[b].Unlock()

	}

	s.curLock.RLock()
	curSize := len(s.cur)
	s.curLock.RUnlock()

	log.Infof("replica %d: Garbage collecting delivered done", s.id)
	log.Infof("replica %d: Garbage collecting preprepared done", s.id)
	log.Infof("replica %d: number of elements in pending %d, preprepared before %d, preprepared after %d, clients.delivered %d, cur %d, deleted from delivered %d, deleted from preprepared %d", s.id, approxPendingSize, approxPrepreparedSize, approxPrepreparedSizeAfter, approxClientsDeliveredSize, curSize, clientDeleted, prepreparedDeleted)

	//for b, p := range s.preprepared{
	//	// TODO lock the bucket in general?
	//	s.bucketLocks[uint64(b)].Lock()
	//	for key, clientReq := range  p {
	//		if client, ok := s.clients[clientReq.client]; ok {
	//			if clientReq.seq < client.lowWatermark {
	//				delete(p, key)
	//			}
	//		} else {
	//			log.Errorf("Client %s not found", clientReq.client)
	//		}
	//	}
	//	s.bucketLocks[uint64(b)].Unlock()
	//}
	//
	//log.Criticalf("replica %d: Garbage collecting preprepared done", s.id)

	s.advanceWatermarks(s.lastCheckpoint.subject.Seq.Seq)

	log.Infof("CHECKPOINT %d done", s.lastCheckpoint.subject.Seq.Seq)
	s.lastCheckpoint.checkpointDone = true

	for _, d := range s.blockHandlingDispatchers {
		d.batchCompleted(s.lastCheckpoint.subject.Seq.Seq)
	}
	//s.maybeSendNextBatch() //since we're not advancing watermarks here, this shouldn't change when we send batches
}
