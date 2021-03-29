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
	"github.com/golang/protobuf/proto"
	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
)

func (instance *BFTInstance) maybeSendCommit() {
	batch := instance.batch
	if batch.prepared {
		return
	}
	if batch.preprep == nil || len(batch.prep) < instance.sbft.commonCaseQuorum()-1 {
		return
	}
	sbft := instance.sbft
	if !sbft.isInRecovery() && config.Config.SigSharding && len(batch.verifiers) < sbft.oneCorrectQuorum()-1 {
		return
	}
	log.Infof("PREPARED %d", batch.subject.Seq.Seq)
	instance.sendCommit()
	//s.processBacklog() //TODO is that necessary here?
}

func (instance *BFTInstance) sendCommit() {
	batch := instance.batch
	batch.prepared = true
	c := batch.subject
	//instance.backend.Persist(s.chainId, prepared + strconv.Itoa(int(seq % s.config.WatermarkDist)), c)

	commit := &pb.Commit{
		Seq:    c.Seq,
		Digest: c.Digest,
	}

	if config.Config.BatchSignature {
		commit.Signature = instance.sbft.sys.Sign(c.Digest)
	}

	instance.sbft.broadcastSystem(&pb.Msg{Type: &pb.Msg_Commit{commit}}, instance.backend)
}

func (instance *BFTInstance) handleCommit(c *pb.Commit, src uint64) {
	if config.Config.BatchSignature {
		err := instance.sbft.sys.CheckSig(c.Digest, src, c.Signature)
		if err != nil {
			log.Noticef("replica %d: invalid commit message: %s", instance.sbft.id, err)
			return
		}
	}

	batch := instance.batch
	if !proto.Equal(&pb.Subject{Seq: c.Seq, Digest: c.Digest}, batch.subject) {
		log.Errorf("replica %d: commit does not match expected subject %v, got %v", instance.sbft.id, batch.subject, c)
		return
	}
	if _, ok := batch.commit[src]; ok {
		log.Infof("replica %d: duplicate commit for %v from %d", instance.sbft.id, c.Seq, src)
		return
	}

	batch.commit[src] = c

	//maybe mark as committed
	if batch.committed {
		return
	}
	if batch.preprep == nil || len(batch.commit) < instance.sbft.commonCaseQuorum() {
		return
	}
	batch.committed = true
	log.Noticef("replica %d: executing %v %x", instance.sbft.id, batch.subject.Seq, batch.subject.Digest)
	log.Infof("COMMITTED %d with %d requests", batch.subject.Seq.Seq, len(batch.preprep.Batch.Payloads))

	// Send nil to the instance's message channel to allow it to shut down
	// (The instance's goroutine is probably blocked waiting for a message on this channel.)
	instance.msgChan <- nil

	// Send nil to the instance's message channel to allow it to shut down
	// (The instance's goroutine is probably blocked waiting for a message on this channel.)
	instance.msgChan <- nil
	s := instance.sbft
	//instance.backend.Persist(s.chainId, committed, batch.subject)

	// Question: What exactly does this condition mean?
	if !s.leads(s.id, c.Seq.Seq, instance.epoch) {
		for i, payload := range batch.preprep.Batch.Payloads {
			digest := instance.batch.requestHashes[i]
			key := hash2str(digest)
			bucket := s.getBucket(digest)
			//s.lock.Lock()

			s.bucketLocks[bucket].Lock()
			if s.deleteFromPending(bucket, key) {
				if s.bucketToLeader[bucket] == s.id {
					s.subtractFromLeaderPendingSize(int64(len(payload)))
				}
			}

			//if _, ok := s.pending[bucket][key]; ok {
			//delete(s.pending[bucket], key)
			//}
			s.bucketLocks[bucket].Unlock()

			//for _, bucket := range s.activeBuckets[batch.preprep.Leader]{
			//	if _, ok := s.pending[bucket][key]; ok {
			//		delete(s.pending[bucket], key)
			//	}
			//}
			//s.lock.Unlock()
		}
		log.Debugf("replica %d: Removed request from pending of %d. ", s.id, batch.preprep.Leader)
		//s.nextSeq(src) //TODO is this needed??
	}
	instance.forwardForDelivery(batch)
	//s.processBacklog()                            //TODO is this necessary here?
}
