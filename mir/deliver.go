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
	"github.com/IBM/mirbft/crypto"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
)

func (s *SBFT) deliverBatch(batchInfo *batchInfo) {
	batch := batchInfo.preprep.Batch

	header := batch.DecodeHeader()

	//if _, ok := s.cur[header.Seq]; !ok {
	//	log.Warningf("replica %d: cannot deliver batch %d, batch not available", s.id, header.Seq)
	//	return
	//}

	//batchInfo := s.cur[header.Seq]

	batchInfo.maybeCancelTimer()
	log.Debugf("replica %d: cancelling timeout for batch %d", s.id, batchInfo.subject.Seq.Seq)

	//header.PrevHash = hash(s.lastDelivered.preprep.Batch.Header)	// building the hashchain a posteriori

	batch.Signatures = make(map[uint64][]byte)
	batch.PubKeys = make(map[uint64][]byte)
	if config.Config.BatchSignature {
		for src, c := range batchInfo.commit {
			batch.Signatures[src] = c.Signature
			pk := s.sys.GetPeerInfo(src).Cert().PublicKey
			pkBytes, err := crypto.PublicKeyToBytes(pk)
			if err != nil {
				log.Fatalf("System holds invalid key format for %d", src)
			}
			batch.PubKeys[src] = pkBytes
		}
	}

	log.Criticalf("DELIVERING %d with %d txs", header.Seq, len(batch.Payloads))
	tracing.MainTrace.Event(tracing.COMMIT, int64(header.Seq), 0)
	log.Infof("replica %d batch %d: batch size is %d requests", s.id, header.Seq, len(batch.Payloads))
	log.Infof("replica %d batch %d: number of keys is %d ", s.id, header.Seq, len(batch.Pub))
	log.Infof("replica %d batch %d: number of seqnos is %d ", s.id, header.Seq, len(batch.SeqNos))
	log.Infof("replica %d batch %d: number of hashes is %d ", s.id, header.Seq, len(batchInfo.requestHashes))

	s.clientsLock.RLock()
	for i, pk := range batch.Pub {
		if len(pk) == 0 {
			log.Debug("No public key in batch %d", header.Seq)
			continue
		}
		id := hash2str(pk)
		client := s.clients[id]
		digest := batchInfo.requestHashes[i]
		sn := batch.SeqNos[i]
		client.delivered[sn] = &deliveredRequest{header.Seq, digest}
		s.sys.Response(id, &pb.ResponseMessage{Src: s.id, Request: &pb.Request{Seq: batch.SeqNos[i]}, Response: []byte("DELIVERED")})
		log.Debugf("DELIVERED %d in %d", batch.SeqNos[i], header.Seq)
	}
	s.clientsLock.RUnlock()

	//s.getAdapter(header.Seq).batchCompleted(header.Seq)
	s.lastDelivered.Store(batchInfo)

	s.maybeSendCheckpoint(header.Seq)

	ne, err := s.maybeNewEpochConfig(s.view + uint64(1))
	if err != nil {
		log.Errorf("replica %d: skipping view %d", s.id, s.view+1)
		s.view = s.nextView()
		s.sendViewChange()
		return
	}

	if !ne {
		if !s.isInRecovery() {
			if batchInfo.subject.Seq.Seq+uint64(1) == s.lastRotation+uint64(config.Config.BucketRotationPeriod) {
				log.Debugf("replica %d: HERE last rotation :%d, lastDelivered:%d", s.id, s.lastRotation, batchInfo.subject.Seq.Seq)
				s.rotateBuckets()
				s.maybeAllowMoreProposingInstances()
			}
		}
	}
	log.Infof("replica %d: request %s %s delivered on %d (completed common case)", s.id, batchInfo.subject.Seq, hash2str(batchInfo.subject.Digest), s.id)

	next, ok := s.waitingToBeDelivered[header.Seq+1]

	if ok { // deliver next sequence number if committed
		if next.committed {
			delete(s.waitingToBeDelivered, header.Seq+1)
			s.deliverBatch(next)
		}
	}
}

func (s *SBFT) maybeDeliverBatch(bI *batchInfo) {
	//if proposer is primary and was blacklisted, remove from blacklist
	//if s.primaryID() == bI.preprep.Leader {
	//	_, ok := s.blacklistedLeaders[bI.preprep.Leader]
	//	if ok {
	//		delete(s.blacklistedLeaders, bI.preprep.Leader)
	//	}
	//}

	log.Infof("MAYBEDELIVER %d", bI.subject.Seq.Seq)
	s.cancelViewChangeTimer()

	s.checkForBucketConfig(bI.preprep)
	s.checkForEpochConfig(bI.preprep)

	if bI.timeout == nil { //&& (s.lastDelivered.subject.Seq.Seq == bI.subject.Seq.Seq-1){
		bI.timeout = s.managerDispatcher.TimerForManager(time.Duration(config.Config.EpochTimeoutNsec)*time.Nanosecond, s.requestTimeout)
	}

	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	if lastDelivered.subject.Seq.Seq == bI.subject.Seq.Seq-uint64(1) {
		s.deliverBatch(bI)
		//s.processAllBacklogs()
	} else {
		s.waitingToBeDelivered[bI.subject.Seq.Seq] = bI
	}
}

func (s *SBFT) requestTimeout() {
	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	log.Criticalf("replica %d: batch timed out after: %d", s.id, lastDelivered.subject.Seq.Seq)
	if s.primaryIDView(s.view+1) == s.id {
		leaderToBlame := s.getLeaderOfSequence(lastDelivered.subject.Seq.Seq+1, s.epochConfig[s.view])
		s.blacklistedLeaders[leaderToBlame] = 1
		log.Criticalf("replica %d: leader to blame: %d", s.id, leaderToBlame)
	}
	s.sendViewChange()
}
