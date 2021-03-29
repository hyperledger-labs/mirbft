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

func (instance *BFTInstance) sendPrepare(seq uint64) {
	batch := instance.batch.preprep.Batch
	requestHashes := instance.batch.requestHashes
	//instance.sbft.prepreparedLock.Lock()
	for i, _ := range batch.Payloads {
		digest := requestHashes[i]
		bucket := instance.sbft.getBucket(digest)
		instance.sbft.bucketLocks[bucket].Lock()
		instance.sbft.preprepared[bucket][hash2str(digest)] = &clientRequest{client: hash2str(batch.Pub[i]), seq: batch.SeqNos[i]}
		instance.sbft.bucketLocks[bucket].Unlock()
	}

	//instance.sbft.prepreparedLock.Unlock()
	p := instance.batch.subject
	instance.sbft.broadcastSystem(&pb.Msg{Type: &pb.Msg_Prepare{p}}, instance.backend)
}

func (instance *BFTInstance) handlePrepare(p *pb.Subject, src uint64) {
	log.Infof("replica %d: handling prepare for sequence %d from %d", instance.sbft.id, p.Seq.Seq, src)
	batch := instance.batch
	if !proto.Equal(p, batch.subject) {
		log.Errorf("replica %d: prepare does not match expected subject %v, got %v", instance.sbft.id, batch.subject, p)
		return
	}
	if _, ok := batch.prep[src]; ok {
		log.Errorf("replica %d: duplicate prepare for %v from %d", instance.sbft.id, p.Seq, src)
		return
	}
	batch.prep[src] = p
	if instance.sbft.verifies(src, p.Digest) {
		batch.verifiers = append(batch.verifiers, src)
	}
	instance.maybeSendCommit()
}
