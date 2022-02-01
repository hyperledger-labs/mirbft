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
	"bytes"
	"fmt"
	"reflect"

	pb "github.com/IBM/mirbft/protos"
)

func (s *SBFT) maybeSendNewEpoch() {
	log.Debugf("replica %d: maybeSendNewEpoch", s.id)
	if s.lastNewViewSent != nil && s.lastNewViewSent.View == s.view {
		return
	}

	vset := make(map[uint64]*pb.Signed)
	var vcs []*pb.ViewChange

	for src, state := range s.replicaState {
		if state.viewchange != nil && state.viewchange.View == s.view {
			vset[uint64(src)] = state.signedViewchange
			vcs = append(vcs, state.viewchange)
		}
	}

	//if len(vcs) < s.viewChangeQuorum() {
	//	log.Warningf("replica %d: not enough view change messages: %d", s.id, len(vcs))
	//	return
	//}

	s.haltAllInstances()
	xset, checkpoint, ok := s.makeXset(vcs)
	if !ok {
		log.Debugf("replica %d: xset not yet sufficient", s.id)
		return
	}

	if !reflect.DeepEqual(s.lastCheckpoint.subject.Digest, checkpoint.DecodeHeader().DataHash) {
		log.Criticalf("replica %d: forfeiting primary - do not have latest checkpoint %d", s.id, checkpoint.DecodeHeader().Seq)
		return // TODO must get state
	}

	nv := &pb.NewView{
		View:       s.view,
		Vset:       vset,
		Checkpoint: checkpoint,
	}

	var batches []*pb.Batch
	var err error
	if xset == nil {
		log.Errorf("replica %d: no batch has been committed above the last stable checkpoint", s.id)
		nv.Config, err = s.makeEpochConfig(s.view, false, nv.Checkpoint.DecodeHeader().Seq+1)
		if err != nil {
			log.Criticalf("replica %d: forfeiting primary - %s", s.id, err.Error())
		}
	} else {
		batches = make([]*pb.Batch, 0, 0)
		for _, entry := range xset {
			if batch, ok := s.cur[entry.Seq.Seq]; ok { //TODO: add view number in cur key?
				if batch.preprep != nil {
					if reflect.DeepEqual(batch.subject.Digest, entry.Digest) {
						log.Debugf("%+v", batch)
						batches = append(batches, s.MakeBatch(entry.Seq.Seq, batch.preprep.Batch.SeqNos, batch.preprep.Batch.Payloads, batch.preprep.Batch.PayloadSigs, batch.preprep.Batch.Pub, 0))
					} else {
						log.Criticalf("replica %d: forfeiting primary - do not have request in store for %d %x", s.id, entry.Seq.Seq, entry.Digest)
						return // TODO must get state
					}
				} else {
					log.Criticalf("replica %d: forfeiting primary - do not have latest batch payloads %d", s.id, entry.Seq.Seq)
					return // TODO must get state
				}
			} else {
				log.Criticalf("replica %d: forfeiting primary - do not have latest batch %d", s.id, entry.Seq.Seq)
				return // TODO must get state
			}
		}
		if len(xset) == 0 {
			nv.Config, err = s.makeEpochConfig(s.view, false, nv.Checkpoint.DecodeHeader().Seq+1)
			if err != nil {
				log.Errorf("replica %d: forfeiting primary - %s", s.id, err.Error())
			}
		} else {
			nv.Config, err = s.makeEpochConfig(s.view, false, xset[len(xset)-1].Seq.Seq+1)
			if err != nil {
				log.Errorf("replica %d: forfeiting primary - %s", s.id, err.Error())
			}
		}
	}
	nv.Xset = batches

	log.Errorf("replica %d: sending new view for %d", s.id, nv.View)
	s.lastNewViewSent = nv
	s.broadcast(&pb.Msg{Type: &pb.Msg_NewView{nv}})
}

func (s *SBFT) checkNewEpochSignatures(nv *pb.NewView) ([]*pb.ViewChange, error) {
	var vcs []*pb.ViewChange
	for vcsrc, svc := range nv.Vset {
		vc := &pb.ViewChange{}
		err := s.checkSig(svc, vcsrc, vc)
		if err == nil {
			//TODO Should we checkBatch???
			if vc.View != nv.View {
				err = fmt.Errorf("view does not match")
			}
		}
		if err != nil {
			return nil, fmt.Errorf("viewchange from %d: %s", vcsrc, err)
		}
		vcs = append(vcs, vc)
	}

	return vcs, nil
}

func (s *SBFT) handleNewEpoch(nv *pb.NewView, src uint64) {

	log.Errorf("replica %d: handling new view", s.id)
	if nv == nil {
		log.Debugf("replica %d: empty new view", s.id)
		return
	}

	if nv.View < s.view {
		log.Debugf("replica %d: discarding old new view from %d for %d, we are in %d", s.id, src, nv.View, s.view)
		return
	}

	if nv.View == s.view && s.activeView {
		log.Debugf("replica %d: discarding new view from %d for %d, we are already active in %d", s.id, src, nv.View, s.view)
		return
	}

	if src != s.primaryIDView(nv.View) {
		log.Warningf("replica %d: invalid new view from %d for %d, src should be %d", s.id, src, nv.View, s.primaryIDView(nv.View))
		return
	}

	vcs, err := s.checkNewEpochSignatures(nv)
	if err != nil {
		log.Errorf("replica %d: invalid new view from %d: %s", s.id, src, err)
		s.sendViewChange()
		return
	}

	s.haltAllInstances()
	xset, checkpoint, ok := s.makeXset(vcs)

	if !ok {
		log.Errorf("replica %d: invalid new view from %d: %s", s.id, src, err)
		s.sendViewChange()
		return
	}

	if nv.Checkpoint == nil {
		log.Errorf("replica %d: invalid new view from %d: missing checkpoint", s.id, src)
		s.sendViewChange()
		return
	}

	if !bytes.Equal(nv.Checkpoint.Hash(), checkpoint.Hash()) {
		log.Errorf("replica %d: invalid new view from %d: highest checkpoint doesn't match expected: %v, %v",
			s.id, src, nv.Checkpoint, checkpoint, nv)
		s.sendViewChange()
		return
	}

	if nv.Xset == nil {
		if xset != nil {
			log.Errorf("replica %d: invalid new view from %d: xset shouldn't be empty", s.id, src)
			s.sendViewChange()
			return
		}
	} else {
		for i, entry := range xset {
			digest, _ := makeBatchHash(nv.Xset[i].SeqNos, nv.Xset[i].Payloads, nv.Xset[i].Pub)
			log.Debugf("%x, %x", digest, entry.Digest)
			if !bytes.Equal(digest, entry.Digest) {
				log.Errorf("replica %d: invalid new view from %d: xset entry for %d does not match expected", s.id, src, entry.Seq.Seq)
				s.sendViewChange()
				return
			}
			_, err = s.checkBatch(&pb.SeqView{View: nv.View, Seq: nv.Xset[i].DecodeHeader().Seq}, nv.Xset[i], true, false)
			if err != nil {
				log.Errorf("replica %d: invalid new view from %d: invalid batches, %s",
					s.id, src, err)
				s.sendViewChange()
				return
			}
		}
	}

	log.Debugf("replica %d: handling new view -- maybe sending cfg echo and cfg echo accept", s.id)
	s.sendEpochCfgEcho(nv, src)
	s.maybeSendEpochCfgAccept(nv.Config.Epoch)
}

func (s *SBFT) applyNewView(nv *pb.NewView, src uint64) {
	log.Errorf("replica %d applying new view", s.id)
	addToBlacklist := true

	oldconfig, ok := s.epochConfig[nv.Config.Epoch-1]

	lastDelivered := s.lastDelivered.Load().(*batchInfo)
	if !ok || len(oldconfig.leaders) == 0 || lastDelivered.subject.Seq.Seq == oldconfig.last {
		//means change was due to no epoch config being present ???
		addToBlacklist = false
	}
	s.view = nv.View

	s.cancelViewChangeTimer()
	s.discardAllBacklogs(s.primaryID())
	//s.discardBacklog(s.primaryID())

	// maybe fetch state
	if s.lastCheckpoint.subject.Seq.Seq < nv.Checkpoint.DecodeHeader().Seq {
		//TODO fetch state
	}

	if nv.Xset != nil {
		log.Errorf("replica %d: %d batches in new-view to process", s.id, len(nv.Xset))
		for _, batch := range nv.Xset {
			batchHeader := batch.DecodeHeader()
			lastDelivered := s.lastDelivered.Load().(*batchInfo)
			// if the last batch delivered has a smaller sequence number
			if lastDelivered.subject.Seq.Seq < batchHeader.Seq {
				// if the we already preprepared a batch for this sequence number but haven't delivered
				localBatch, ok := s.cur[batchHeader.Seq]
				if ok && localBatch.preprep != nil {
					// if the the batch we have locally does not match the one in the Xset
					if !reflect.DeepEqual(batchHeader.DataHash, localBatch.preprep.Batch.DecodeHeader().DataHash) {
						// remove the old entry for this seq no
						delete(s.cur, batchHeader.Seq)
					}
				}

				_, ok = s.cur[batchHeader.Seq]
				if !ok || !s.cur[batchHeader.Seq].prepared {
					log.Errorf("batch %d not prepared", batchHeader.Seq)
					seq := batchHeader.Seq
					pp := &pb.Preprepare{
						Seq:   &pb.SeqView{Seq: seq, View: s.view},
						Batch: batch,
					}
					requestHashes := make([][]byte, len(pp.Batch.Payloads))
					for i, payload := range pp.Batch.Payloads {
						requestHashes[i] = requestHash(payload, pp.Batch.SeqNos[i], pp.Batch.Pub[i])
					}
					s.createPreprepareEntry(pp, batchHeader, requestHashes)

					batch := s.cur[batchHeader.Seq].preprep.Batch
					//instance.sbft.prepreparedLock.Lock()
					for i, _ := range batch.Payloads {
						digest := requestHashes[i]
						bucket := s.getBucket(digest)
						s.bucketLocks[bucket].Lock()
						s.preprepared[bucket][hash2str(digest)] = &clientRequest{client: hash2str(batch.Pub[i]), seq: batch.SeqNos[i]}
						s.bucketLocks[bucket].Unlock()
					}
				}

				s.cur[batchHeader.Seq].prepared = true
				//d:=s.getAdapter(seq)
				// we should also deliver this batch

				if !s.cur[batchHeader.Seq].committed {
					for i, payload := range s.cur[batchHeader.Seq].preprep.Batch.Payloads {
						digest := s.cur[batchHeader.Seq].requestHashes[i]
						key := hash2str(digest)
						bucket := s.getBucket(digest)
						s.bucketLocks[bucket].Lock()
						if s.deleteFromPending(bucket, key) {
							if s.bucketToLeader[bucket] == s.id {
								s.subtractFromLeaderPendingSize(int64(len(payload)))
							}

						}
						s.bucketLocks[bucket].Unlock()
					}
					log.Errorf("replica %d: Removed requests from pending at new view for sequence %d ", s.id, batchHeader.Seq)
				}
				s.cur[batchHeader.Seq].committed = true

				log.Errorf("New view deliver batch %d", batchHeader.Seq)
				s.maybeDeliverBatch(s.cur[batchHeader.Seq])

			} else {
				log.Infof("replica %d: Batches with greater than or equal sequence number have been delivered", s.id)
			}
		}
	} else {
		// the xset is empty
		log.Infof("replica %d: the xset is empty", s.id)
	}

	// after a new-view message, prepare to accept new requests.
	s.processLog()

	if addToBlacklist {

		log.Errorf("replica %d: computing node to add to blacklist; current epoch: %d, old epoch %d, size of leaders in old epoch %d", s.id, nv.Config.Epoch, nv.Config.Epoch-1, len(s.epochConfig[nv.Config.Epoch-1].leaders))
		//blacklist the leader that caused the view change
		//firstInNewEpoch := s.epochConfig[nv.Config.Epoch].first
		//leaderToBlame := s.getLeaderOfSequence(firstInNewEpoch, s.epochConfig[s.oldview]) //TODO actually, I might want to do s.epochConfig[nv.Config.Epoch - 1] (think of switching to a faulty primary for instance)
		//leaderToBlame := s.getLeaderOfSequence(firstInNewEpoch, s.epochConfig[nv.Config.Epoch-1])
		//log.Errorf("replica %d: new view; first in new epoch is %d - I blame %d for the view change; currently s.view is %d, nv.Config.Epoch is %d", s.id, firstInNewEpoch, leaderToBlame, s.view, nv.Config.Epoch)

		//s.blacklistedLeaders[leaderToBlame] = 1
	} else {
		log.Errorf("replica %d: new view %d -- not adding to blacklist", s.id, nv.Config.Epoch)
	}

	// apply the new epoch configuration
	s.epochConfig[nv.Config.Epoch].valid = true // it is already validated
	s.applyNewEpochConfig(s.epochConfig[nv.Config.Epoch])
	log.Debugf("active view is true")
	log.Errorf("replica %d now active in view %d; primary: %v", s.id, s.view, s.isPrimary())

	s.activeView = true

	s.resumeAllInstances()
	s.maybeAllowMoreProposingInstances()
	s.processAllBacklogs()
}

func (s *SBFT) processLog() {
	lastDelivered := s.lastDelivered.Load().(*batchInfo).subject.Seq.Seq
	for key, batch := range s.cur {
		batch.maybeCancelTimer()
		//if batch.preprep != nil && !batch.committed {
		if batch.preprep != nil && batch.preprep.Seq.Seq > lastDelivered {
			log.Errorf("replica %d: batch %d (key %d) was never delivered, we should move its requests back to pending", s.id, batch.subject.Seq.Seq, key)
			// If the batch included a configuration request either for epoch configuration
			// or for bucket rotation configuration, this will be the last request and we should
			// discard it.
			last := len(batch.preprep.Batch.Payloads) - 1
			if batch.preprep.Batch.IsConfigBatch() {
				last--
			}
			for i, payload := range batch.preprep.Batch.Payloads {
				if i > last {
					break
				}
				req := &pb.Request{
					Seq:       batch.preprep.Batch.SeqNos[i],
					Payload:   payload,
					Signature: batch.preprep.Batch.PayloadSigs[i],
					PubKey:    batch.preprep.Batch.Pub[i],
				}
				bucket, size, err := s.returnRequestToPending(req)
				if err == nil {
					s.internalRequestQueue <- ProcessedRequestData{bucket, size}
				}
			}

			log.Errorf("replica %d: deleting batch %d from cur and waiting to be delivered", s.id, batch.subject.Seq.Seq)
			delete(s.waitingToBeDelivered, batch.subject.Seq.Seq)
			delete(s.cur, batch.subject.Seq.Seq)
			delete(s.getAdapter(batch.subject.Seq.Seq).runningInstances, batch.subject.Seq.Seq)
		}
	}
	for _, batch := range s.batches {
		log.Errorf("replica %d: batch was never proposed, we should move its requests back to pending", s.id)
		for _, req := range batch {
			bucket, size, err := s.returnRequestToPending(req)
			if err == nil {
				s.internalRequestQueue <- ProcessedRequestData{bucket, size}
			}
		}
	}
	for _, d := range s.blockHandlingDispatchers {
		for e, _ := range d.runningInstances {
			delete(d.runningInstances, e) //TODO check if this is truly necessary
		}
	}
}
