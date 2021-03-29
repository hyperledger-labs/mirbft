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
	"sync/atomic"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
)

const msgPerSeq = 3000 // (pre)prepare, commit, checkpoint

func (dispatcher *Dispatcher) testBacklogMessage(m *pb.Msg, src uint64) bool { //TODO update this method
	s := dispatcher.sbft

	var crtEpoch *epochConfig = (*epochConfig)(atomic.LoadPointer(&s.currentEpochConfig))

	crtView := crtEpoch.epoch

	test := func(seq *pb.SeqView) bool {
		if !s.isRunning() {
			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own manager-related message (replica ID: %d) due to service not running", src)
			}

			return true
		}
		//view := atomic.LoadUint64(&s.view)
		if (seq.Seq > atomic.LoadUint64(&s.lastCheckpoint.subject.Seq.Seq)) || seq.View > crtView {
			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own manager-related message (replica ID: %d) -- message for future sequences", src)
			}

			return true
		}
		return false
	}

	testPrepareAndCommit := func(seq *pb.SeqView) bool {
		if !s.isRunning() {

			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own perpare or commit message (replica ID: %d) due to service not running", src)
			}

			return true
		}
		//view := atomic.LoadUint64(&s.view)
		if (seq.Seq > atomic.LoadUint64(&s.highWatermark)) || seq.View > crtView {

			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own perpare or commit message (replica ID: %d) -- message for future sequences", src)
			}
			return true
		}

		_, ok := dispatcher.runningInstances[seq.Seq]
		if !ok {

			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own perpare or commit message (replica ID: %d) -- no running instance for this message", src)
			}
			log.Infof("no running instance with sequence %d, adding to backlog", seq.Seq)
			return true
		}
		return false
	}

	testPreprepare := func(seq *pb.SeqView) bool {
		if !s.isRunning() {

			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own preprepare message (replica ID: %d) due to service not running", src)
			}

			return true
		}
		//view := atomic.LoadUint64(&s.view)
		if !s.canExecuteInstance(seq.Seq, crtEpoch) || seq.View > crtView {

			if src == dispatcher.sbft.id {
				log.Errorf("I have backlogged my own preprepare message (replica ID: %d) -- message for future sequences", src)
			}
			return true
		}
		return false
	}

	if pp := m.GetPreprepare(); pp != nil {
		return testPreprepare(pp.Seq)
	} else if p := m.GetPrepare(); p != nil {
		return testPrepareAndCommit(p.Seq)
	} else if c := m.GetCommit(); c != nil {
		return testPrepareAndCommit(c.Seq)
	} else if chk := m.GetCheckpoint(); chk != nil {
		return test(&pb.SeqView{Seq: chk.Seq}) // TODO should we discard future checkpoints?
	}
	return false
}

func (adapter *Dispatcher) recordBacklogMsg(m *pb.Msg, src uint64) {

	adapter.backLogs[src].log = append(adapter.backLogs[src].log, m)
	log.Debugf("replica %d: added message from %d: %s", adapter.sbft.id, src, m)

	if len(adapter.backLogs[src].log) > config.Config.WatermarkDist*msgPerSeq*10 {
		log.Criticalf("replica %d: backlog for %d full, discarding and reconnecting", adapter.sbft.id, src)
		adapter.discardBacklog(src)
		//TODO request interrupt instead
		adapter.backend.Reconnect(adapter.sbft.chainId, src)
	} else {
	}

}

func (adapter *Dispatcher) discardBacklog(src uint64) {
	adapter.backLogs[src].log = nil
}

func (s *SBFT) discardAllBacklogs(src uint64) {
	for _, a := range s.blockHandlingDispatchers {
		a.backlogRequest(false, src)
	}
}

func (adapter *Dispatcher) processBacklog() {
	for src := range adapter.backLogs {
		srcu := uint64(src)
		log.Debugf("replica %d: processing backlog for source %d", adapter.sbft.id, src)
		bL := &adapter.backLogs[src]
		j := 0
		for i := 0; i < len(bL.log); i++ {
			m := bL.log[i]
			if m != nil && !adapter.testBacklogMessage(m, srcu) {
				log.Debugf("replica %d: processing stored message from %d: %s", adapter.sbft.id, srcu, m)
				bL.log[i] = nil
				//adapter.handleMessage(m, srcu)
				adapter.Receive(m, srcu)
			} else {
				if m != nil {
					bL.log[j] = m
					j++
				}
			}
		}
		adapter.backLogs[src].log = bL.log[:j]
	}
	log.Debugf("replica %d: finished processing backlog", adapter.sbft.id)
}

//should only be called by manager thread
func (s *SBFT) processAllBacklogs() {
	for _, a := range s.blockHandlingDispatchers {
		a.backlogRequest(true, 0)
	}
	s.managerDispatcher.processBacklog()

}
