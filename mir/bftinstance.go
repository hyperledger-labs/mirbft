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

type BFTInstance struct {
	sbft         *SBFT
	epoch        *epochConfig
	backend      System
	dispatcher   *Dispatcher
	hasHalted    bool //true if this instance has encountered an issue and has requested a view change
	batch        *batchInfo
	msgChan      chan *MsgData //this channel should have capacity max(no. of requests in block, 2*n)
	deliveryChan chan *batchInfo
	haltChan     chan uint64
}

func newBFTInstance(s *SBFT, d *Dispatcher, b System, dc chan *batchInfo, hc chan uint64) *BFTInstance {
	nI := &BFTInstance{sbft: s, backend: b, dispatcher: d, deliveryChan: dc, haltChan: hc}

	nI.hasHalted = false
	nI.batch = nil //we don't yet have the batchInfo when the instance is started
	nI.msgChan = make(chan *MsgData, uint64(config.Config.N)*3+1)
	return nI
}

func (instance *BFTInstance) forwardForDelivery(batch *batchInfo) {
	instance.sbft.managerData.managerDeliverQueue <- batch
}

func (instance *BFTInstance) handleMessage(qm *MsgData) {
	src := qm.src
	m := qm.m
	//a running BFT instance should only handle prepare, commit and request messages; preprepares trigger the initialization of the thread
	//the preprepare is provided when the thread is started (or is generated when requests are received)
	//chekcpoints should now be handled by the BFT manager
	//new views etc. should also be handled by the BFT manager

	if p := m.GetPrepare(); p != nil {
		instance.handlePrepare(p, src)
		return
	} else if c := m.GetCommit(); c != nil {
		instance.handleCommit(c, src)
		return
	}

	log.Warningf("replica %d: received invalid message from %d", instance.sbft.id, src)
}

//func (instance *BFTInstance) checkForStop() {
//	stop:=atomic.LoadUint64(&instance.sbft.stop)
//
//	if stop % 4 == 0 {
//		return
//	}
//
//	instance.sbft.wg.Done()
//
//	for stop % 4 !=0 {
//		time.Sleep(time.Duration(instance_sleep_microseconds) * time.Microsecond)
//		stop=atomic.LoadUint64(&instance.sbft.stop)
//	}
//
//	instance.sbft.wg.Add(1)
//}

func (instance *BFTInstance) sendPreprepareAndRun(batch []*pb.Request, seq pb.SeqView) {

	s := instance.sbft
	//s.wg.Add(1)
	//instance.checkForStop()

	instance.epoch = s.getCurrentEpochConfig()
	instance.sendPreprepare(batch, seq)

	if instance.batch == nil {
		//at this point a batchInfo should have been created; it means the sendPreprepare found an issue
		log.Errorf("Abandoning instance for sequence %d", seq.Seq)
		for _, req := range batch {
			bucket, size, err := s.returnRequestToPending(req)
			if err == nil {
				s.internalRequestQueue <- ProcessedRequestData{bucket, size}
			}
		}
		s.wg.Done()
		instance.dispatcher.managerQueue <- &completedEvent{seq: seq.Seq}
		return
	}

	instance.run(instance.batch.preprep, instance.sbft.id, true)
}

func (s *SBFT) checkStopAndAdd() bool {
	st := atomic.LoadUint64(&s.stop)

	if st%4 != 3 {
		s.wg.Add(1)
		st2 := atomic.LoadUint64(&s.stop)
		if st != st2 {
			s.wg.Done()
			return false
		}
		return true
	}

	return false
}

func (instance *BFTInstance) requestViewChange() {
	log.Error("TimerForRequestHandler")
	instance.hasHalted = true
	instance.msgChan <- nil // This allows message processing goroutine (probably blocked on this channel) to continue and return.
	instance.sbft.managerData.interruptQueue <- &InterruptRequest{true, instance.sbft.view, instance}
}

func (instance *BFTInstance) run(preprepare *pb.Preprepare, src uint64, alreadyStarted bool) {

	s := instance.sbft
	if !alreadyStarted { //it was added when the batch was cut
		if !s.checkStopAndAdd() {
			instance.dispatcher.managerQueue <- &completedEvent{seq: preprepare.Seq.Seq}
			return
		}
		instance.epoch = s.getCurrentEpochConfig()
		instance.handlePreprepare(preprepare, src)
	} else {
		if s.instancesHalted() {
			log.Errorf("Halting instance for sequence %d", preprepare.Seq.Seq)

			last := len(preprepare.Batch.Payloads) - 1
			if preprepare.Batch.IsConfigBatch() {
				last--
			}
			for i, payload := range preprepare.Batch.Payloads {
				if i > last {
					break
				}
				req := &pb.Request{
					Seq:       preprepare.Batch.SeqNos[i],
					Payload:   payload,
					Signature: preprepare.Batch.PayloadSigs[i],
					PubKey:    preprepare.Batch.Pub[i],
				}
				bucket, size, err := s.returnRequestToPending(req)
				if err == nil {
					s.internalRequestQueue <- ProcessedRequestData{bucket, size}
				}
			}

			s.wg.Done()
			instance.dispatcher.managerQueue <- &completedEvent{seq: preprepare.Seq.Seq}
			return
		}
	}

	if instance.batch == nil {
		//at this point a batchInfo should have been created; it means the handlePreprepare found an issue
		log.Errorf("Don't have a batchInfo for current -- returning")
		s.wg.Done()
		instance.dispatcher.managerQueue <- &completedEvent{seq: preprepare.Seq.Seq}
		return
	}

	log.Infof("Instance: processing batch with sequence %d", preprepare.Seq.Seq)

	// Process incoming messages until the batch is committed or the system halts
	for instance.batch.committed == false {
		if instance.hasHalted {
			s.wg.Done()
			instance.dispatcher.managerQueue <- &completedEvent{seq: preprepare.Seq.Seq}
			return
		}
		if s.instancesHalted() {
			s.wg.Done()
			instance.dispatcher.managerQueue <- &completedEvent{seq: preprepare.Seq.Seq}
			return
		}

		// The select has no empty default block. This is to avoid busy-waiting.
		// The channel must be closed or sent a nil value when:
		// - The batch is committed
		// - The instance halts
		// - The system halts
		// TODO: Implement the last condition.
		//       So far we send a nil value on the channel when this instance is done.
		//       Closing the channel results in an error, as some thread is still sending values.
		//       Find out what happens.
		select {
		case m := <-instance.msgChan:
			if m != nil {
				instance.handleMessage(m)
			}
		}
		//select {
		//	case _ := <-instance.haltChan:
		//		s.wg.Done()
		//		instance.dispatcher.managerQueue <- &completedEvent{seq:preprepare.Seq.Seq}
		//		return
		//	default:
		//		select {
		//			case m:=<-instance.msgChan:
		//				instance.handleMessage(m)
		//			default:
		//				//no message received
		//		}
		//}

	}

	log.Infof("Instance committed batch with seq %d, returning", preprepare.Seq.Seq)

	s.wg.Done()

}
