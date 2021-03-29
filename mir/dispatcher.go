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
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	pb "github.com/IBM/mirbft/protos"
)

const adapterQueueSize uint64 = 6500000
const adapterPriorityQueueSize uint64 = 1300000

type MsgData struct {
	m   *pb.Msg
	src uint64
}

type ProcessedRequestData struct {
	bucket uint64
	size   uint64
}

type UnprocessedRequest struct {
	request *pb.Request
	digest  []byte
	bucket  uint64
}

type InterruptRequest struct {
	isViewChangeRequest bool
	data                uint64       //view for view change
	instance            *BFTInstance //the insance requesting the interrupt
}

type Backlog struct {
	log []*pb.Msg
}

type Dispatcher struct {
	sbft                          *SBFT
	runningInstances              map[uint64]*BFTInstance
	connectionId                  uint64
	numRequestHandlingDispatchers uint64 //TODO this is global for all blockHandlingDispatchers; should be external
	isRequestHandler              bool
	isManager                     bool
	backend                       System
	backLogs                      []Backlog
	queue                         chan Executable
	managerQueue                  chan Executable //this queue contains requests from the manager
}

func NewDispatcher(N int, connId uint64, b System, isBFTManager bool, isRequestHandler bool, numRequestHandlingDispatchers uint64) (*Dispatcher, error) {
	bA := &Dispatcher{sbft: nil, connectionId: connId, backend: b, isManager: isBFTManager, isRequestHandler: isRequestHandler, numRequestHandlingDispatchers: numRequestHandlingDispatchers}

	bA.runningInstances = make(map[uint64]*BFTInstance)
	bA.backLogs = make([]Backlog, N)

	bA.queue = make(chan Executable, adapterQueueSize)
	bA.managerQueue = make(chan Executable, adapterPriorityQueueSize)

	//if isBFTManager {
	//	go bA.runManager()
	//} else if isRequestHandler {
	//	go bA.runRequestProcessor()
	//}

	return bA, nil
}

func (b *Dispatcher) GetSBFT() *SBFT {
	return b.sbft
}

func (b *Dispatcher) Backend() System {
	return b.backend
}

func (b *Dispatcher) isInstanceHandler() bool {
	if b.isManager {
		return false
	}
	if b.isRequestHandler {
		return false
	}
	return true
}

//tests if it is a common case protocol message and if yes, returns its sequence number
func (b *Dispatcher) getMsgSeq(m *pb.Msg) (uint64, error) {
	if pp := m.GetPreprepare(); pp != nil {
		return pp.GetSeq().Seq, nil
	} else if p := m.GetPrepare(); p != nil {
		return p.GetSeq().Seq, nil
	} else if c := m.GetCommit(); c != nil {
		return c.GetSeq().Seq, nil
	} else if c := m.GetCheckpoint(); c != nil {
		return c.GetSeq(), nil
	}
	return 0, fmt.Errorf("wrong message type")
}

func (s *SBFT) isManagerMsg(m *pb.Msg) bool {
	if h := m.GetHello(); h != nil {
		return true
	} else if vs := m.GetViewChange(); vs != nil {
		return true
	} else if nv := m.GetNewView(); nv != nil {
		return true
	} else if ch := m.GetCheckpoint(); ch != nil {
		return true
	} else if nve := m.GetEpochCfgEcho(); nve != nil {
		return true
	} else if nva := m.GetEpochCfgAccept(); nva != nil {
		return true
	} else if ec := m.GetConfig(); ec != nil {
		return true
	}
	return false
}

func (b *Dispatcher) isMySequence(seq uint64) bool {
	if seq%b.numRequestHandlingDispatchers == b.connectionId {
		return true
	}
	return false
}

// the consensus instance should check this
//func (b *Dispatcher) alreadyProcessed(seq uint64) bool {
//	_, ok := b.
//
//	return false
//}

func (b *Dispatcher) RunRequestProcessor() {
	s := b.sbft
	s.nextProposalInfo = nil
	stopped := false
	for i := 0; i < runtime.NumCPU(); i++ {
		go b.handleRequest()
	}
	s.wg.Add(1)
	// TODO: This is still busy-waiting.
	//  Refactor to remove the last empty default statement in the loop.
	for {
		select {
		case to := <-s.batchTimeoutQueue:
			to.Execute(b)
		default:
		}
		if !s.isRunning() {
			if !stopped {
				s.nextProposalInfo = nil
				log.Debugf("Request processor saw stop")
				s.wg.Done()
				stopped = true
			}
			continue
		}

		if stopped {
			s.wg.Add(1) //TODO correct ???
			stopped = false
		}

		if s.nextProposalInfo == nil {
			select {
			case s.nextProposalInfo = <-b.sbft.nextProposalData:
				if s.nextProposalInfo.lastConfigChange > s.lastLeaderPendingSizeRecomputation {
					s.updateLeaderPendingSize()
					log.Critical("Updated leader pending size")
					s.lastLeaderPendingSizeRecomputation = s.nextProposalInfo.lastConfigChange
				}
				s.maybeSendNextBatch()
			default:
			}
		}

		select {
		case req := <-s.requestQueue:
			log.Infof("replica %d: got client request", b.sbft.id)
			s.processRequestAdditionNotification(req.bucket, req.size)
		case req := <-s.internalRequestQueue:
			s.processRequestAdditionNotification(req.bucket, req.size)
		default:
		}

	}
}

//func (b*Dispatcher) RunRequestProcessor() {
//	s:=b.sbft
//	var exclusiveMode bool
//	exclusiveMode = false
//	s.wg.Add(1)
//	for {
//		if exclusiveMode == false {
//
//			if !s.isRunning() {
//				exclusiveMode = true
//				s.wg.Done()
//				continue
//			}
//
//			select {
//			case req := <-s.requestQueue:
//				s.handleRequest(req, s.id, false)
//			default:
//			}
//			if len(b.sbft.batches) != 0 {
//				select {
//				case nextProposal := <-b.sbft.nextProposalData:
//					adapter := nextProposal.adapter
//					seq := nextProposal.seq
//					s.wg.Add(1)
//					batch := s.batches[0]
//					s.batches = s.batches[1:]
//					adapter.proposeBatch(batch, seq)
//				default:
//				}
//			}
//		} else {
//			//exclusiveMode = true
//
//			// if we are not currently processing requests (the leader set and assignments might not be clear)
//			// we do allow handleRequest, but -- requests are only added to the pending queues, no batches are created
//			// once we are allowed to resume -- and the mapping might have changed; while there are entries to dequeue from the nextProposalData channel -- and there are batches; send the next batch
//
//			select {
//			case req := <-s.requestQueue:
//				s.handleRequest(req, s.id, true)
//			default:
//			}
//
//			stop := atomic.LoadUint64(&s.stop)
//
//			if s.isRunning() {
//				s.wg.Add(1)
//				stop2 := atomic.LoadUint64(&s.stop)
//				if stop2 != stop {
//					s.wg.Done()
//					continue
//				}
//				exclusiveMode = false
//				s.updateLeaderPendingSize()
//
//				try := true
//
//				for try == true {
//					s.maybeSendNextBatch()
//					if len(b.sbft.batches) == 0 {
//						try = false
//						break
//					}
//
//					select {
//					case nextProposal := <-b.sbft.nextProposalData:
//						adapter := nextProposal.adapter
//						seq := nextProposal.seq
//						s.wg.Add(1)
//						batch := s.batches[0]
//						s.batches = s.batches[1:]
//						adapter.proposeBatch(batch, seq)
//					default:
//						try = false
//						break
//					}
//				}
//
//			}
//		}
//	}
//}

//func (b* Dispatcher) SpawnNewProposingInstance(batch []*Request) {
//	var batch *[]Request = nil
//
//	for () {
//		if batch != nil  {
//			nextPrpopsal:= <-b.sbft.nextProposalData
//			adapter:=nextPrpopsal.adapter
//			seq:=nextPrpopsal.seq
//			adapter.receiveBatchToPropose(batch, seq)
//			batch = nil
//			}
//		} else {
//			batch = <-b.sbft.batchQueue
//		}
//	}
//}

func (b *Dispatcher) RunManager() {
	s := b.sbft
	m := s.managerData
	// The repetition of the case statements in the default blocks and the nested
	// select statements implements a prioritization of certain channels over others.
	for {
		select {
		case to := <-s.managerTimeoutQueue:
			log.Errorf("Received timeout request")
			to.Execute(b)
		case interrupt := <-m.interruptQueue:
			log.Errorf("Received interrupt request")
			if interrupt.isViewChangeRequest {
				s.haltAllInstances()
				s.sendViewChange()
			}
		default:
			select {
			case to := <-s.managerTimeoutQueue:
				log.Errorf("Received timeout request")
				to.Execute(b)
			case interrupt := <-m.interruptQueue:
				log.Errorf("Received interrupt request")
				if interrupt.isViewChangeRequest {
					s.haltAllInstances()
					s.sendViewChange()
				}
			case mD := <-m.managerMsgQueue:
				log.Infof("Dequeueing %+v", mD.m.Type)
				s.Receive(mD.m, mD.src)
			default:
				select {
				case to := <-s.managerTimeoutQueue:
					log.Errorf("Received timeout request")
					to.Execute(b)
				case interrupt := <-m.interruptQueue:
					log.Errorf("Received interrupt request")
					if interrupt.isViewChangeRequest {
						s.haltAllInstances()
						s.sendViewChange()
					}
				case mD := <-m.managerMsgQueue:
					log.Infof("Dequeueing %+v", mD.m.Type)
					s.Receive(mD.m, mD.src)
				case bI := <-m.managerDeliverQueue:
					log.Infof("Dequeueing %d", bI.subject.Seq.Seq)
					s.maybeDeliverBatch(bI)
				}
			}
		}
	}
}

//func (b* Dispatcher) Timer(d time.Duration, f func()) Canceller {
//	tm := &Timer{tf: f, execute: true}
//	b.initTimer(tm, d)
//	return tm
//}
//
//
//func (b *Dispatcher) initTimer(t *Timer, d time.Duration) {
//	send := func() {
//		if t.execute {
//			b.queue <- t
//		}
//	}
//	time.AfterFunc(d, send)
//}

func (b *Dispatcher) TimerForManager(d time.Duration, f func()) Canceller {
	tm := &Timer{tf: f, execute: true}
	b.initManagerTimer(tm, d)
	return tm
}

func (b *Dispatcher) initManagerTimer(t *Timer, d time.Duration) {
	send := func() {
		if t.execute {
			b.sbft.managerTimeoutQueue <- t
		}
	}
	time.AfterFunc(d, send)
}

func (b *Dispatcher) TimerForRequestHandler(d time.Duration, f func()) Canceller {
	tm := &Timer{tf: f, execute: true}
	b.initBatchTimer(tm, d)
	return tm
}

func (b *Dispatcher) initBatchTimer(t *Timer, d time.Duration) {
	send := func() {
		if t.execute {
			b.sbft.batchTimeoutQueue <- t
		}
	}
	time.AfterFunc(d, send)
}

// Question: Does this do busy-waiting? If yes, is that intended?
// Suggestion to make it blocking, but keep the priority of manager queue:
// Select from managerQueue only, if that is empty, select from both.

//select {
//case p := <- b.managerQueue:
//	p.Execute(b)
//default:
//	select {
//	case p := <- b.managerQueue:
//		p.Execute(b)
//	case e := <- b.queue:
//		e.Execute(b)
//	}
//}

func (b *Dispatcher) Run() {
	if b.isInstanceHandler() {
		b.sbft.wg.Add(1)
	}

	for {
		select {
		case p := <-b.managerQueue:
			p.Execute(b)
		default:
			select {
			case p := <-b.managerQueue:
				p.Execute(b)
			case e := <-b.queue:
				e.Execute(b)
			}
		}
	}
}

func (b *Dispatcher) GetChainId() string {
	return b.sbft.chainId
}

func (b *Dispatcher) Connection(replica uint64) {
	//go func() {
	b.queue <- &connectionEvent{peerid: replica}
	//}()
}

func (b *Dispatcher) handleConnection(replica uint64) {
	b.sbft.Connection(replica) //TODO do I need to change anything here?
}

func (b *Dispatcher) Request(m *pb.Msg, src uint64) (bool, uint64, error) {
	//go func() {
	//	b.queue <- &requestEvent{msg: m, src: src}
	//}()
	//go func() {

	if !b.isRequestHandler {
		log.Errorf("Received request on wrong connection")
		return false, 0, fmt.Errorf("Received request on wrong connection")
	}

	req := m.GetRequest()

	if req == nil {
		log.Errorf("Wrong messge type -- expecting request")
		return false, 0, fmt.Errorf("Wrong message type -- expeting request")
	}

	//TODO make bucket to leader a slice; do atomic reads and writes to it
	s := b.sbft
	digest := requestHash(req.Payload, req.Seq, req.PubKey)
	bucket := s.getBucket(digest)
	s.bucketToLeaderLock.RLock()
	target := s.bucketToLeader[bucket]
	s.bucketToLeaderLock.RUnlock()

	b.sbft.unprocessedRequestQueue <- &UnprocessedRequest{req, digest, bucket}

	if target == s.id {
		return true, target, nil
	} else {
		return false, target, nil
	}

	//b.handleRequest(m)
	//}()
}

func (b *Dispatcher) handleRequest() {

	for {
		uReq := <-b.sbft.unprocessedRequestQueue
		bucket, size, err := b.sbft.processNewRequest(uReq.request, uReq.digest, uReq.bucket)
		if err != nil {
			continue
		}
		//go func() {
		b.sbft.requestQueue <- ProcessedRequestData{bucket, size}
		//}()
	}
}

func (b *Dispatcher) Receive(m *pb.Msg, src uint64) {
	//go func() {
	b.queue <- &msgEvent{msg: m, src: src}
	//}()
}

func (b *Dispatcher) handleMessage(m *pb.Msg, src uint64) {
	//assume this method is not called concurrently
	s := b.sbft
	if b.isManager && s.isManagerMsg(m) {
		s.managerData.managerMsgQueue <- &MsgData{m, src}
		return
	}

	log.Debugf("replica %d: received message from %d", s.id, src)

	// Question: Is this the sequence number of the message in the log (proposed by the leader)?
	seq, err := b.getMsgSeq(m)

	if err != nil {
		log.Errorf("Wrong message type received")
		return
	}

	if !b.isMySequence(seq) {
		log.Errorf("Message sent to wrong connection")
	}

	// Question: Are we also checking for the high watermark somewhere? Is this just an optimization to kick out old messages early?
	if seq < atomic.LoadUint64(&b.sbft.lowWatermark) {
		//message for old sequence, can drop
		return
	}

	if b.testBacklogMessage(m, src) {
		log.Infof("Adding message for sequence %d to backlog", seq)
		b.recordBacklogMsg(m, src)
		return
	}

	// If this is a preprepare message, start a new "one-shot" PBFT instance for it.
	if pp := m.GetPreprepare(); pp != nil {

		// Check if view view number matches
		if pp.Seq.View < s.getView() {
			log.Errorf("Received preprepare for old view")
			return
		}
		_, ok := b.runningInstances[seq]
		if ok {
			log.Criticalf("Instance with sequence %d already running -- duplicate preprepare", pp.Seq.Seq)
			return
		}
		instance := newBFTInstance(s, b, b.backend, s.managerData.managerDeliverQueue, nil)
		log.Infof("adding running instance with sequence %d", seq)
		b.runningInstances[seq] = instance
		go instance.run(pp, src, false)
		if b.sbft.isRunning() {
			b.processBacklog()
		}
		return
		// If this is not a preprepare message, pass it on to the corresponding BFT instance
	} else {
		i, ok := b.runningInstances[seq]
		if !ok {
			panic(fmt.Sprintf("Replica %d: trying to enqueue message for instance that is not yet running", b.sbft.id))
			return

		}
		i.msgChan <- &MsgData{m, src}
	}
}

func (b *Dispatcher) handleStop(stop uint64) {
	log.Debugf("handleStop")
	b.sbft.wg.Done()
}

func (b *Dispatcher) backlogRequest(process bool, src uint64) {
	//go func() {
	b.managerQueue <- &backlogEvent{process: process, src: src}
	//}()
}

func (b *Dispatcher) handleBacklogRequest(process bool, src uint64) {
	if process {
		b.processBacklog()
	} else { //discard
		b.discardBacklog(src)
	}
}

func (b *Dispatcher) batchCompleted(seq uint64) {
	b.managerQueue <- &completedEvent{seq: seq}
}

//this function should be called when a checkpoint has advanced the low watermark
func (b *Dispatcher) handleCompleted(seq uint64) {
	for s := range b.runningInstances {
		if s < seq {
			delete(b.runningInstances, s)
			log.Infof("deleting running instance with sequence %d", s)
		}
	}
	//_, ok := b.runningInstances[seq]
	//if !ok {
	//	log.Errorf("Attempting to remove instance with sequence %d not currently running", seq)
	//	return
	//}
	//delete(b.runningInstances, seq)
}

func (b *Dispatcher) proposeBatch(batch []*pb.Request, seq pb.SeqView) {
	log.Infof("Proposing batch with seq %d", seq.Seq)
	//go func() {
	b.queue <- &batchEvent{batch: batch, seq: seq}
	//}()
}

func (b *Dispatcher) handleNewBatchToPropose(batch []*pb.Request, seq pb.SeqView) {
	//no need to do the checks in receive, as I am the one sending this request to myself
	s := b.sbft
	_, ok := b.runningInstances[seq.Seq]
	if ok {
		panic("Instance already present when trying to propose")
	}
	instance := newBFTInstance(s, b, b.backend, s.managerData.managerDeliverQueue, nil)
	log.Infof("adding my running instance with sequence %d", seq.Seq)
	b.runningInstances[seq.Seq] = instance
	//TODO maybe check that I don't already have a running instance for seq.Seq; if that's the case, I have a problem
	go instance.sendPreprepareAndRun(batch, seq)
	b.processBacklog()
}
