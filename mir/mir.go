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
	"math"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/IBM/mirbft/config"
	"github.com/IBM/mirbft/connection"
	pb "github.com/IBM/mirbft/protos"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
)

const preprepared string = "preprepared"
const prepared string = "prepared"
const committed string = "committed"
const viewchange string = "viewchange"
const epochconfig string = "config"

//const batchQueueSize uint64 = 128
const requestQueueSize uint64 = 65536 //TODO should there be a correlation between the sizes of these two queues?
const timeoutQueueSize uint64 = 4000
const numeRequestHandlingGoroutines int = 32
const requestPerBatch = 4000 //TODO move to config file

// Receiver defines the API that is exposed by SBFT to the system.
type Receiver interface {
	Receive(msg *pb.Msg, src uint64)
	Request(msg *pb.Msg, src uint64) (bool, uint64, error)
	Connection(replica uint64)
	GetChainId() string
	Run()
}

// System defines the API that needs to be provided for SBFT.
type System interface {
	Send(chainId string, msg *pb.Msg, dest uint64)
	Deliver(chainId string, batch *pb.Batch)
	AddReceiver(chainId string, receiver Receiver)
	Persist(chainId string, key string, data proto.Message)
	Restore(chainId string, key string, out proto.Message) bool
	DeleteAll()
	LastBatch(chainId string) *pb.Batch
	Sign(data []byte) []byte
	CheckSig(data []byte, src uint64, sig []byte) error
	Reconnect(chainId string, replica uint64)
	Validate(chainID string, req *pb.Request) ([][]*pb.Request, bool)
	Cut(chainID string) []*pb.Request
	GetPeerInfo(src uint64) *connection.PeerInfo
	Response(id string, response *pb.ResponseMessage)
}

// Canceller allows cancelling of a scheduled timer event.
type Canceller interface {
	Cancel()
}

type ProposalScheduleData struct {
	adapter          *Dispatcher
	seq              pb.SeqView
	lastConfigChange uint64 //the sequence number at which the epoch change /roration this proposal depends on occurred
}

type PendingEntry struct {
	request  *pb.Request
	previous string
	next     string
}

type ManagerData struct {
	managerMsgQueue     chan *MsgData //contains message to be processed by the manager
	managerDeliverQueue chan *batchInfo
	interruptQueue      chan *InterruptRequest
}

func (s *SBFT) NewManagerData() *ManagerData {
	md := &ManagerData{}
	md.managerMsgQueue = make(chan *MsgData, config.Config.N*8)
	md.managerDeliverQueue = make(chan *batchInfo, config.Config.WatermarkDist+1)   //can't have more than watermarkDist batches waiting to be delivered
	md.interruptQueue = make(chan *InterruptRequest, config.Config.WatermarkDist+2) //we have max watermarkDisk running instances + the reuest receiving threa
	return md
}

type SBFT struct {
	sys        System
	requestSys System

	chainId string
	mrand   *rand.Rand

	batchTimer        Canceller
	lastNewViewSent   *pb.NewView
	viewChangeTimeout time.Duration
	viewChangeTimer   Canceller

	nextProposalInfo *ProposalScheduleData

	cur           map[uint64]*batchInfo
	curLock       sync.RWMutex
	lowWatermark  uint64
	highWatermark uint64
	//lastSeq        uint64
	//nextSeqNo      map[uint64]uint64
	lastRotation   uint64 // the first sequence number in the current rotation
	lastCheckpoint *batchInfo
	lastDelivered  *atomic.Value

	batches                            [][]*pb.Request
	pending                            map[uint64]map[string]*PendingEntry // bucket id -> pending request digest
	pendingPriority                    map[uint64][]string
	leaderPendingSize                  int64  // our pending queue size
	lastLeaderPendingSizeRecomputation uint64 // the last time we recomputed our queue size from scratch

	id                 uint64
	view               uint64 // TODO rename "view" to "epoch"
	leaders            map[uint64]uint64
	blacklistedLeaders map[uint64]uint64

	activeView bool
	inRecovery uint64

	rotationPrimary      uint64
	bucketSize           *big.Int
	buckets              map[uint64][]*big.Int // the sharding of request hash space to buckets
	activeBuckets        map[uint64][]uint64   // leader id -> the first and last bucket of the node
	lastActiveBucket     uint64
	bucketToLeader       map[uint64]uint64 // bucket id -> leader who is responsible for the bucket
	bucketToLeaderLock   sync.RWMutex
	nextActiveBucketLock sync.RWMutex
	nextActiveBucket     map[uint64]uint64 // node id -> the next bucket to propose a request form
	bucketPriorityLock   sync.RWMutex
	bucketPriority       []uint64                // priority queue of buckets base on which contains the oldest request
	vBuckets             map[uint64][][]*big.Int // signature verification sharding
	pBuckets             map[uint64][][]*big.Int // payload sharding
	bucketLocks          map[uint64]*sync.RWMutex
	commitLock           *sync.Mutex

	toBeDeleted        map[uint64][]string
	epochConfig        map[uint64]*epochConfig // available epoch configurations
	currentEpochConfig unsafe.Pointer

	waitingToBeDelivered map[uint64]*batchInfo
	preprepared          []map[string]*clientRequest // bucket id -> (request digest -> seq no)

	clients      map[string]*clientInfo
	clientsLock  sync.RWMutex
	replicaState []replicaInfo

	stop                     uint64
	blockHandlingDispatchers []*Dispatcher
	wg                       sync.WaitGroup

	managerDispatcher         *Dispatcher
	requestHandlingDispatcher *Dispatcher

	nextProposalToSchedule uint64
	managerData            *ManagerData
	//batchQueue chan []*Request //contains batches to be serviced
	unprocessedRequestQueue chan *UnprocessedRequest
	requestQueue            chan ProcessedRequestData //contains requests to be processed
	batchTimeoutQueue       chan Executable
	managerTimeoutQueue     chan Executable
	internalRequestQueue    chan ProcessedRequestData  //contains requests to be re-added to pending
	nextProposalData        chan *ProposalScheduleData //can have at most watermarkDistance proposals to schedule
}

type clientRequest struct {
	client string
	seq    uint64
}

type deliveredRequest struct {
	seq    uint64
	digest []byte
}

type clientInfo struct {
	lowWatermark  uint64
	highWatermark uint64
	delivered     map[uint64]*deliveredRequest
	// since the manager handles both checkpoints and delivery, and these are the only places where client.delivered is read or written, I am not protecting it with a mutex
}

type batchInfo struct {
	subject        *pb.Subject
	timeout        Canceller
	preprep        *pb.Preprepare
	requestHashes  [][]byte
	verifiers      []uint64
	prep           map[uint64]*pb.Subject
	commit         map[uint64]*pb.Commit
	checkpoint     map[uint64]*pb.Checkpoint
	prepared       bool
	committed      bool
	checkpointDone bool
}

type replicaInfo struct {
	//backLog          []*Msg
	hello            *pb.Hello
	signedViewchange *pb.Signed
	viewchange       *pb.ViewChange
}

type epochConfig struct {
	src              uint64
	epoch            uint64
	primary          uint64
	leaders          []uint64
	blacklist        []uint64
	length           uint64
	first            uint64 // first seq no of this epoch
	last             uint64 // last seq no of this epoch
	nextActiveBucket uint64
	subject          *pb.Subject
	newview          *pb.NewView
	echo             map[uint64]*pb.Subject
	accept           map[uint64]*pb.Subject
	valid            bool
	echoed           bool
	accepted         bool
	applied          bool
}

var log = logging.MustGetLogger("sbft")

type dummyCanceller struct{}

func (d dummyCanceller) Cancel() {}

// New creates a new SBFT instance.
func New(id uint64, sys System, dispatchers []*Dispatcher, managerDispatcher *Dispatcher, requestHandlingDispatcher *Dispatcher) (*SBFT, error) {
	if config.Config.F*3+1 > config.Config.N {
		return nil, fmt.Errorf("invalid combination of N (%d) and F (%d)", config.Config.N, config.Config.F)
	}

	s := &SBFT{
		sys:                                sys,
		id:                                 id,
		chainId:                            "chain",
		viewChangeTimer:                    dummyCanceller{},
		replicaState:                       make([]replicaInfo, config.Config.N),
		cur:                                make(map[uint64]*batchInfo, config.Config.WatermarkDist*2),
		clients:                            make(map[string]*clientInfo),
		leaders:                            make(map[uint64]uint64),
		blacklistedLeaders:                 make(map[uint64]uint64),
		vBuckets:                           make(map[uint64][][]*big.Int),
		pBuckets:                           make(map[uint64][][]*big.Int),
		bucketLocks:                        make(map[uint64]*sync.RWMutex),
		commitLock:                         &sync.Mutex{},
		epochConfig:                        make(map[uint64]*epochConfig),
		buckets:                            make(map[uint64][]*big.Int),
		bucketSize:                         nil,
		waitingToBeDelivered:               make(map[uint64]*batchInfo),
		currentEpochConfig:                 nil,
		activeBuckets:                      make(map[uint64][]uint64),
		bucketToLeader:                     make(map[uint64]uint64),
		nextActiveBucket:                   make(map[uint64]uint64),
		pending:                            make(map[uint64]map[string]*PendingEntry),
		pendingPriority:                    make(map[uint64][]string),
		preprepared:                        make([]map[string]*clientRequest, config.Config.Buckets*config.Config.MaxLeaders),
		toBeDeleted:                        make(map[uint64][]string),
		leaderPendingSize:                  0,
		lastLeaderPendingSizeRecomputation: 0,
		bucketPriority:                     make([]uint64, 0, 0),
		blockHandlingDispatchers:           dispatchers,
		managerDispatcher:                  managerDispatcher,
		requestHandlingDispatcher:          requestHandlingDispatcher,
		batches:                            make([][]*pb.Request, 0, 3),
		lowWatermark:                       0,
		highWatermark:                      uint64(config.Config.WatermarkDist),
		lastRotation:                       0,
		lastDelivered: 						new(atomic.Value),
		lastCheckpoint: &batchInfo{
			preprep: &pb.Preprepare{Batch: &pb.Batch{Header: nil}},
			subject: &pb.Subject{Seq: &pb.SeqView{View: 0, Seq: 0}},
		},
	}

	s.lastDelivered.Store(&batchInfo{
		preprep: &pb.Preprepare{Batch: &pb.Batch{Header: nil}},
		subject: &pb.Subject{Seq: &pb.SeqView{View: 0, Seq: 0}},
	})

	s.managerData = s.NewManagerData()
	for i := uint64(0); i < uint64(config.Config.MaxLeaders); i++ {
		s.leaders[i] = i
		s.nextActiveBucket[i] = 0
	}

	for _, d := range s.blockHandlingDispatchers {
		d.sbft = s
	}

	managerDispatcher.sbft = s
	requestHandlingDispatcher.sbft = s

	//s.sys.AddReceiver(chainID, s)

	s.view = 0
	s.epochConfig[0] = &epochConfig{
		epoch:   s.view,
		primary: s.primaryID(),
		first:   1,
		leaders: make([]uint64, 0, 0),
		last:    uint64(config.Config.BucketRotationPeriod),
		length:  uint64(config.Config.BucketRotationPeriod),
		applied: true,
		echo:    make(map[uint64]*pb.Subject),
		accept:  make(map[uint64]*pb.Subject),
	}
	for i := uint64(0); i < uint64(config.Config.MaxLeaders); i++ {
		s.epochConfig[s.view].leaders = append(s.epochConfig[s.view].leaders, i)
	}

	atomic.StorePointer(&s.currentEpochConfig, (unsafe.Pointer(s.epochConfig[0])))

	s.nextProposalToSchedule = s.id + 1 //everyone is a leader at the beginning //TODO is this still needed?

	s.rotationPrimary = 0
	s.initBuckets()
	log.Debugf("replica %d: Buckets (%d) %+v", s.id, len(s.buckets), s.buckets)
	for i := 0; i < len(s.buckets); i++ {
		s.bucketLocks[uint64(i)] = new(sync.RWMutex)
		s.pendingPriority[uint64(i)] = make([]string, 2, 2)
		s.pendingPriority[uint64(i)][0] = ""
		s.pendingPriority[uint64(i)][1] = ""
		s.toBeDeleted[uint64(i)] = make([]string, 0, config.Config.WatermarkDist*requestPerBatch)
		s.pending[uint64(i)] = make(map[string]*PendingEntry)
		s.preprepared[i] = make(map[string]*clientRequest, 32000) //TODO make this a paramenter
	}

	if config.Config.Censoring > 0 {
		source := rand.NewSource(time.Now().UnixNano())
		s.mrand = rand.New(source)
	}

	s.assignBuckets()
	s.assignVerificationBuckets()
	s.assignPayloadBuckets(false)
	s.unsetRecovery()
	s.activeView = true

	s.stop = 0
	//s.batchQueue = make (chan []*Request, batchQueueSize)
	s.requestQueue = make(chan ProcessedRequestData, requestQueueSize)
	s.unprocessedRequestQueue = make(chan *UnprocessedRequest, requestQueueSize)
	s.batchTimeoutQueue = make(chan Executable, timeoutQueueSize)
	s.managerTimeoutQueue = make(chan Executable, timeoutQueueSize)
	s.internalRequestQueue = make(chan ProcessedRequestData, 9*requestQueueSize)
	s.nextProposalData = make(chan *ProposalScheduleData, config.Config.WatermarkDist+1)

	//svc := &Signed{}
	//if s.sys.Restore(s.chainId, viewchange, svc) {
	//	vc := &ViewChange{}
	//	err := proto.Unmarshal(svc.Data, vc)
	//	if err != nil {
	//		return nil, err
	//	}
	//	fmt.Println(fmt.Sprintf("rep %d VIEW %d   %d", s.id, s.view, vc.View))
	//	s.view = vc.View
	//	s.replicaState[s.id].signedViewchange = svc
	//	s.activeView = false
	//}
	//
	//pp := &Preprepare{}
	//// TODO must restore the cur
	//if s.sys.Restore(s.chainId, preprepared, pp) && pp.Seq.View >= s.view {
	//	s.view = pp.Seq.View
	//	s.activeView = true
	//	if pp.Seq.Seq > s.seq() {
	//		// TODO double add to BC?
	//		s.acceptPreprepare(pp)
	//	}
	//}
	//
	//c := &Subject{}
	//if s.sys.Restore(s.chainId, prepared, c) {
	//	if b, ok := s.cur[c.Seq.Seq]; ok {
	//		if reflect.DeepEqual(c, &b.subject) && c.Seq.View >= s.view {
	//			b.prepared = true
	//		}
	//	}
	//}
	//
	//ex := &Subject{}
	//if s.sys.Restore(s.chainId, committed, ex) {
	//	if b, ok := s.cur[ex.Seq.Seq]; ok {
	//		if reflect.DeepEqual(c, &b.subject) && ex.Seq.View >= s.view {
	//			b.committed = true
	//		}
	//	}
	//}

	s.cancelViewChangeTimer()

	s.maybeAllowMoreProposingInstances()
	return s, nil
}

////////////////////////////////////////////////

func (s *SBFT) GetChainId() string {
	return s.chainId
}

func (s *SBFT) primaryIDView(v uint64) uint64 {
	return v % uint64(config.Config.N)
}

func (s *SBFT) primaryID() uint64 {
	return s.primaryIDView(s.view)
}

func (s *SBFT) isPrimary() bool {
	return s.primaryID() == s.id
}

func (s *SBFT) isLeader(id uint64) bool {
	for _, v := range s.leaders {
		if id == v {
			return true
		}
	}
	return false
}

func (s *SBFT) isLeaderInCrtEpoch(id uint64) bool {
	config := s.getCurrentEpochConfig()
	for _, v := range config.leaders {
		if id == v {
			return true
		}
	}
	return false
}

func (s *SBFT) leads(id uint64, seqNo uint64, config *epochConfig) bool {
	i := (seqNo - config.first) % uint64(len(s.leaders))
	return config.leaders[i] == id
}

func (s *SBFT) getLeaderOfSequence(seqNo uint64, config *epochConfig) uint64 {
	log.Errorf("replica %d: get leader of sequence %d, config.first %d, leader set size %d", s.id, seqNo, config.first, len(config.leaders))
	return config.leaders[(seqNo-config.first)%uint64(len(config.leaders))]
}

//func (s *SBFT) seq() uint64 {
//	return s.lastSeq
//	// return s.sys.LastBatch(s.chainId).DecodeHeader().Seq
//}
//
//func (s *SBFT) nextSeq(leader uint64) SeqView {
//	s.lastSeq = s.nextSeqNo[leader]
//	seqView := SeqView{Seq: s.nextSeqNo[leader], View: s.view}
//	s.nextSeqNo[leader] = s.nextSeqNo[leader] + uint64(len(s.epochConfig[s.view].leaders))
//	return seqView
//}

func (s *SBFT) getCurrentEpochConfig() *epochConfig {
	ec := (*epochConfig)(atomic.LoadPointer(&s.currentEpochConfig))
	return ec
}

func (s *SBFT) setCurrentEpochConfig(config *epochConfig) {
	atomic.StorePointer(&s.currentEpochConfig, unsafe.Pointer(config))
}

func (s *SBFT) nextView() uint64 {
	v := s.getView()
	return v + 1
}

//only the manager/delivery thread should be calling this method
func (s *SBFT) maybeAllowMoreProposingInstances() {

	if !s.isLeader(s.id) {
		log.Infof("replica %d: not leader -- now allowing more proposing instances")
		s.processAllBacklogs()
		return
	}

	hW := atomic.LoadUint64(&s.highWatermark)
	nE := atomic.LoadUint64(&s.epochConfig[s.view].last) + 1
	nF := atomic.LoadUint64(&s.epochConfig[s.view].first)
	nR := atomic.LoadUint64(&s.lastRotation)

	lastCfgChange := nF
	if nF < nR {
		lastCfgChange = nR
	}

	nR += uint64(config.Config.BucketRotationPeriod)

	min := hW + 1

	if !s.isInRecovery() {
		if nR < min {
			min = nR
		}
	} else {
		if nE < min {
			min = nE
		}
	}

	nP := atomic.LoadUint64(&s.nextProposalToSchedule)

	log.Errorf("replica %d maybe allow more proposing instances: nextProposalToSchodeule %d, highWatermark: %d, epoch.last: %d, nextRotation: %d, min is %d", s.id, nP, hW, nE-1, nR, min)

	nL := uint64(len(s.epochConfig[s.view].leaders))

	updated := false

	for nP < min {
		log.Infof("replica %d can now propose instance with sequence %d", s.id, nP)
		s.nextProposalData <- &ProposalScheduleData{s.getAdapter(nP), pb.SeqView{View: s.view, Seq: nP}, lastCfgChange}
		nP = nP + nL
		atomic.StoreUint64(&s.nextProposalToSchedule, nP)
		updated = true
	}

	if updated {
		s.processAllBacklogs()
	}

}

func (s *SBFT) canExecuteInstance(seq uint64, epochConfig *epochConfig) bool {

	hW := atomic.LoadUint64(&s.highWatermark)

	if seq > hW {
		return false
	}

	nE := epochConfig.last

	if s.isInRecovery() {
		if seq > nE {
			return false
		}
	} else {
		nR := atomic.LoadUint64(&s.lastRotation)
		nR += uint64(config.Config.BucketRotationPeriod)

		if seq >= nR {
			return false
		}
	}

	return true
}

func (s *SBFT) getAdapter(seq uint64) *Dispatcher {
	numAdapters := uint64(len(s.blockHandlingDispatchers))
	adapter := seq % numAdapters
	return s.blockHandlingDispatchers[adapter]
}

func (s *SBFT) commonCaseQuorum() int {
	//When N=3F+1 this should be 2F+1 (N-F)
	//More generally, we need every two common case quorums of size X to intersect in at least F+1 orderers,
	//hence 2X>=N+F+1, or X is:
	return int(math.Ceil(float64(config.Config.N+config.Config.F+1) / float64(2)))
}

func (s *SBFT) viewChangeQuorum() int {
	//When N=3F+1 this should be 2F+1 (N-F)
	//More generally, we need every view change quorum to intersect with every common case quorum at least F+1 orderers, hence:
	//Y >= N-X+F+1
	return config.Config.N + config.Config.F + 1 - s.commonCaseQuorum()
}

func (s *SBFT) oneCorrectQuorum() int {
	return config.Config.F + 1
}

func (s *SBFT) send(dst uint64, m *pb.Msg, sys System) {
	sys.Send(s.chainId, m, dst)
}

func (s *SBFT) broadcastSystem(m *pb.Msg, sys System) {
	for i := uint64(0); i < uint64(config.Config.N); i++ {
		sys.Send("protocol", m, i)
	}
}

func (s *SBFT) broadcast(m *pb.Msg) {
	for i := uint64(0); i < uint64(config.Config.N); i++ {
		s.sys.Send("manager", m, i)
	}
}

func (s *SBFT) broadcastToRest(m *pb.Msg, sys System) {
	for i := uint64(0); i < uint64(config.Config.N); i++ {
		if i != s.id {
			sys.Send("protocol", m, i)
		}
	}
}

func (s *SBFT) getView() uint64 {
	return s.view
	//v:=atomic.LoadUint64(&s.view)
	//return v
}

func (s *SBFT) setView(view uint64) {
	s.view = view
	//atomic.StoreUint64(&s.view, view)

}

////////////////////////////////////////////////

// Receive is the ingress method for SBFT messages.
func (s *SBFT) Receive(m *pb.Msg, src uint64) {
	log.Debugf("replica %d: received message from %d: %s", s.id, src, m)

	//_, ok := s.blacklistedLeaders[src]
	//if ok {
	//	delete(s.blacklistedLeaders, src)
	//}

	if h := m.GetHello(); h != nil {
		s.handleHello(h, src)
		return
	} else if req := m.GetRequest(); req != nil {
		panic("replica should not receive request via the Receive method")
		return
	} else if vs := m.GetViewChange(); vs != nil {
		s.handleViewChange(vs, src)
		return
	} else if nv := m.GetNewView(); nv != nil {
		s.handleNewEpoch(nv, src)
		return
	} else if c := m.GetCheckpoint(); c != nil {
		if s.managerDispatcher.testBacklogMessage(m, src) {
			log.Debugf("replica %d: message for future seq, storing for later", s.id)
			s.managerDispatcher.recordBacklogMsg(m, src)
			return
		}
		s.handleCheckpoint(c, src)
		return
	} else if nve := m.GetEpochCfgEcho(); nve != nil {
		s.handleEpochCfgEcho(nve, src)
		return
	} else if nva := m.GetEpochCfgAccept(); nva != nil {
		s.handleEpochCfgAccept(nva, src)
		return
	} else if ec := m.GetConfig(); ec != nil {
		s.handleEpochCfg(ec, src)
		return
	}

	log.Errorf("replica %d: manager received wrong message type %v", s.id, m)

	//s.handleQueueableMessage(m, src)
}

func (instance *BFTInstance) handleQueueableMessage(m *pb.Msg, src uint64) {

	if pp := m.GetPreprepare(); pp != nil {
		instance.handlePreprepare(pp, src)
		return
	} else if p := m.GetPrepare(); p != nil {
		instance.handlePrepare(p, src)
		return
	} else if c := m.GetCommit(); c != nil {
		instance.handleCommit(c, src)
		return
	}

	log.Warningf("replica %d: received invalid message from %d", instance.sbft.id, src)
}

func (s *SBFT) advanceWatermarks(low uint64) {
	log.Errorf("replica %d advancing watermarks; high watermark now %d", s.id, low+uint64(config.Config.WatermarkDist))
	atomic.StoreUint64(&s.lowWatermark, low)
	atomic.StoreUint64(&s.highWatermark, low+uint64(config.Config.WatermarkDist))
	s.maybeAllowMoreProposingInstances()
}

func (s *SBFT) inWatermarkRange(seq uint64) bool {
	lw := atomic.LoadUint64(&s.lowWatermark)
	hw := atomic.LoadUint64(&s.highWatermark)
	if seq > lw && seq <= hw {
		return true
	}
	return false
}

//don't really need this, as waiting for delivery is equivalent to this
//func (s* SBFT) waitForInstanceCompletion() {
//	st := s.stop
//	if st % 4 != 0 {
//		panic("calling stop when instances not running")
//	}
//
//	st++
//	atomic.StoreUint64(&s.stop,st)
//	s.wg.Wait() //wait for running instances to complete and for requestHandler to acknowledge
//}

func (s *SBFT) haltAllInstances() {

	log.Debugf("replica %d: starting halt all instances", s.id)

	//all currently scheduled proposals should be cancelled
L:
	for {
		select {
		case <-s.nextProposalData:
		default:
			break L
		}
	}

	log.Debugf("replica %d: drained proposal queue", s.id)

	st := s.stop
	if st%4 == 3 {
		log.Debugf("replica %d: instances already stopped, haltAllInstances returning", s.id)
		return
	}
	st = st + 3
	if st%4 != 3 {
		panic("Wrong stop value!")
	}
	atomic.StoreUint64(&s.stop, st)

	log.Debugf("replica %d: set stop flag", s.id)

	for _, d := range s.blockHandlingDispatchers {
		d.managerQueue <- &stopEvent{1}
	}
	log.Debugf("replica %d: notified dispatchers", s.id)

	//TODO maybe use channels to disseminate halt request
	s.wg.Wait()
	log.Debugf("replica %d: finished wait group", s.id)
}

func (s *SBFT) resumeAllInstances() {
	st := s.stop
	if st%4 == 0 {
		panic("resuming instances that are already running")
	}
	md := st % 4
	st = st - md + 4

	if st%4 != 0 {
		panic("Wrong stop value!")
	}
	s.wg.Add(len(s.blockHandlingDispatchers))
	atomic.StoreUint64(&s.stop, st)
}

//this function may be called by other threads than the manager
func (s *SBFT) isRunning() bool {
	st := atomic.LoadUint64(&s.stop)

	if st%4 == 0 {
		return true
	}

	return false
}

//this function may be called by other threads than the manager
func (s *SBFT) instancesHalted() bool {
	st := atomic.LoadUint64(&s.stop)

	if st%4 == 3 {
		return true
	}

	return false
}

func (s *SBFT) isInRecovery() bool {
	r := atomic.LoadUint64(&s.inRecovery)
	if r == 1 {
		return true
	}
	return false
}

func (s *SBFT) setRecovery() {
	log.Errorf("replica %d: now in recovery mode", s.id)
	atomic.StoreUint64(&s.inRecovery, 1)
}

func (s *SBFT) unsetRecovery() {
	atomic.StoreUint64(&s.inRecovery, 0)
}

func (c *clientInfo) getHighWatermark() uint64 {
	return atomic.LoadUint64(&c.highWatermark)
}

func (c *clientInfo) getLowWatermark() uint64 {
	return atomic.LoadUint64(&c.lowWatermark)
}

func (c *clientInfo) setHighWatermark(hw uint64) {
	atomic.StoreUint64(&c.highWatermark, hw)
}

func (c *clientInfo) setLowWatermark(lw uint64) {
	atomic.StoreUint64(&c.lowWatermark, lw)
}

func (bI *batchInfo) maybeCancelTimer() {
	if bI.timeout == nil {
		return
	}
	bI.timeout.Cancel()
}

//the methods dealing with pending are not synchronized
// the method returns true if the request was added in pending and false if the request
// was already in pending and hence was not added
func (s *SBFT) insertInPending(bucket uint64, key string, req *pb.Request) bool {
	_, ok := s.pending[bucket][key]
	if ok {
		//already in pending
		return false
	}
	isFirst := false
	if s.pendingPriority[bucket][0] == "" {
		isFirst = true
	}

	if isFirst {
		newEntry := &PendingEntry{req, "", ""}

		s.pending[bucket][key] = newEntry

		s.pendingPriority[bucket][0] = key
		s.pendingPriority[bucket][1] = key

	} else {
		lastKey := s.pendingPriority[bucket][1]
		last := s.pending[bucket][lastKey]

		newEntry := &PendingEntry{req, lastKey, ""}

		s.pending[bucket][key] = newEntry
		last.next = key
		s.pendingPriority[bucket][1] = key
	}
	return true
}

func (s *SBFT) popFromPending(bucket uint64) (string, *pb.Request) {
	firstKey := s.pendingPriority[bucket][0]
	if firstKey == "" {
		log.Debugf("replica %d: no entries to remove from pending", s.id)
		return "", nil
	}
	toRemove, ok := s.pending[bucket][firstKey]
	if !ok {
		log.Criticalf("replica %d: trying to remove entry that is not in pending", s.id)
		return "", nil
	}

	nextKey := toRemove.next
	prevKey := toRemove.previous

	s.pendingPriority[bucket][0] = nextKey

	//var prev *PendingEntry = nil

	//if toRemove.previous != "" {
	//	prev, ok = s.pending[bucket][toRemove.previous]
	//	if !ok {
	//		log.Criticalf("replica %d: trying to find entry that is not in pending", s.id)
	//		return "", nil
	//	}
	//}

	if toRemove.next != "" {
		next, ok := s.pending[bucket][toRemove.next]
		if !ok {
			log.Criticalf("replica %d: trying to find entry that is not in pending", s.id)
			return "", nil
		}
		next.previous = prevKey
	} else {
		//only one entry in the bucket
		s.pendingPriority[bucket][1] = ""
	}

	//prev should be nil
	//if prev != nil {
	//	prev.next = toRemove.next
	//}

	delete(s.pending[bucket], firstKey)

	return firstKey, toRemove.request
}

func (s *SBFT) deleteFromPending(bucket uint64, key string) bool {
	toRemove, ok := s.pending[bucket][key]
	if !ok {
		log.Debugf("replica %d: could not delete entry %s from pending, not present", s.id, key)
		return false
	}

	prevKey := toRemove.previous
	nextKey := toRemove.next

	var prev *PendingEntry = nil
	if prevKey != "" {
		prev, ok = s.pending[bucket][toRemove.previous]
		if !ok {
			log.Criticalf("replica %d: trying to find entry that is not in pending", s.id)
			return false
		}
	}

	var next *PendingEntry = nil
	if nextKey != "" {
		next, ok = s.pending[bucket][toRemove.next]
		if !ok {
			log.Criticalf("replica %d: trying to find entry that is not in pending", s.id)
			return false
		}
	}

	if prev != nil {
		prev.next = nextKey
	} else {
		s.pendingPriority[bucket][0] = nextKey
	}

	if next != nil {
		next.previous = prevKey
	} else {
		s.pendingPriority[bucket][1] = prevKey
	}

	delete(s.pending[bucket], key)
	return true
}
