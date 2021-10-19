package iss

import (
	"container/list"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspbftpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type Config struct {
	SegmentLength   int // In sequence numbers
	MaxBatchSize    int // In requests
	MaxProposeDelay int // In ticks
	NumBuckets      int
	leaderPolicy    LeaderSelectionPolicy
}

type Membership []t.NodeID

type ISS struct {
	config     Config
	membership Membership
	buckets    []*requestBucket
	epoch      t.EpochNr
	orderers   map[t.SBInstanceID]sbInstance
	logger     logging.Logger
}

func New(config Config, membership Membership, logger logging.Logger) *ISS {

	// Initialize bucket data structures.
	buckets := make([]*requestBucket, config.NumBuckets, config.NumBuckets)
	for i := 0; i < config.NumBuckets; i++ {
		buckets[i] = &requestBucket{} // No need to initialize requests filed. It's zero value is already an empty list.
	}

	// Compute initial bucket assignment
	leaderBuckets := distributeBuckets(buckets, config.leaderPolicy.Leaders(0))

	orderers := make(map[t.SBInstanceID]sbInstance)
	for i, leader := range config.leaderPolicy.Leaders(0) {
		orderers[t.SBInstanceID(i)] = newPbftInstance(leader, config.SegmentLength, leaderBuckets[leader])
	}
	return &ISS{
		config:     config,
		membership: membership,
		buckets:    buckets,
		epoch:      0,
		orderers:   orderers,
		logger:     logger,
	}
}

// leaders must not be empty!
// Will need to be updated to have a more sophisticated, liveness-ensuring implementation.
func distributeBuckets(buckets []*requestBucket, leaders []t.NodeID) map[t.NodeID][]*requestBucket {
	leaderBuckets := make(map[t.NodeID][]*requestBucket)
	for _, leader := range leaders {
		leaderBuckets[leader] = make([]*requestBucket, 0)
	}

	leaderIdx := 0
	for _, bucket := range buckets {
		leaderBuckets[leaders[leaderIdx]] = append(leaderBuckets[leaders[leaderIdx]], bucket)
		leaderIdx++
		if leaderIdx == len(leaders) {
			leaderIdx = 0
		}
	}

	return leaderBuckets
}

func (iss *ISS) ApplyEvent(event *eventpb.Event) *events.EventList {
	switch e := event.Type.(type) {
	case *eventpb.Event_Tick:
		return iss.handleTick()
	case *eventpb.Event_RequestReady:
		return iss.handleRequest(e.RequestReady.RequestRef)
	case *eventpb.Event_MessageReceived:
		return iss.handleMessage(e.MessageReceived.Msg, t.NodeID(e.MessageReceived.From))
	default:
		panic(fmt.Sprintf("unknown ISS event type: %T", event.Type))
	}

	return &events.EventList{}
}

func (iss *ISS) Status() (s *status.StateMachine, err error) {
	return nil, nil
}

func (iss *ISS) handleRequest(ref *requestpb.RequestRef) *events.EventList {
	iss.logger.Log(logging.LevelDebug, "ISS handling message.")

	// Add request to its bucket
	iss.buckets[iss.bucketNumber(ref)].add(ref)

	// TODO: Check if batch is full and propose if yes.

	return &events.EventList{}
}

func (iss *ISS) bucketNumber(reqRef *requestpb.RequestRef) int {
	return int(reqRef.ClientId+reqRef.ReqNo) % iss.config.NumBuckets // If types change, this needs to be updated.
}

func (iss *ISS) handleMessage(message *messagepb.Message, from t.NodeID) *events.EventList {
	return nil
}

func (iss *ISS) handleTick() *events.EventList {
	// On each tick, propose a new batch
	// TODO:
	return &events.EventList{}
}

type requestBucket struct {
	requests list.List
}

func (b *requestBucket) add(reqRef *requestpb.RequestRef) {
	b.requests.PushBack(reqRef)
}

type sbInstance interface {
	handleMessage(msg *isspb.SBMessage, source t.NodeID) *events.EventList
	handleTick() *events.EventList
}

type pbftSlot struct {
	Sn t.SeqNr
}

type pbftInstance struct {
	leader  t.NodeID
	slots   map[t.SeqNr]*pbftSlot
	buckets []*requestBucket
}

func newPbftInstance(leader t.NodeID, length int, buckets []*requestBucket) *pbftInstance {
	slots := make(map[t.SeqNr]*pbftSlot)
	for sn := 0; sn < length; sn++ {
		slots[t.SeqNr(sn)] = &pbftSlot{Sn: t.SeqNr(sn)}
	}
	return &pbftInstance{
		leader:  leader,
		slots:   slots,
		buckets: buckets,
	}
}

func (pbft *pbftInstance) handleMessage(message *isspb.SBMessage, from t.NodeID) *events.EventList {
	switch msg := message.Type.(type) {

	case *isspb.SBMessage_PbftPreprepare:
		return pbft.handlePreprepare(msg.PbftPreprepare)

	default:
		// Panic if message type is not known.
		panic(fmt.Sprintf("unknown DummyProtocol message type (from %d): %T", from, message.Type))
	}
}

func (pbft *pbftInstance) handleTick() *events.EventList {
	return nil
}

func (pbft *pbftInstance) handlePreprepare(preprepare *isspbftpb.Preprepare) *events.EventList {
	return nil
}
