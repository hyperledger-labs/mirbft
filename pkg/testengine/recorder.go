/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"crypto"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/recording"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/processor"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
)

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type Link struct {
	Source     uint64
	EventQueue *EventQueue
	Delay      int64
}

func (l *Link) Send(dest uint64, msg *msgs.Msg) {
	l.EventQueue.InsertMsgReceived(dest, l.Source, msg, l.Delay)
}

type NodeConfig struct {
	InitParms    *state.EventInitialParameters
	RuntimeParms *RuntimeParameters
}

type RuntimeParameters struct {
	TickInterval int
	LinkLatency  int

	ProcessWALLatency      int
	ProcessNetLatency      int
	ProcessHashLatency     int
	ProcessClientLatency   int
	ProcessAppLatency      int
	ProcessReqStoreLatency int
	ProcessEventsLatency   int
}

type clientReq struct {
	clientID uint64
	reqNo    uint64
}

// ackHelper is a map-key usable version of the msgs.RequestAck
type ackHelper struct {
	clientID uint64
	reqNo    uint64
	digest   string
}

func newAckHelper(ack *msgs.RequestAck) ackHelper {
	return ackHelper{
		reqNo:    ack.ReqNo,
		clientID: ack.ClientId,
		digest:   string(ack.Digest),
	}
}

type ReqStore struct {
	requests    map[ackHelper][]byte
	allocations map[clientReq][]byte
}

func NewReqStore() *ReqStore {
	return &ReqStore{
		requests:    map[ackHelper][]byte{},
		allocations: map[clientReq][]byte{},
	}
}

func (rs *ReqStore) PutRequest(ack *msgs.RequestAck, data []byte) error {
	helper := newAckHelper(ack)
	rs.requests[helper] = data
	// TODO, deal with free-ing
	return nil
}

func (rs *ReqStore) GetRequest(ack *msgs.RequestAck) ([]byte, error) {
	helper := newAckHelper(ack)
	data := rs.requests[helper]
	return data, nil
}

func (rs *ReqStore) PutAllocation(clientID, reqNo uint64, digest []byte) error {
	rs.allocations[clientReq{clientID: clientID, reqNo: reqNo}] = digest
	return nil
}

func (rs *ReqStore) GetAllocation(clientID, reqNo uint64) ([]byte, error) {
	digest := rs.allocations[clientReq{clientID: clientID, reqNo: reqNo}]
	return digest, nil
}

func (rs *ReqStore) Sync() error {
	return nil
}

type WAL struct {
	LowIndex uint64
	List     *list.List
}

func NewWAL(initialState *msgs.NetworkState, initialCP []byte) *WAL {
	wal := &WAL{
		List:     list.New(),
		LowIndex: 1,
	}

	wal.List.PushBack(&msgs.Persistent{
		Type: &msgs.Persistent_CEntry{
			CEntry: &msgs.CEntry{
				SeqNo:           0,
				CheckpointValue: initialCP,
				NetworkState:    initialState,
			},
		},
	})

	wal.List.PushBack(&msgs.Persistent{
		Type: &msgs.Persistent_FEntry{
			FEntry: &msgs.FEntry{
				EndsEpochConfig: &msgs.EpochConfig{
					Number:  0,
					Leaders: initialState.Config.Nodes,
				},
			},
		},
	})

	return wal
}

func (wal *WAL) Write(index uint64, p *msgs.Persistent) error {
	if index != wal.LowIndex+uint64(wal.List.Len()) {
		return errors.Errorf("WAL out of order: expect next index %d, but got %d", wal.LowIndex+uint64(wal.List.Len()), index)
	}

	wal.List.PushBack(p)

	return nil
}

func (wal *WAL) Truncate(index uint64) error {
	if index < wal.LowIndex {
		return errors.Errorf("asked to truncated to index %d, but lowIndex is %d", index, wal.LowIndex)
	}

	toRemove := int(index - wal.LowIndex)
	if toRemove >= wal.List.Len() {
		return errors.Errorf("asked to truncate to index %d, but highest index is %d", index, wal.LowIndex+uint64(wal.List.Len()))
	}

	for ; toRemove > 0; toRemove-- {
		wal.List.Remove(wal.List.Front())
		wal.LowIndex++
	}

	return nil
}

// TODO, deal with this in the processor
func (wal *WAL) LoadAll(iter func(index uint64, p *msgs.Persistent)) error {
	i := uint64(0)
	for el := wal.List.Front(); el != nil; el = el.Next() {
		iter(wal.LowIndex+i, el.Value.(*msgs.Persistent))
		i++
	}
	return nil
}

func (wal *WAL) Sync() error {
	return nil
}

type Node struct {
	ID                           uint64
	Config                       *NodeConfig
	WAL                          *WAL
	Link                         *Link
	Hasher                       processor.Hasher
	Interceptor                  processor.EventInterceptor
	ReqStore                     *ReqStore
	WorkItems                    *processor.WorkItems
	Clients                      *processor.Clients
	State                        *NodeState
	ProcessResultEventsPending   bool
	ProcessReqStoreEventsPending bool
	ProcessWALActionsPending     bool
	ProcessNetActionsPending     bool
	ProcessHashActionsPending    bool
	ProcessAppActionsPending     bool
	ProcessClientActionsPending  bool
	StateMachine                 *statemachine.StateMachine
}

func (n *Node) Initialize(initParms *state.EventInitialParameters, logger statemachine.Logger) error {
	n.WorkItems = processor.NewWorkItems()

	n.Clients = &processor.Clients{
		RequestStore: n.ReqStore,
		Hasher:       n.Hasher,
	}

	n.StateMachine = &statemachine.StateMachine{
		Logger: logger,
	}

	n.ProcessResultEventsPending = false
	n.ProcessReqStoreEventsPending = false
	n.ProcessWALActionsPending = false
	n.ProcessNetActionsPending = false
	n.ProcessHashActionsPending = false
	n.ProcessAppActionsPending = false
	n.ProcessClientActionsPending = false

	events, err := processor.RecoverWALForExistingNode(n.WAL, initParms)
	if err != nil {
		return err
	}

	n.WorkItems.ResultEvents().PushBackList(events)
	return nil
}

type RecorderClient struct {
	Config *ClientConfig
	Hasher processor.Hasher
}

func (rc *RecorderClient) RequestByReqNo(reqNo uint64) []byte {
	if reqNo >= rc.Config.Total {
		// We've sent all we should
		return nil
	}

	var buf bytes.Buffer
	buf.Write(uint64ToBytes(rc.Config.ID))
	buf.Write([]byte("-"))
	buf.Write(uint64ToBytes(reqNo))

	return buf.Bytes()
}

type NodeState struct {
	Hasher                  processor.Hasher
	ActiveHash              hash.Hash
	LastSeqNo               uint64
	ReconfigPoints          []*ReconfigPoint
	PendingReconfigurations []*msgs.Reconfiguration
	ReqStore                *ReqStore
	CheckpointSeqNo         uint64
	CheckpointHash          []byte
	CheckpointState         *msgs.NetworkState

	// The below vars are used for assertions on results,
	// but are not used directly in execution.
	StateTransfers []uint64
}

func (ns *NodeState) Snap(networkConfig *msgs.NetworkState_Config, clientsState []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error) {
	pr := ns.PendingReconfigurations
	ns.PendingReconfigurations = nil

	ns.CheckpointSeqNo = ns.LastSeqNo
	ns.CheckpointState = &msgs.NetworkState{
		Config:                  networkConfig,
		Clients:                 clientsState,
		PendingReconfigurations: pr,
	}
	ns.CheckpointHash = ns.ActiveHash.Sum(nil)
	ns.ActiveHash = ns.Hasher.New()
	ns.ActiveHash.Write(ns.CheckpointHash)

	nsBytes, err := proto.Marshal(ns.CheckpointState)
	if err != nil {
		return nil, nil, err
	}

	// Note, this is a test hack, balooning the checkpoint size, but it
	// simplifies having to go look up the checkpoint data on another node.
	// TODO, fetch it properly?
	value := append([]byte{}, ns.CheckpointHash...)
	value = append(value, nsBytes...)

	return value, pr, nil
}

func (ns *NodeState) TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error) {
	ns.StateTransfers = append(ns.StateTransfers, seqNo)
	networkState := &msgs.NetworkState{}
	if err := proto.Unmarshal(snap[32:], networkState); err != nil {
		return nil, err
	}

	ns.LastSeqNo = seqNo
	ns.CheckpointSeqNo = seqNo
	ns.CheckpointState = networkState
	ns.CheckpointHash = snap[:32]
	ns.ActiveHash = ns.Hasher.New()
	ns.ActiveHash.Write(ns.CheckpointHash)

	return networkState, nil
}

func (ns *NodeState) Apply(batch *msgs.QEntry) error {
	ns.LastSeqNo++
	if batch.SeqNo != ns.LastSeqNo {
		return errors.Errorf("unexpected out of order commit sequence number, expected %d, got %d", ns.LastSeqNo, batch.SeqNo)
	}

	for _, request := range batch.Requests {
		req, err := ns.ReqStore.GetRequest(request)
		if err != nil {
			return errors.WithMessage(err, "unexpected reqstore error")
		}
		if req == nil {
			return errors.Errorf("reqstore should have request if we are committing it")
		}

		ns.ActiveHash.Write(request.Digest)

		for _, reconfigPoint := range ns.ReconfigPoints {
			if reconfigPoint.ClientID == request.ClientId &&
				reconfigPoint.ReqNo == request.ReqNo {
				ns.PendingReconfigurations = append(ns.PendingReconfigurations, reconfigPoint.Reconfiguration)
			}
		}
	}

	return nil
}

type ClientConfig struct {
	ID          uint64
	MaxInFlight int
	Total       uint64

	// IgnoreNodes may be set to cause the client not to send requests
	// to a particular set of nodes.  This is useful for testing request
	// forwarding behavior.
	IgnoreNodes []uint64
}

func (cc *ClientConfig) shouldSkip(nodeID uint64) bool {
	for _, id := range cc.IgnoreNodes {
		if id == nodeID {
			return true
		}
	}

	return false
}

type ReconfigPoint struct {
	ClientID        uint64
	ReqNo           uint64
	Reconfiguration *msgs.Reconfiguration
}

type Recorder struct {
	NetworkState   *msgs.NetworkState
	NodeConfigs    []*NodeConfig
	ClientConfigs  []*ClientConfig
	ReconfigPoints []*ReconfigPoint
	Mangler        Mangler
	LogOutput      io.Writer
	Hasher         processor.Hasher
	RandomSeed     int64
}

type interceptorFunc func(*state.Event) error

func (icf interceptorFunc) Intercept(e *state.Event) error {
	return icf(e)
}

func (r *Recorder) Recording(output *gzip.Writer) (*Recording, error) {
	eventQueue := &EventQueue{
		List:    list.New(),
		Rand:    rand.New(rand.NewSource(r.RandomSeed)),
		Mangler: r.Mangler,
	}

	nodes := make([]*Node, len(r.NodeConfigs))
	for i, recorderNodeConfig := range r.NodeConfigs {
		nodeID := uint64(i)

		reqStore := NewReqStore()

		nodeState := &NodeState{
			Hasher:         r.Hasher,
			ActiveHash:     r.Hasher.New(),
			ReconfigPoints: r.ReconfigPoints,
			ReqStore:       reqStore,
		}

		checkpointValue, _, err := nodeState.Snap(r.NetworkState.Config, r.NetworkState.Clients)
		if err != nil {
			return nil, errors.WithMessage(err, "could not generate initial checkpoint")
		}

		wal := NewWAL(r.NetworkState, checkpointValue)

		nodes[i] = &Node{
			Hasher:   r.Hasher,
			State:    nodeState,
			WAL:      wal,
			ReqStore: reqStore,
			Link: &Link{
				EventQueue: eventQueue,
				Source:     nodeID,
				Delay:      int64(recorderNodeConfig.RuntimeParms.LinkLatency),
			},
			Interceptor: interceptorFunc(func(e *state.Event) error {
				return eventlog.WriteRecordedEvent(output, &recording.Event{
					NodeId:     nodeID,
					Time:       eventQueue.FakeTime,
					StateEvent: e,
				})
			}),
			Config: recorderNodeConfig,
		}

		eventQueue.InsertInitialize(nodeID, recorderNodeConfig.InitParms, 0)
	}

	clients := make([]*RecorderClient, len(r.ClientConfigs))
	for i, clientConfig := range r.ClientConfigs {
		client := &RecorderClient{
			Config: clientConfig,
			Hasher: r.Hasher,
		}

		clients[i] = client
	}

	return &Recording{
		Hasher:           r.Hasher,
		EventQueue:       eventQueue,
		Nodes:            nodes,
		Clients:          clients,
		EventQueueOutput: output,
		LogOutput:        r.LogOutput,
	}, nil
}

type Recording struct {
	Hasher           processor.Hasher
	EventQueue       *EventQueue
	Nodes            []*Node
	Clients          []*RecorderClient
	LogOutput        io.Writer
	EventQueueOutput *gzip.Writer
}

func (r *Recording) Step() error {
	if r.EventQueue.List.Len() == 0 {
		return errors.Errorf("event log is empty, nothing to do")
	}

	event := r.EventQueue.ConsumeEvent()
	nodeID := event.Target
	node := r.Nodes[int(nodeID)]
	runtimeParms := node.Config.RuntimeParms

	switch {
	case event.Initialize != nil:
		// If this is an Initialize, it is either the first event for the node,
		// and nothing else is in the log, or, it is a restart.  In the case of
		// a restart, we clear any outstanding events associated to this NodeID from
		// the log.  This is especially important for things like pending actions,
		// but also makes sense for things like in flight messages.
		el := r.EventQueue.List.Front()
		for el != nil {
			x := el
			el = el.Next()
			if x.Value.(*Event).Target == nodeID {
				r.EventQueue.List.Remove(x)
			}
		}

		err := node.Initialize(
			event.Initialize.InitParms,
			NamedLogger{
				Output: r.LogOutput,
				Level:  statemachine.LevelInfo,
				Name:   fmt.Sprintf("node%d", nodeID),
			},
		)
		if err != nil {
			return errors.WithMessage(err, "unexpected error initializing node")
		}

		r.EventQueue.InsertTickEvent(nodeID, int64(runtimeParms.TickInterval))

		for _, clientState := range node.State.CheckpointState.Clients {
			client := r.Clients[int(clientState.Id)]
			if client.Config.shouldSkip(nodeID) {
				continue
			}
			data := client.RequestByReqNo(clientState.LowWatermark)
			if data != nil {
				r.EventQueue.InsertClientProposal(nodeID, clientState.Id, clientState.LowWatermark, data, int64(runtimeParms.ProcessClientLatency))
			}
		}
	case event.MsgReceived != nil:
		if node.StateMachine == nil {
			// TODO, is this the best option? In many ways it would be better
			// to prevent the receive from going into the eventqueue
			break
		}
		node.WorkItems.ResultEvents().Step(event.MsgReceived.Source, event.MsgReceived.Msg)
	case event.ClientProposal != nil:
		prop := event.ClientProposal
		client := node.Clients.Client(prop.ClientID)
		reqNo, err := client.NextReqNo()
		if errors.Is(err, processor.ErrClientNotExist) {
			r.EventQueue.InsertClientProposal(nodeID, prop.ClientID, prop.ReqNo, prop.Data, int64(runtimeParms.ProcessClientLatency*100))
			break
		}

		if err != nil {
			return errors.WithMessage(err, "unanticipated client error")
		}

		tClient := r.Clients[int(prop.ClientID)]
		if tClient.Config.shouldSkip(nodeID) {
			return errors.Errorf("node %d was supposed to be skipped by client %d, but got event anyway", nodeID, prop.ClientID)
		}

		if reqNo != prop.ReqNo {
			data := tClient.RequestByReqNo(reqNo)
			if data != nil {
				r.EventQueue.InsertClientProposal(nodeID, prop.ClientID, reqNo, data, int64(runtimeParms.ProcessClientLatency))
			}
			break
		}

		events, err := client.Propose(prop.ReqNo, prop.Data)
		if err != nil {
			return errors.WithMessage(err, "unanticipated client propose error")
		}
		node.WorkItems.AddClientResults(events)

		data := tClient.RequestByReqNo(reqNo + 1)
		if data != nil {
			r.EventQueue.InsertClientProposal(nodeID, prop.ClientID, reqNo+1, data, int64(runtimeParms.ProcessClientLatency))
		}
	case event.Tick != nil:
		node.WorkItems.ResultEvents().TickElapsed()
		r.EventQueue.InsertTickEvent(nodeID, int64(runtimeParms.TickInterval))
	case event.ProcessReqStoreEvents != nil:
		node.WorkItems.AddReqStoreResults(event.ProcessReqStoreEvents)
		node.ProcessReqStoreEventsPending = false
	case event.ProcessResultEvents != nil:
		actions, err := processor.ProcessStateMachineEvents(node.StateMachine, node.Interceptor, event.ProcessResultEvents)
		if err != nil {
			return errors.WithMessage(err, "could not process state machine events")
		}
		node.WorkItems.AddStateMachineResults(actions)
		node.ProcessResultEventsPending = false
	case event.ProcessWALActions != nil:
		netActions, err := processor.ProcessWALActions(node.WAL, event.ProcessWALActions)
		if err != nil {
			return errors.WithMessage(err, "could not process WAL actions")
		}
		node.WorkItems.AddWALResults(netActions)
		node.ProcessWALActionsPending = false
	case event.ProcessNetActions != nil:
		netResults, err := processor.ProcessNetActions(nodeID, node.Link, event.ProcessNetActions)
		if err != nil {
			return errors.WithMessage(err, "could not process net actions")
		}
		node.WorkItems.AddNetResults(netResults)
		node.ProcessNetActionsPending = false
	case event.ProcessHashActions != nil:
		hashResults, err := processor.ProcessHashActions(node.Hasher, event.ProcessHashActions)
		if err != nil {
			return errors.WithMessage(err, "could not process hash actions")
		}
		node.WorkItems.AddHashResults(hashResults)
		node.ProcessHashActionsPending = false
	case event.ProcessClientActions != nil:
		clientResults, err := node.Clients.ProcessClientActions(event.ProcessClientActions)
		if err != nil {
			return errors.WithMessage(err, "could not process client actions")
		}
		node.WorkItems.AddClientResults(clientResults)
		node.ProcessClientActionsPending = false
	case event.ProcessAppActions != nil:
		appResults, err := processor.ProcessAppActions(node.State, event.ProcessAppActions)
		if err != nil {
			return errors.WithMessage(err, "could not process app actions")
		}
		node.WorkItems.AddAppResults(appResults)
		node.ProcessAppActionsPending = false
	default:
		return errors.Errorf("unknown event type")
	}

	if node.WorkItems == nil {
		return nil
	}

	if !node.ProcessWALActionsPending && node.WorkItems.WALActions().Len() > 0 {
		node.ProcessWALActionsPending = true
		r.EventQueue.InsertProcessWALActions(nodeID, node.WorkItems.WALActions(), int64(runtimeParms.ProcessWALLatency))
		node.WorkItems.ClearWALActions()
	}

	if !node.ProcessNetActionsPending && node.WorkItems.NetActions().Len() > 0 {
		node.ProcessNetActionsPending = true
		r.EventQueue.InsertProcessNetActions(nodeID, node.WorkItems.NetActions(), int64(runtimeParms.ProcessNetLatency))
		node.WorkItems.ClearNetActions()
	}

	if !node.ProcessClientActionsPending && node.WorkItems.ClientActions().Len() > 0 {
		node.ProcessClientActionsPending = true
		r.EventQueue.InsertProcessClientActions(nodeID, node.WorkItems.ClientActions(), int64(runtimeParms.ProcessClientLatency))
		node.WorkItems.ClearClientActions()
	}

	if !node.ProcessHashActionsPending && node.WorkItems.HashActions().Len() > 0 {
		node.ProcessHashActionsPending = true
		r.EventQueue.InsertProcessHashActions(nodeID, node.WorkItems.HashActions(), int64(runtimeParms.ProcessHashLatency))
		node.WorkItems.ClearHashActions()
	}

	if !node.ProcessAppActionsPending && node.WorkItems.AppActions().Len() > 0 {
		node.ProcessAppActionsPending = true
		r.EventQueue.InsertProcessAppActions(nodeID, node.WorkItems.AppActions(), int64(runtimeParms.ProcessAppLatency))
		node.WorkItems.ClearAppActions()
	}

	if !node.ProcessReqStoreEventsPending && node.WorkItems.ReqStoreEvents().Len() > 0 {
		node.ProcessReqStoreEventsPending = true
		r.EventQueue.InsertProcessReqStoreEvents(nodeID, node.WorkItems.ReqStoreEvents(), int64(runtimeParms.ProcessReqStoreLatency))
		node.WorkItems.ClearReqStoreEvents()
	}

	if !node.ProcessResultEventsPending && node.WorkItems.ResultEvents().Len() > 0 {
		events := node.WorkItems.ResultEvents()
		node.ProcessResultEventsPending = true
		r.EventQueue.InsertProcessResultEvents(nodeID, events, int64(runtimeParms.ProcessEventsLatency))
		node.WorkItems.ClearResultEvents()
	}

	return nil
}

// DrainClients will execute the recording until all client requests have committed.
// It will return with an error if the number of accumulated log entries exceeds timeout.
// If any step returns an error, this function returns that error.
func (r *Recording) DrainClients(timeout int) (count int, err error) {

	targetReqs := map[uint64]uint64{}
	for _, client := range r.Clients {
		targetReqs[client.Config.ID] = client.Config.Total
	}

	for {
		count++
		err := r.Step()
		if err != nil {
			return 0, err
		}

		allDone := true
	outer:
		for _, node := range r.Nodes {
			for _, client := range node.State.CheckpointState.Clients {
				if targetReqs[client.Id] != client.LowWatermark {
					allDone = false
					break outer
				}
			}
		}

		if allDone {
			return count, nil
		}

		if count > timeout {
			var errText string
			for _, node := range r.Nodes {
				for _, client := range node.State.CheckpointState.Clients {
					if targetReqs[client.Id] != client.LowWatermark {
						errText = fmt.Sprintf("(at least) node%d failed with client %d committing only through %d when expected %d", node.Config.InitParms.Id, client.Id, client.LowWatermark, targetReqs[client.Id])
					}
				}
			}
			return 0, errors.Errorf("timed out after %d entries:%s", count, errText)
		}
	}
}

type Spec struct {
	NodeCount     int
	ClientCount   int
	ReqsPerClient uint64
	BatchSize     uint32
	ClientsIgnore []uint64
	TweakRecorder func(r *Recorder)
}

func (s *Spec) Recorder() *Recorder {
	batchSize := uint32(1)
	if s.BatchSize != 0 {
		batchSize = s.BatchSize
	}

	var recorderNodeConfigs []*NodeConfig
	for i := 0; i < s.NodeCount; i++ {
		recorderNodeConfigs = append(recorderNodeConfigs, &NodeConfig{
			InitParms: &state.EventInitialParameters{
				Id:                   uint64(i),
				HeartbeatTicks:       2,
				SuspectTicks:         4,
				NewEpochTimeoutTicks: 8,
				BufferSize:           5 * 1024 * 1024,
				BatchSize:            batchSize,
			},
			RuntimeParms: &RuntimeParameters{
				TickInterval:           500,
				LinkLatency:            100,
				ProcessWALLatency:      100,
				ProcessNetLatency:      15,
				ProcessHashLatency:     25,
				ProcessClientLatency:   15,
				ProcessAppLatency:      30,
				ProcessReqStoreLatency: 150,
				ProcessEventsLatency:   10,
			},
		})
	}

	networkState := mirbft.StandardInitialNetworkState(s.NodeCount, s.ClientCount)

	clientConfigs := make([]*ClientConfig, s.ClientCount)
	for i, cl := range networkState.Clients {
		clientConfigs[i] = &ClientConfig{
			ID:          cl.Id,
			MaxInFlight: int(networkState.Config.CheckpointInterval / 2),
			Total:       s.ReqsPerClient,
			IgnoreNodes: s.ClientsIgnore,
		}
	}

	r := &Recorder{
		NetworkState:  networkState,
		NodeConfigs:   recorderNodeConfigs,
		LogOutput:     os.Stdout,
		Hasher:        crypto.SHA256,
		ClientConfigs: clientConfigs,
	}

	if s.TweakRecorder != nil {
		s.TweakRecorder(r)
	}

	return r
}
