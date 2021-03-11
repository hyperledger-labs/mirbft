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

	"github.com/IBM/mirbft"
	"github.com/IBM/mirbft/pkg/eventlog"
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/recording"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/processor"
	"github.com/IBM/mirbft/pkg/statemachine"
)

type Hasher interface {
	New() hash.Hash
}

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type Link struct {
	Source   uint64
	EventLog *EventLog
	Delay    int64
}

func (l *Link) Send(dest uint64, msg *msgs.Msg) {
	l.EventLog.InsertMsgReceived(dest, l.Source, msg, l.Delay)
}

type NodeConfig struct {
	InitParms    *state.EventInitialParameters
	RuntimeParms *RuntimeParameters
}

type RuntimeParameters struct {
	TickInterval         int
	LinkLatency          int
	ReadyLatency         int
	ProcessLatency       int
	ClientProcessLatency int
	PersistLatency       int
	WALReadDelay         int
	ReqReadDelay         int
	StateTransferLatency int
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
func (wal *WAL) LoadAll(iter func(index uint64, p *msgs.Persistent)) {
	i := uint64(0)
	for el := wal.List.Front(); el != nil; el = el.Next() {
		iter(wal.LowIndex+i, el.Value.(*msgs.Persistent))
		i++
	}
}

func (wal *WAL) Sync() error {
	return nil
}

type Node struct {
	ID                    uint64
	Config                *NodeConfig
	WAL                   *WAL
	Link                  *Link
	Hasher                Hasher
	ReqStore              *ReqStore
	State                 *NodeState
	Processor             *processor.Processor
	PendingStateEvents    *statemachine.EventList
	PendingStateActions   *statemachine.ActionList
	ProcessEventsPending  bool
	ProcessActionsPending bool
	StateMachine          *statemachine.StateMachine
}

func (n *Node) Initialize(initParms *state.EventInitialParameters, logger statemachine.Logger) (*msgs.NetworkState, error) {
	nodeID := n.Config.InitParms.Id

	n.Processor = &processor.Processor{
		NodeID:       nodeID,
		Link:         n.Link,
		Hasher:       n.Hasher,
		App:          n.State,
		WAL:          n.WAL,
		RequestStore: n.ReqStore,
	}

	n.StateMachine = &statemachine.StateMachine{
		Logger: logger,
	}
	n.PendingStateActions = &statemachine.ActionList{}
	n.ProcessActionsPending = false
	n.PendingStateEvents = &statemachine.EventList{}
	n.ProcessEventsPending = false
	n.PendingStateEvents.Initialize(initParms)

	var maxCEntry *msgs.CEntry

	n.WAL.LoadAll(func(index uint64, p *msgs.Persistent) {
		n.PendingStateEvents.LoadPersistedEntry(index, p)

		if cEntryT, ok := p.Type.(*msgs.Persistent_CEntry); ok {
			maxCEntry = cEntryT.CEntry
		}
	})

	if maxCEntry.SeqNo != n.State.CheckpointSeqNo {
		return nil, errors.Errorf("expected last CEntry in the WAL to match our state")
	}

	if _, err := n.State.TransferTo(maxCEntry.SeqNo, maxCEntry.CheckpointValue); err != nil {
		return nil, errors.WithMessage(err, "error in mock transfer to intiialize")
	}

	n.PendingStateEvents.CompleteInitialization()

	return maxCEntry.NetworkState, nil
}

type RecorderClient struct {
	Config *ClientConfig
	Hasher Hasher
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
	Hasher                  Hasher
	ActiveHash              hash.Hash
	LastSeqNo               uint64
	ReconfigPoints          []*ReconfigPoint
	PendingReconfigurations []*msgs.Reconfiguration
	ReqStore                *ReqStore
	CheckpointSeqNo         uint64
	CheckpointHash          []byte
	CheckpointState         *msgs.NetworkState
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
	Hasher         Hasher
	RandomSeed     int64
}

func (r *Recorder) Recording(output *gzip.Writer) (*Recording, error) {
	eventLog := &EventLog{
		List: list.New(),
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
				EventLog: eventLog,
				Source:   nodeID,
				Delay:    int64(recorderNodeConfig.RuntimeParms.LinkLatency),
			},
			Config:             recorderNodeConfig,
			PendingStateEvents: &statemachine.EventList{},
		}

		eventLog.InsertInitialize(nodeID, recorderNodeConfig.InitParms, 0)
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
		Hasher:   r.Hasher,
		EventLog: eventLog,
		Nodes:    nodes,
		Clients:  clients,
		Rand:     rand.New(rand.NewSource(r.RandomSeed)),
		// TODO incorporate these
		EventLogOutput: output,
		// Mangler: r.Mangler,
		LogOutput: r.LogOutput,
	}, nil
}

type Recording struct {
	Hasher         Hasher
	EventLog       *EventLog
	Nodes          []*Node
	Clients        []*RecorderClient
	Rand           *rand.Rand
	LogOutput      io.Writer
	EventLogOutput *gzip.Writer
}

func (r *Recording) Step() error {
	if r.EventLog.List.Len() == 0 {
		return errors.Errorf("event log is empty, nothing to do")
	}

	event := r.EventLog.ConsumeEvent()
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
		el := r.EventLog.List.Front()
		for el != nil {
			x := el
			el = el.Next()
			if x.Value.(*Event).Target == nodeID {
				r.EventLog.List.Remove(x)
			}
		}

		networkState, err := node.Initialize(
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

		r.EventLog.InsertTickEvent(nodeID, int64(runtimeParms.TickInterval))

		for _, clientState := range networkState.Clients {
			client := r.Clients[int(clientState.Id)]
			if client.Config.shouldSkip(nodeID) {
				continue
			}
			data := client.RequestByReqNo(clientState.LowWatermark)
			if data != nil {
				r.EventLog.InsertClientProposal(nodeID, clientState.Id, clientState.LowWatermark, data, int64(runtimeParms.ClientProcessLatency))
			}
		}
	case event.MsgReceived != nil:
		node.PendingStateEvents.Step(event.MsgReceived.Source, event.MsgReceived.Msg)
	case event.ClientProposal != nil:
		prop := event.ClientProposal
		client := node.Processor.Client(prop.ClientID)
		reqNo, err := client.NextReqNo()
		if errors.Is(err, processor.ErrClientNotExist) {
			r.EventLog.InsertClientProposal(nodeID, prop.ClientID, prop.ReqNo, prop.Data, int64(runtimeParms.ClientProcessLatency*100))
			break
		}

		if err != nil {
			return errors.WithMessage(err, "unanticipated client error")
		}

		tClient := r.Clients[int(prop.ClientID)]

		if reqNo != prop.ReqNo {
			data := tClient.RequestByReqNo(reqNo)
			if data != nil {
				r.EventLog.InsertClientProposal(nodeID, prop.ClientID, reqNo, data, int64(runtimeParms.ClientProcessLatency))
			}
			break
		}

		err = client.Propose(prop.ReqNo, prop.Data)
		if err != nil {
			return errors.WithMessage(err, "unanticipated client propose error")
		}

		data := tClient.RequestByReqNo(reqNo + 1)
		if data != nil {
			r.EventLog.InsertClientProposal(nodeID, prop.ClientID, reqNo+1, data, int64(runtimeParms.ClientProcessLatency))
		}
	case event.Tick != nil:
		node.PendingStateEvents.TickElapsed()
		r.EventLog.InsertTickEvent(nodeID, int64(runtimeParms.TickInterval))
	case event.ProcessEvents != nil:
		node.PendingStateEvents.ActionsReceived()
		iter := node.PendingStateEvents.Iterator()
		for stateEvent := iter.Next(); stateEvent != nil; stateEvent = iter.Next() {
			if r.EventLogOutput != nil {
				err := eventlog.WriteRecordedEvent(r.EventLogOutput, &recording.Event{
					NodeId:     nodeID,
					Time:       event.Time,
					StateEvent: stateEvent,
				})
				if err != nil {
					return errors.WithMessage(err, "could not write event before processing")
				}
			}

			// TODO, persist to the log first
			node.PendingStateActions.PushBackList(node.StateMachine.ApplyEvent(stateEvent))
		}
		node.PendingStateEvents = &statemachine.EventList{}
		node.ProcessEventsPending = false
	case event.ProcessActions != nil:
		events, err := node.Processor.Process(node.PendingStateActions)
		if err != nil {
			return errors.WithMessage(err, "error during processing")
		}
		node.PendingStateEvents.PushBackList(events)
		node.PendingStateActions = &statemachine.ActionList{}
		node.ProcessActionsPending = false
	default:
		return errors.Errorf("unknown event type")
	}

	cEvents := node.Processor.ClientWork.Results()
	if cEvents != nil && cEvents.Len() > 0 {
		node.PendingStateEvents.PushBackList(cEvents)
	}

	if node.PendingStateEvents.Len() > 0 && !node.ProcessEventsPending {
		r.EventLog.InsertProcessEvents(nodeID, int64(runtimeParms.ProcessLatency))
		node.ProcessEventsPending = true
	}

	// TODO ProcessLatency is a cludge, fix it in the processor

	if node.PendingStateActions.Len() > 0 && !node.ProcessActionsPending {
		r.EventLog.InsertProcessActions(nodeID, int64(runtimeParms.ProcessLatency))
		node.ProcessActionsPending = true
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

func BasicRecorder(nodeCount, clientCount int, reqsPerClient uint64) *Recorder {
	var recorderNodeConfigs []*NodeConfig
	for i := 0; i < nodeCount; i++ {
		recorderNodeConfigs = append(recorderNodeConfigs, &NodeConfig{
			InitParms: &state.EventInitialParameters{
				Id:                   uint64(i),
				HeartbeatTicks:       2,
				SuspectTicks:         4,
				NewEpochTimeoutTicks: 8,
				BufferSize:           5 * 1024 * 1024,
				BatchSize:            1,
			},
			RuntimeParms: &RuntimeParameters{
				TickInterval:         500,
				LinkLatency:          100,
				ReadyLatency:         50,
				ProcessLatency:       10,
				ClientProcessLatency: 10,
				PersistLatency:       10,
				StateTransferLatency: 800,
			},
		})
	}

	networkState := mirbft.StandardInitialNetworkState(nodeCount, clientCount)

	clientConfigs := make([]*ClientConfig, clientCount)
	for i, cl := range networkState.Clients {
		clientConfigs[i] = &ClientConfig{
			ID:          cl.Id,
			MaxInFlight: int(networkState.Config.CheckpointInterval / 2),
			Total:       reqsPerClient,
		}
	}

	return &Recorder{
		NetworkState:  networkState,
		NodeConfigs:   recorderNodeConfigs,
		LogOutput:     os.Stdout,
		Hasher:        crypto.SHA256,
		ClientConfigs: clientConfigs,
	}
}
