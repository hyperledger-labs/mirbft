/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"compress/gzip"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"

	"github.com/IBM/mirbft"
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/recording"
	"github.com/IBM/mirbft/pkg/pb/state"

	"github.com/pkg/errors"
)

type Hasher func() hash.Hash

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type RecorderNodeConfig struct {
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
	storedAcks  map[ackHelper]struct{}
	correctAcks map[ackHelper]struct{}
}

func NewReqStore() *ReqStore {
	return &ReqStore{
		storedAcks:  map[ackHelper]struct{}{},
		correctAcks: map[ackHelper]struct{}{},
	}
}

func (rs *ReqStore) Store(ack *msgs.RequestAck) {
	helper := newAckHelper(ack)
	rs.storedAcks[helper] = struct{}{}
	// TODO, deal with free-ing
}

func (rs *ReqStore) Has(ack *msgs.RequestAck) bool {
	helper := newAckHelper(ack)
	_, ok := rs.storedAcks[helper]
	return ok
}

func (rs *ReqStore) StoreCorrect(ack *msgs.RequestAck) {
	helper := newAckHelper(ack)
	rs.correctAcks[helper] = struct{}{}
}

func (rs *ReqStore) HasCorrect(ack *msgs.RequestAck) bool {
	helper := newAckHelper(ack)
	_, ok := rs.correctAcks[helper]
	return ok
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
				CheckpointValue: []byte("fake-initial-value"),
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

func (wal *WAL) Append(index uint64, p *msgs.Persistent) {
	if index != wal.LowIndex+uint64(wal.List.Len()) {
		panic(fmt.Sprintf("WAL out of order: expect next index %d, but got %d", wal.LowIndex+uint64(wal.List.Len()), index))
	}

	wal.List.PushBack(p)
}

func (wal *WAL) Truncate(index uint64) {
	if index < wal.LowIndex {
		panic(fmt.Sprintf("asked to truncated to index %d, but lowIndex is %d", index, wal.LowIndex))
	}

	toRemove := int(index - wal.LowIndex)
	if toRemove >= wal.List.Len() {
		panic(fmt.Sprintf("asked to truncate to index %d, but highest index is %d", index, wal.LowIndex+uint64(wal.List.Len())))
	}

	for ; toRemove > 0; toRemove-- {
		wal.List.Remove(wal.List.Front())
		wal.LowIndex++
	}
}

func (wal *WAL) LoadAll(iter func(index uint64, p *msgs.Persistent)) {
	i := uint64(0)
	for el := wal.List.Front(); el != nil; el = el.Next() {
		iter(wal.LowIndex+i, el.Value.(*msgs.Persistent))
		i++
	}
}

type RecorderNode struct {
	PlaybackNode               *PlaybackNode
	State                      *NodeState
	WAL                        *WAL
	ReqStore                   *ReqStore
	Config                     *RecorderNodeConfig
	AwaitingProcessEvent       bool
	AwaitingClientProcessEvent bool
}

type RecorderClient struct {
	Config *ClientConfig
	Hasher Hasher
}

func (rc *RecorderClient) RequestByReqNo(reqNo uint64) *msgs.RequestAck {
	if reqNo >= rc.Config.Total {
		// We've sent all we should
		return nil
	}

	h := rc.Hasher()
	h.Write(uint64ToBytes(rc.Config.ID))
	h.Write([]byte("-"))
	h.Write(uint64ToBytes(reqNo))

	return &msgs.RequestAck{
		ClientId: rc.Config.ID,
		ReqNo:    reqNo,
		Digest:   h.Sum(nil),
	}
}

type NodeState struct {
	Hasher                  Hasher
	ActiveHash              hash.Hash
	LastSeqNo               uint64
	ReconfigPoints          []*ReconfigPoint
	PendingReconfigurations []*msgs.Reconfiguration
	Checkpoints             *list.List
	CheckpointsBySeqNo      map[uint64]*list.Element
	ReqStore                *ReqStore
}

func (ns *NodeState) Set(seqNo uint64, value []byte, networkState *msgs.NetworkState) *state.CheckpointResult {
	ns.ActiveHash = ns.Hasher()

	checkpoint := &state.CheckpointResult{
		SeqNo:        seqNo,
		NetworkState: networkState,
		Value:        value,
	}

	ns.LastSeqNo = seqNo

	el := ns.Checkpoints.PushBack(checkpoint)
	ns.CheckpointsBySeqNo[seqNo] = el
	if ns.Checkpoints.Len() > 100 {
		// prevent the size of the storage from growing indefinitely,
		// keep only the last 100 checkpoints.
		del := ns.Checkpoints.Remove(ns.Checkpoints.Front())
		delete(ns.CheckpointsBySeqNo, del.(*state.CheckpointResult).SeqNo)
	}

	return checkpoint
}

func (ns *NodeState) LastCheckpoint() *state.CheckpointResult {
	return ns.Checkpoints.Back().Value.(*state.CheckpointResult)
}

func (ns *NodeState) Commit(commit *state.ActionCommit) {
	ns.LastSeqNo++
	if commit.Batch.SeqNo != ns.LastSeqNo {
		panic(fmt.Sprintf("unexpected out of order commit sequence number, expected %d, got %d", ns.LastSeqNo, commit.Batch.SeqNo))
	}

	for _, request := range commit.Batch.Requests {
		if !ns.ReqStore.Has(request) {
			panic("reqstore should have request if we are committing it")
		}

		ns.ActiveHash.Write(request.Digest)

		for _, reconfigPoint := range ns.ReconfigPoints {
			if reconfigPoint.ClientID == request.ClientId &&
				reconfigPoint.ReqNo == request.ReqNo {
				ns.PendingReconfigurations = append(ns.PendingReconfigurations, reconfigPoint.Reconfiguration)
			}
		}
	}
}
func (ns *NodeState) Checkpoint(checkpoint *state.ActionCheckpoint) *state.CheckpointResult {
	// We must have a checkpoint

	if checkpoint.SeqNo != ns.LastSeqNo {
		panic("asked to checkpoint for uncommitted sequence")
	}

	return ns.Set(
		checkpoint.SeqNo,
		ns.ActiveHash.Sum(nil),
		&msgs.NetworkState{
			Config:  checkpoint.NetworkConfig,
			Clients: checkpoint.ClientStates,
		},
	)
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
	NetworkState        *msgs.NetworkState
	RecorderNodeConfigs []*RecorderNodeConfig
	ClientConfigs       []*ClientConfig
	ReconfigPoints      []*ReconfigPoint
	Mangler             Mangler
	LogOutput           io.Writer
	Hasher              Hasher
	RandomSeed          int64
}

func (r *Recorder) Recording(output *gzip.Writer) (*Recording, error) {
	eventLog := &EventLog{
		List:    list.New(),
		Output:  output,
		Mangler: r.Mangler,
		Rand:    rand.New(rand.NewSource(r.RandomSeed)),
	}

	player, err := NewPlayer(eventLog, r.LogOutput)
	if err != nil {
		return nil, errors.WithMessage(err, "could not construct player")
	}

	nodes := make([]*RecorderNode, len(r.RecorderNodeConfigs))
	for i, recorderNodeConfig := range r.RecorderNodeConfigs {
		nodeID := uint64(i)

		checkpointValue := []byte("fake-initial-value")

		wal := NewWAL(r.NetworkState, checkpointValue)

		eventLog.InsertStateEvent(
			nodeID,
			&state.Event{
				Type: &state.Event_Initialize{
					Initialize: recorderNodeConfig.InitParms,
				},
			},
			0,
		)

		reqStore := NewReqStore()

		nodeState := &NodeState{
			Hasher:             r.Hasher,
			ReconfigPoints:     r.ReconfigPoints,
			Checkpoints:        list.New(),
			CheckpointsBySeqNo: map[uint64]*list.Element{},
			ReqStore:           reqStore,
		}

		nodes[i] = &RecorderNode{
			State:        nodeState,
			WAL:          wal,
			ReqStore:     reqStore,
			PlaybackNode: player.Node(uint64(i)),
			Config:       recorderNodeConfig,
		}
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
		Player:   player,
		Nodes:    nodes,
		Clients:  clients,
	}, nil
}

type Recording struct {
	Hasher   Hasher
	EventLog *EventLog
	Player   *Player
	Nodes    []*RecorderNode
	Clients  []*RecorderClient
}

func (r *Recording) Step() error {
	if r.EventLog.List.Len() == 0 {
		return errors.Errorf("event log is empty, nothing to do")
	}

	err := r.Player.Step()
	if err != nil {
		return errors.WithMessagef(err, "could not step recorder's underlying player")
	}

	lastEvent := r.Player.LastEvent

	node := r.Nodes[int(lastEvent.NodeId)]
	runtimeParms := node.Config.RuntimeParms
	playbackNode := node.PlaybackNode
	nodeState := node.State

	switch stateEvent := lastEvent.StateEvent.Type.(type) {
	case *state.Event_Tick:
		r.EventLog.InsertTickEvent(lastEvent.NodeId, int64(runtimeParms.TickInterval))
	case *state.Event_AddResults:
	case *state.Event_AddClientResults:
		for _, req := range stateEvent.AddClientResults.Persisted {
			node.ReqStore.Store(req)
		}
	case *state.Event_Step:
	case *state.Event_ActionsReceived:
		if !node.AwaitingProcessEvent {
			return errors.Errorf("node %d was not awaiting a processing message, but got one", lastEvent.NodeId)
		}
		node.AwaitingProcessEvent = false
		processing := playbackNode.Processing

		clientActionResults := &state.EventClientActionResults{}
		apply := &state.EventActionResults{}

		iter := processing.Iterator()
		for action := iter.Next(); action != nil; action = iter.Next() {
			switch t := action.Type.(type) {
			case *state.Action_Send:
				send := t.Send
				for _, i := range send.Targets {
					linkLatency := runtimeParms.LinkLatency
					if i == lastEvent.NodeId {
						// There's no latency to send to ourselves
						linkLatency = 0
					}
					if n := r.Player.Node(i); n.StateMachine == nil {
						continue
					}
					r.EventLog.InsertStepEvent(
						i,
						&state.EventInboundMsg{
							Source: lastEvent.NodeId,
							Msg:    send.Msg,
						},
						int64(linkLatency+runtimeParms.PersistLatency),
					)
				}
			case *state.Action_Hash:
				hashRequest := t.Hash
				hasher := r.Hasher()
				for _, data := range hashRequest.Data {
					hasher.Write(data)
				}

				apply.Digests = append(apply.Digests, &state.HashResult{
					Digest: hasher.Sum(nil),
					Type:   hashRequest.Origin.Type,
				})
			case *state.Action_TruncateWriteAhead:
				node.WAL.Truncate(t.TruncateWriteAhead.Index)
			case *state.Action_AppendWriteAhead:
				node.WAL.Append(t.AppendWriteAhead.Index, t.AppendWriteAhead.Data)
			case *state.Action_AllocatedRequest:
				reqSlot := t.AllocatedRequest
				client := r.Clients[int(reqSlot.ClientId)]
				if client.Config.ID != reqSlot.ClientId {
					panic("sanity check")
				}

				if client.Config.shouldSkip(lastEvent.NodeId) {
					continue
				}

				req := client.RequestByReqNo(reqSlot.ReqNo)
				if req == nil {
					continue
				}

				clientActionResults.Persisted = append(clientActionResults.Persisted, req)
			case *state.Action_ForwardRequest:
				forward := t.ForwardRequest
				if !node.ReqStore.Has(forward.Ack) {
					panic("we asked ourselves to forward an ack we do not have")
				}

				for _, dNodeID := range forward.Targets {

					dNode := r.Nodes[int(dNodeID)]
					if dNode.ReqStore.Has(forward.Ack) {
						continue
					}

					if !dNode.ReqStore.HasCorrect(forward.Ack) {
						// TODO, this is a bit crude, it assumes that
						// if the request is not correct at this moment,
						// there is no buffering, and that it will not become
						// correct in the future
						continue
					}

					r.EventLog.InsertStateEvent(
						dNodeID,
						&state.Event{
							Type: &state.Event_AddClientResults{
								AddClientResults: &state.EventClientActionResults{
									Persisted: []*msgs.RequestAck{forward.Ack},
								},
							},
						},
						int64(runtimeParms.LinkLatency),
					)
				}
			case *state.Action_CorrectRequest:
				node.ReqStore.StoreCorrect(t.CorrectRequest)
			case *state.Action_Commit:
				nodeState.Commit(t.Commit)
			case *state.Action_Checkpoint:
				apply.Checkpoints = append(apply.Checkpoints, nodeState.Checkpoint(t.Checkpoint))
			case *state.Action_StateTransfer:
				var networkState *msgs.NetworkState
				for _, node := range r.Nodes {
					el, ok := node.State.CheckpointsBySeqNo[t.StateTransfer.SeqNo]
					if !ok {
						// if no node has the state, networkState will be nil
						// which signals to the state machine to try another target
						continue
					}

					networkState = el.Value.(*state.CheckpointResult).NetworkState
					break
				}
				r.EventLog.InsertStateEvent(
					lastEvent.NodeId,
					&state.Event{
						Type: &state.Event_Transfer{
							Transfer: &msgs.CEntry{
								SeqNo:           t.StateTransfer.SeqNo,
								CheckpointValue: t.StateTransfer.Value,
								NetworkState:    networkState,
							},
						},
					},
					int64(runtimeParms.StateTransferLatency),
				)
			default:
				panic(fmt.Sprintf("unhandled type: %T", t))
			}
		}

		r.EventLog.InsertStateEvent(
			lastEvent.NodeId,
			&state.Event{
				Type: &state.Event_AddClientResults{
					AddClientResults: clientActionResults,
				},
			},
			0, // TODO, maybe have some additional reqstore latency here?
		)

		r.EventLog.InsertStateEvent(
			lastEvent.NodeId,
			&state.Event{
				Type: &state.Event_AddResults{
					AddResults: apply,
				},
			},
			int64(runtimeParms.ReadyLatency),
		)
	case *state.Event_Initialize:
		// If this is an Initialize, it is either the first event for the node,
		// and nothing else is in the log, or, it is a restart.  In the case of
		// a restart, we clear any outstanding events associated to this NodeID from
		// the log.  This is especially important for things like pending actions,
		// but also makes sense for things like in flight messages.
		el := r.EventLog.List.Front()
		for el != nil {
			x := el
			el = el.Next()
			if x.Value.(*recording.Event).NodeId == lastEvent.NodeId {
				r.EventLog.List.Remove(x)
			}
		}

		delay := int64(0)

		var maxCEntry *msgs.CEntry

		node.WAL.LoadAll(func(index uint64, p *msgs.Persistent) {
			delay += int64(runtimeParms.WALReadDelay)
			r.EventLog.InsertStateEvent(
				lastEvent.NodeId,
				&state.Event{
					Type: &state.Event_LoadEntry{
						LoadEntry: &state.EventPersistedEntry{
							Index: index,
							Data:  p,
						},
					},
				},
				delay,
			)

			if cEntryT, ok := p.Type.(*msgs.Persistent_CEntry); ok {
				maxCEntry = cEntryT.CEntry
			}
		})

		nodeState.Set(maxCEntry.SeqNo, maxCEntry.CheckpointValue, maxCEntry.NetworkState)

		r.EventLog.InsertStateEvent(
			lastEvent.NodeId,
			&state.Event{
				Type: &state.Event_CompleteInitialization{
					CompleteInitialization: &state.EventLoadCompleted{},
				},
			},
			delay,
		)
	case *state.Event_LoadEntry:
	case *state.Event_Transfer:
		node.State.Set(stateEvent.Transfer.SeqNo, stateEvent.Transfer.CheckpointValue, stateEvent.Transfer.NetworkState)
	case *state.Event_CompleteInitialization:
		r.EventLog.InsertTickEvent(lastEvent.NodeId, int64(runtimeParms.TickInterval))
	default:
		panic(fmt.Sprintf("unhandled state event type: %T", lastEvent.StateEvent.Type))
	}

	if playbackNode.Processing == nil &&
		playbackNode.Actions.Len() != 0 &&
		!node.AwaitingProcessEvent {
		r.EventLog.InsertProcess(lastEvent.NodeId, int64(runtimeParms.ProcessLatency))
		node.AwaitingProcessEvent = true
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
			for _, client := range node.State.LastCheckpoint().NetworkState.Clients {
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
				for _, client := range node.State.LastCheckpoint().NetworkState.Clients {
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
	var recorderNodeConfigs []*RecorderNodeConfig
	for i := 0; i < nodeCount; i++ {
		recorderNodeConfigs = append(recorderNodeConfigs, &RecorderNodeConfig{
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
		NetworkState:        networkState,
		RecorderNodeConfigs: recorderNodeConfigs,
		LogOutput:           os.Stdout,
		Hasher:              sha256.New,
		ClientConfigs:       clientConfigs,
	}
}
