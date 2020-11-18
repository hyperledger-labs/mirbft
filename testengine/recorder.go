/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math/rand"

	"github.com/IBM/mirbft"
	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Hasher func() hash.Hash

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type RecorderNodeConfig struct {
	InitParms    *pb.StateEvent_InitialParameters
	RuntimeParms *RuntimeParameters
}

type RuntimeParameters struct {
	TickInterval   int
	LinkLatency    int
	ReadyLatency   int
	ProcessLatency int
	PersistLatency int
	WALReadDelay   int
	ReqReadDelay   int
}

type ReqStore struct {
	ReqAcks   *list.List
	ReqAckMap map[*pb.RequestAck]*list.Element
}

func NewReqStore() *ReqStore {
	return &ReqStore{
		ReqAcks:   list.New(),
		ReqAckMap: map[*pb.RequestAck]*list.Element{},
	}
}

func (rs *ReqStore) Store(ack *pb.RequestAck, _ []byte) {
	el := rs.ReqAcks.PushBack(ack)
	rs.ReqAckMap[ack] = el
	// TODO, deal with free-ing
}

func (rs *ReqStore) Uncommitted(forEach func(*pb.RequestAck)) error {
	for el := rs.ReqAcks.Front(); el != nil; el = el.Next() {
		forEach(el.Value.(*pb.RequestAck))
	}
	return nil
}

type WAL struct {
	LowIndex uint64
	List     *list.List
}

func NewWAL(initialState *pb.NetworkState, initialCP []byte) *WAL {
	wal := &WAL{
		List:     list.New(),
		LowIndex: 1,
	}

	wal.List.PushBack(&pb.Persistent{
		Type: &pb.Persistent_CEntry{
			CEntry: &pb.CEntry{
				SeqNo:           0,
				CheckpointValue: []byte("fake-initial-value"),
				NetworkState:    initialState,
			},
		},
	})

	wal.List.PushBack(&pb.Persistent{
		Type: &pb.Persistent_FEntry{
			FEntry: &pb.FEntry{
				EndsEpochConfig: &pb.EpochConfig{
					Number:  0,
					Leaders: initialState.Config.Nodes,
				},
			},
		},
	})

	return wal
}

func (wal *WAL) Append(index uint64, p *pb.Persistent) {
	if index != wal.LowIndex+uint64(wal.List.Len()) {
		panic(fmt.Sprintf("WAL out of order: expect next index %d, but got %d", wal.LowIndex+uint64(wal.List.Len())+1, index))
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

	// fmt.Printf("JKY WAL -- truncating to %d, removing %d since LowIndex %d\n", index, toRemove, wal.LowIndex)
	for ; toRemove > 0; toRemove-- {
		wal.List.Remove(wal.List.Front())
		wal.LowIndex++
	}
}

func (wal *WAL) LoadAll(iter func(index uint64, p *pb.Persistent)) {
	i := uint64(0)
	for el := wal.List.Front(); el != nil; el = el.Next() {
		// fmt.Printf("JKY WAL -- loading %d\n", wal.LowIndex+i)
		iter(wal.LowIndex+i, el.Value.(*pb.Persistent))
		i++
	}
}

type RecorderNode struct {
	PlaybackNode         *PlaybackNode
	State                *NodeState
	WAL                  *WAL
	ReqStore             *ReqStore
	Config               *RecorderNodeConfig
	AwaitingProcessEvent bool
}

type RecorderClient struct {
	Config            *ClientConfig
	NextNodeReqNoSend []uint64
}

func (rc *RecorderClient) RequestByReqNo(reqNo uint64) *pb.Request {
	if reqNo > rc.Config.Total {
		// We've sent all we should
		return nil
	}

	var buffer bytes.Buffer
	buffer.Write(uint64ToBytes(rc.Config.ID))
	buffer.Write([]byte("-"))
	buffer.Write(uint64ToBytes(reqNo))

	return &pb.Request{
		ClientId: rc.Config.ID,
		ReqNo:    reqNo,
		Data:     buffer.Bytes(),
	}
}

type NodeState struct {
	Hasher                  hash.Hash
	Value                   []byte
	Length                  uint64
	ReconfigPoints          []*ReconfigPoint
	PendingReconfigurations []*pb.Reconfiguration
}

func (ns *NodeState) Commit(commits []*mirbft.Commit, node uint64) []*pb.CheckpointResult {
	var results []*pb.CheckpointResult
	for _, commit := range commits {
		if commit.Batch != nil {
			for _, request := range commit.Batch.Requests {
				ns.Hasher.Write(request.Digest)
				ns.Length++

				for _, reconfigPoint := range ns.ReconfigPoints {
					if reconfigPoint.ClientID == request.ClientId &&
						reconfigPoint.ReqNo == request.ReqNo {
						ns.PendingReconfigurations = append(ns.PendingReconfigurations, reconfigPoint.Reconfiguration)
					}
				}
			}

			continue
		}

		// We must have a checkpoint
		// XXX include the network config stuff into the value

		results = append(results, &pb.CheckpointResult{
			SeqNo: commit.Checkpoint.SeqNo,
			NetworkState: &pb.NetworkState{
				Config:  commit.Checkpoint.NetworkConfig,
				Clients: commit.Checkpoint.ClientsState,
			},
			Value: ns.Hasher.Sum(nil),
		})
	}

	ns.Value = ns.Hasher.Sum(nil)

	return results

}

type ClientConfig struct {
	ID          uint64
	TxLatency   uint64
	MaxInFlight int
	Total       uint64
}

type ReconfigPoint struct {
	ClientID        uint64
	ReqNo           uint64
	Reconfiguration *pb.Reconfiguration
}

type Recorder struct {
	NetworkState        *pb.NetworkState
	RecorderNodeConfigs []*RecorderNodeConfig
	ClientConfigs       []*ClientConfig
	ReconfigPoints      []*ReconfigPoint
	Mangler             Mangler
	Logger              *zap.Logger
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

	player, err := NewPlayer(eventLog, r.Logger)
	if err != nil {
		return nil, errors.WithMessage(err, "could not construct player")
	}

	nodes := make([]*RecorderNode, len(r.RecorderNodeConfigs))
	for i, recorderNodeConfig := range r.RecorderNodeConfigs {
		nodeID := uint64(i)

		wal := NewWAL(r.NetworkState, []byte("fake-initial-value"))

		eventLog.InsertStateEvent(
			nodeID,
			&pb.StateEvent{
				Type: &pb.StateEvent_Initialize{
					Initialize: recorderNodeConfig.InitParms,
				},
			},
			0,
		)

		nodes[i] = &RecorderNode{
			State: &NodeState{
				Hasher:         r.Hasher(),
				ReconfigPoints: r.ReconfigPoints,
			},
			WAL:          wal,
			ReqStore:     NewReqStore(),
			PlaybackNode: player.Node(uint64(i)),
			Config:       recorderNodeConfig,
		}
	}

	clients := make([]*RecorderClient, len(r.ClientConfigs))
	for i, clientConfig := range r.ClientConfigs {
		client := &RecorderClient{
			Config:            clientConfig,
			NextNodeReqNoSend: make([]uint64, len(nodes)),
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

type Mangler interface {
	Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent
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

	switch lastEvent.StateEvent.Type.(type) {
	case *pb.StateEvent_Tick:
		r.EventLog.InsertTickEvent(lastEvent.NodeId, int64(runtimeParms.TickInterval))
	case *pb.StateEvent_AddResults:
		nodeStatus := node.PlaybackNode.Status
		for _, rw := range nodeStatus.ClientWindows {
			for _, client := range r.Clients {
				if client.Config.ID != rw.ClientID {
					continue
				}

				for i := client.NextNodeReqNoSend[lastEvent.NodeId]; i <= rw.HighWatermark; i++ {
					req := client.RequestByReqNo(i)
					if req == nil {
						continue
					}
					r.EventLog.InsertProposeEvent(lastEvent.NodeId, req, int64(client.Config.TxLatency))
					client.NextNodeReqNoSend[lastEvent.NodeId] = i + 1
				}
			}
		}
	case *pb.StateEvent_Step:
	case *pb.StateEvent_Propose:
	case *pb.StateEvent_ActionsReceived:
		if !node.AwaitingProcessEvent {
			return errors.Errorf("node %d was not awaiting a processing message, but got one", lastEvent.NodeId)
		}
		node.AwaitingProcessEvent = false
		processing := playbackNode.Processing

		for _, req := range processing.StoreRequests {
			node.ReqStore.Store(req.RequestAck, req.RequestData)
		}

		for _, write := range processing.WriteAhead {
			switch {
			case write.Append != nil:
				node.WAL.Append(write.Append.Index, write.Append.Data)
			case write.Truncate != nil:
				node.WAL.Truncate(*write.Truncate)
			default:
				panic("Append or Truncate must be set")
			}
		}

		for _, send := range processing.Send {
			for _, i := range send.Targets {
				linkLatency := runtimeParms.LinkLatency
				if uint64(i) == lastEvent.NodeId {
					// There's no latency to send to ourselves
					linkLatency = 0
				}
				r.EventLog.InsertStepEvent(
					uint64(i),
					&pb.StateEvent_InboundMsg{
						Source: lastEvent.NodeId,
						Msg:    send.Msg,
					},
					int64(linkLatency+runtimeParms.PersistLatency),
				)
			}
		}

		apply := &pb.StateEvent_ActionResults{
			Digests: make([]*pb.HashResult, len(processing.Hash)),
		}

		for i, hashRequest := range processing.Hash {
			hasher := r.Hasher()
			for _, data := range hashRequest.Data {
				hasher.Write(data)
			}

			apply.Digests[i] = &pb.HashResult{
				Digest: hasher.Sum(nil),
				Type:   hashRequest.Origin.Type,
			}
		}

		apply.Checkpoints = nodeState.Commit(processing.Commits, lastEvent.NodeId)

		r.EventLog.InsertStateEvent(
			lastEvent.NodeId,
			&pb.StateEvent{
				Type: &pb.StateEvent_AddResults{
					AddResults: apply,
				},
			},
			int64(runtimeParms.ReadyLatency),
		)
	case *pb.StateEvent_Initialize:
		// If this is an Initialize, it is either the first event for the node,
		// and nothing else is in the log, or, it is a restart.  In the case of
		// a restart, we clear any outstanding events associated to this NodeID from
		// the log.  This is especially important for things like pending actions,
		// but also makes sense for things like in flight messages.
		el := r.EventLog.List.Front()
		for el != nil {
			x := el
			el = el.Next()
			if x.Value.(*rpb.RecordedEvent).NodeId == lastEvent.NodeId {
				// fmt.Printf("JKY: removing %T\n", r.EventLog.List.Remove(x).(*rpb.RecordedEvent).StateEvent.Type)
				r.EventLog.List.Remove(x)
			}
		}

		delay := int64(0)

		node.WAL.LoadAll(func(index uint64, p *pb.Persistent) {
			delay += int64(runtimeParms.WALReadDelay)
			r.EventLog.InsertStateEvent(
				lastEvent.NodeId,
				&pb.StateEvent{
					Type: &pb.StateEvent_LoadEntry{
						LoadEntry: &pb.StateEvent_PersistedEntry{
							Index: index,
							Data:  p,
						},
					},
				},
				delay,
			)
		})

		node.ReqStore.Uncommitted(func(ack *pb.RequestAck) {
			delay += int64(runtimeParms.ReqReadDelay)
			r.EventLog.InsertStateEvent(
				lastEvent.NodeId,
				&pb.StateEvent{
					Type: &pb.StateEvent_LoadRequest{
						LoadRequest: &pb.StateEvent_OutstandingRequest{
							RequestAck: ack,
						},
					},
				},
				delay,
			)
		})

		r.EventLog.InsertStateEvent(
			lastEvent.NodeId,
			&pb.StateEvent{
				Type: &pb.StateEvent_CompleteInitialization{
					CompleteInitialization: &pb.StateEvent_LoadCompleted{},
				},
			},
			delay,
		)
	case *pb.StateEvent_LoadEntry:
	case *pb.StateEvent_LoadRequest:
	case *pb.StateEvent_CompleteInitialization:
		r.EventLog.InsertTickEvent(lastEvent.NodeId, int64(runtimeParms.TickInterval))
	default:
		panic(fmt.Sprintf("unhandled state event type: %T", lastEvent.StateEvent.Type))
	}

	if playbackNode.Processing == nil &&
		!isEmpty(playbackNode.Actions) &&
		!node.AwaitingProcessEvent {
		r.EventLog.InsertProcess(lastEvent.NodeId, int64(runtimeParms.ProcessLatency))
		node.AwaitingProcessEvent = true
	}

	return nil
}

func isEmpty(actions *mirbft.Actions) bool {
	return len(actions.Send) == 0 &&
		len(actions.WriteAhead) == 0 &&
		len(actions.Hash) == 0 &&
		len(actions.StoreRequests) == 0 &&
		len(actions.ForwardRequests) == 0 &&
		len(actions.Commits) == 0
}

// DrainClients will execute the recording until all client requests have committed.
// It will return with an error if the number of accumulated log entries exceeds timeout.
// If any step returns an error, this function returns that error.
func (r *Recording) DrainClients(timeout int) (count int, err error) {

	totalReqs := uint64(0)
	for _, client := range r.Clients {
		totalReqs += client.Config.Total
	}

	for {
		count++
		err := r.Step()
		if err != nil {
			return 0, err
		}

		allDone := true
		for _, node := range r.Nodes {
			if node.State.Length < totalReqs {
				allDone = false
				break
			}
		}

		if allDone {
			return count, nil
		}

		if count > timeout {
			return 0, errors.Errorf("timed out after %d entries", count)
		}
	}
}

func BasicRecorder(nodeCount, clientCount int, reqsPerClient uint64) *Recorder {
	var recorderNodeConfigs []*RecorderNodeConfig
	for i := 0; i < nodeCount; i++ {
		recorderNodeConfigs = append(recorderNodeConfigs, &RecorderNodeConfig{
			InitParms: &pb.StateEvent_InitialParameters{
				Id:                   uint64(i),
				HeartbeatTicks:       2,
				SuspectTicks:         4,
				NewEpochTimeoutTicks: 8,
				BufferSize:           5 * 1024 * 1024,
				BatchSize:            1,
			},
			RuntimeParms: &RuntimeParameters{
				TickInterval:   500,
				LinkLatency:    100,
				ReadyLatency:   50,
				ProcessLatency: 10,
				PersistLatency: 10,
			},
		})
	}

	clientIDs := make([]uint64, clientCount)
	for i := 0; i < clientCount; i++ {
		clientIDs[i] = uint64(i)
	}

	networkState := mirbft.StandardInitialNetworkState(nodeCount, clientIDs...)

	clientConfigs := make([]*ClientConfig, clientCount)
	for i := 0; i < clientCount; i++ {
		clientConfigs[i] = &ClientConfig{
			ID:          clientIDs[i],
			MaxInFlight: int(networkState.Config.CheckpointInterval / 2),
			Total:       reqsPerClient,
			TxLatency:   10,
		}
		clientIDs[i] = clientConfigs[i].ID
	}

	logger, err := zap.NewProduction()
	// logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return &Recorder{
		NetworkState:        networkState,
		RecorderNodeConfigs: recorderNodeConfigs,
		Logger:              logger,
		Hasher:              sha256.New,
		ClientConfigs:       clientConfigs,
	}
}
