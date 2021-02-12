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
	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/pkg/eventlog/recorderpb"

	"github.com/pkg/errors"
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
	TickInterval         int
	LinkLatency          int
	ReadyLatency         int
	ProcessLatency       int
	PersistLatency       int
	WALReadDelay         int
	ReqReadDelay         int
	StateTransferLatency int
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

func (wal *WAL) LoadAll(iter func(index uint64, p *pb.Persistent)) {
	i := uint64(0)
	for el := wal.List.Front(); el != nil; el = el.Next() {
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
	Config *ClientConfig
	Hasher Hasher
}

func (rc *RecorderClient) RequestByReqNo(reqNo uint64) *pb.RequestAck {
	if reqNo >= rc.Config.Total {
		// We've sent all we should
		return nil
	}

	h := rc.Hasher()
	h.Write(uint64ToBytes(rc.Config.ID))
	h.Write([]byte("-"))
	h.Write(uint64ToBytes(reqNo))

	return &pb.RequestAck{
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
	PendingReconfigurations []*pb.Reconfiguration
	Checkpoints             *list.List
	CheckpointsBySeqNo      map[uint64]*list.Element
}

func (ns *NodeState) Set(seqNo uint64, value []byte, networkState *pb.NetworkState) *pb.CheckpointResult {
	ns.ActiveHash = ns.Hasher()

	checkpoint := &pb.CheckpointResult{
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
		delete(ns.CheckpointsBySeqNo, del.(*pb.CheckpointResult).SeqNo)
	}

	return checkpoint
}

func (ns *NodeState) LastCheckpoint() *pb.CheckpointResult {
	return ns.Checkpoints.Back().Value.(*pb.CheckpointResult)
}

func (ns *NodeState) Commit(commits []*pb.StateEventResult_Commit, node uint64) []*pb.CheckpointResult {
	var results []*pb.CheckpointResult
	for _, commit := range commits {
		if commit.Batch != nil {
			ns.LastSeqNo++
			if commit.Batch.SeqNo != ns.LastSeqNo {
				panic(fmt.Sprintf("unexpected out of order commit sequence number, expected %d, got %d", ns.LastSeqNo, commit.Batch.SeqNo))
			}

			for _, request := range commit.Batch.Requests {
				ns.ActiveHash.Write(request.Digest)

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

		if commit.SeqNo != ns.LastSeqNo {
			panic("asked to checkpoint for uncommitted sequence")
		}

		checkpoint := ns.Set(
			commit.SeqNo,
			ns.ActiveHash.Sum(nil),
			&pb.NetworkState{
				Config:  commit.NetworkConfig,
				Clients: commit.ClientStates,
			},
		)

		results = append(results, checkpoint)
	}

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
			&pb.StateEvent{
				Type: &pb.StateEvent_Initialize{
					Initialize: recorderNodeConfig.InitParms,
				},
			},
			0,
		)

		nodeState := &NodeState{
			Hasher:             r.Hasher,
			ReconfigPoints:     r.ReconfigPoints,
			Checkpoints:        list.New(),
			CheckpointsBySeqNo: map[uint64]*list.Element{},
		}

		nodes[i] = &RecorderNode{
			State:        nodeState,
			WAL:          wal,
			ReqStore:     NewReqStore(),
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
	case *pb.StateEvent_Tick:
		r.EventLog.InsertTickEvent(lastEvent.NodeId, int64(runtimeParms.TickInterval))
	case *pb.StateEvent_AddResults:
	case *pb.StateEvent_Step:
	case *pb.StateEvent_Propose:
	case *pb.StateEvent_ActionsReceived:
		if !node.AwaitingProcessEvent {
			return errors.Errorf("node %d was not awaiting a processing message, but got one", lastEvent.NodeId)
		}
		node.AwaitingProcessEvent = false
		processing := playbackNode.Processing

		for _, reqSlot := range processing.AllocatedRequests {
			client := r.Clients[int(reqSlot.ClientId)]
			if client.Config.ID != reqSlot.ClientId {
				panic("sanity check")
			}

			req := client.RequestByReqNo(reqSlot.ReqNo)
			if req == nil {
				continue
			}
			r.EventLog.InsertProposeEvent(lastEvent.NodeId, req, int64(client.Config.TxLatency))
		}

		// XXX shouldn't be needed
		// for _, req := range processing.StoreRequests {
		// node.ReqStore.Store(req.Ack, req.RequestData)
		// }

		for _, write := range processing.WriteAhead {
			switch {
			case write.Append != 0:
				node.WAL.Append(write.Append, write.Data)
			case write.Truncate != 0:
				node.WAL.Truncate(write.Truncate)
			default:
				panic("Append or Truncate must be set")
			}
		}

		for _, send := range processing.Send {
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

		if processing.StateTransfer != nil {
			var networkState *pb.NetworkState
			for _, node := range r.Nodes {
				el, ok := node.State.CheckpointsBySeqNo[processing.StateTransfer.SeqNo]
				if !ok {
					// if no node has the state, networkState will be nil
					// which signals to the state machine to try another target
					continue
				}

				networkState = el.Value.(*pb.CheckpointResult).NetworkState
				break
			}
			r.EventLog.InsertStateEvent(
				lastEvent.NodeId,
				&pb.StateEvent{
					Type: &pb.StateEvent_Transfer{
						Transfer: &pb.CEntry{
							SeqNo:           processing.StateTransfer.SeqNo,
							CheckpointValue: processing.StateTransfer.Value,
							NetworkState:    networkState,
						},
					},
				},
				int64(runtimeParms.StateTransferLatency),
			)
		}
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
				r.EventLog.List.Remove(x)
			}
		}

		delay := int64(0)

		var maxCEntry *pb.CEntry

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

			if cEntryT, ok := p.Type.(*pb.Persistent_CEntry); ok {
				maxCEntry = cEntryT.CEntry
			}
		})

		nodeState.Set(maxCEntry.SeqNo, maxCEntry.CheckpointValue, maxCEntry.NetworkState)

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
	case *pb.StateEvent_Transfer:
		node.State.Set(stateEvent.Transfer.SeqNo, stateEvent.Transfer.CheckpointValue, stateEvent.Transfer.NetworkState)
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

func isEmpty(actions *pb.StateEventResult) bool {
	return len(actions.Send) == 0 &&
		len(actions.WriteAhead) == 0 &&
		len(actions.Hash) == 0 &&
		len(actions.AllocatedRequests) == 0 &&
		len(actions.ForwardRequests) == 0 &&
		len(actions.StoreRequests) == 0 &&
		len(actions.Commits) == 0 &&
		actions.StateTransfer == nil
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
			InitParms: &pb.StateEvent_InitialParameters{
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
			TxLatency:   10,
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
