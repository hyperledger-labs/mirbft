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
	"hash"
	"math/rand"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/recorder/recorderpb"

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
}

type RecorderNode struct {
	PlaybackNode         *PlaybackNode
	State                *NodeState
	Config               *RecorderNodeConfig
	AwaitingProcessEvent bool
}

type RecorderClient struct {
	Config            *ClientConfig
	LastNodeReqNoSend []uint64
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

type CommitList struct {
	Commit *mirbft.Commit
	Next   *CommitList
}

type NodeState struct {
	Hasher      hash.Hash
	Value       []byte
	Length      uint64
	FirstCommit *CommitList
	LastCommit  *CommitList
}

func (ns *NodeState) Commit(commits []*mirbft.Commit, node uint64) []*pb.CheckpointResult {
	var results []*pb.CheckpointResult
	for _, commit := range commits {
		if commit.Batch != nil {
			for _, request := range commit.Batch.Requests {
				ns.Hasher.Write(request.Digest)
				ns.Length++
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

type Recorder struct {
	NetworkState        *pb.NetworkState
	RecorderNodeConfigs []*RecorderNodeConfig
	ClientConfigs       []*ClientConfig
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

	nodes := make([]*RecorderNode, len(r.RecorderNodeConfigs))
	for i, recorderNodeConfig := range r.RecorderNodeConfigs {
		nodeID := uint64(i)

		eventLog.InsertStateEvent(
			nodeID,
			&pb.StateEvent{
				Type: &pb.StateEvent_Initialize{
					Initialize: recorderNodeConfig.InitParms,
				},
			},
			0,
		)

		eventLog.InsertStateEvent(
			nodeID,
			&pb.StateEvent{
				Type: &pb.StateEvent_LoadEntry{
					LoadEntry: &pb.StateEvent_PersistedEntry{
						Entry: &pb.Persistent{
							Type: &pb.Persistent_CEntry{
								CEntry: &pb.CEntry{
									SeqNo:           0,
									CheckpointValue: []byte("fake-initial-value"),
									NetworkState:    r.NetworkState,
									CurrentEpoch:    0,
								},
							},
						},
					},
				},
			},
			1,
		)

		eventLog.InsertStateEvent(
			nodeID,
			&pb.StateEvent{
				Type: &pb.StateEvent_LoadEntry{
					LoadEntry: &pb.StateEvent_PersistedEntry{
						Entry: &pb.Persistent{
							Type: &pb.Persistent_EpochChange{
								EpochChange: &pb.EpochChange{
									NewEpoch: 1,
									Checkpoints: []*pb.Checkpoint{
										{
											SeqNo: 0,
											Value: []byte("fake-initial-value"),
										},
									},
								},
							},
						},
					},
				},
			},
			2,
		)

		eventLog.InsertStateEvent(
			nodeID,
			&pb.StateEvent{
				Type: &pb.StateEvent_CompleteInitialization{
					CompleteInitialization: &pb.StateEvent_LoadCompleted{},
				},
			},
			3,
		)

		eventLog.InsertTickEvent(nodeID, int64(recorderNodeConfig.RuntimeParms.TickInterval))
	}

	player, err := NewPlayer(eventLog, r.Logger)
	if err != nil {
		return nil, errors.WithMessage(err, "could not construct player")
	}

	for i, recorderNodeConfig := range r.RecorderNodeConfigs {
		nodes[i] = &RecorderNode{
			State: &NodeState{
				Hasher: r.Hasher(),
			},
			PlaybackNode: player.Node(uint64(i)),
			Config:       recorderNodeConfig,
		}
	}

	clients := make([]*RecorderClient, len(r.ClientConfigs))
	for i, clientConfig := range r.ClientConfigs {
		client := &RecorderClient{
			Config:            clientConfig,
			LastNodeReqNoSend: make([]uint64, len(nodes)),
		}

		clients[i] = client

		for i := 0; i < clientConfig.MaxInFlight; i++ {
			req := client.RequestByReqNo(uint64(i))
			if req == nil {
				continue
			}
			for j := range nodes {
				client.LastNodeReqNoSend[uint64(j)] = uint64(i)
				eventLog.InsertProposeEvent(uint64(j), req, int64(client.Config.TxLatency))
			}
		}
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

				for i := client.LastNodeReqNoSend[lastEvent.NodeId] + 1; i <= rw.HighWatermark; i++ {
					req := client.RequestByReqNo(i)
					if req == nil {
						continue
					}
					client.LastNodeReqNoSend[lastEvent.NodeId] = i
					r.EventLog.InsertProposeEvent(lastEvent.NodeId, req, int64(client.Config.TxLatency))
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
		len(actions.Persist) == 0 &&
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
				BufferSize:           5000,
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
