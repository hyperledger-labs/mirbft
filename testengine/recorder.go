/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"math/rand"
	"time"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Hasher func() hash.Hash

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

type RecorderNode struct {
	PlaybackNode         *PlaybackNode
	State                *NodeState
	Config               *tpb.NodeConfig
	AwaitingProcessEvent bool
}

type RecorderClient struct {
	Config            *ClientConfig
	LastNodeReqNoSend []uint64
}

func (rc *RecorderClient) RequestByReqNo(reqNo uint64) *pb.RequestData {
	if reqNo > rc.Config.Total {
		// We've sent all we should
		return nil
	}

	var buffer bytes.Buffer
	buffer.Write(rc.Config.ID)
	buffer.Write([]byte("-"))
	buffer.Write(uint64ToBytes(reqNo))

	return &pb.RequestData{
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
	LastCommittedSeqNo uint64
	OutstandingCommits []*mirbft.Commit
	Hasher             hash.Hash
	Value              []byte
	Length             uint64
	FirstCommit        *CommitList
	LastCommit         *CommitList
}

func (ns *NodeState) Commit(commits []*mirbft.Commit, node uint64) []*tpb.Checkpoint {
	for _, commit := range commits {
		if commit.QEntry.SeqNo <= ns.LastCommittedSeqNo {
			panic(fmt.Sprintf("trying to commit seqno=%d, but we've already committed seqno=%d", commit.QEntry.SeqNo, ns.LastCommittedSeqNo))
		}
		index := commit.QEntry.SeqNo - ns.LastCommittedSeqNo
		for index >= uint64(len(ns.OutstandingCommits)) {
			ns.OutstandingCommits = append(ns.OutstandingCommits, nil)
		}
		ns.OutstandingCommits[index-1] = commit
	}

	var results []*tpb.Checkpoint

	i := 0
	for _, commit := range ns.OutstandingCommits {
		if commit == nil {
			break
		}
		i++

		if ns.FirstCommit == nil {
			ns.FirstCommit = &CommitList{
				Commit: commit,
			}
			ns.LastCommit = ns.FirstCommit
		} else {
			ns.LastCommit.Next = &CommitList{
				Commit: commit,
			}
			ns.LastCommit = ns.LastCommit.Next
		}

		for _, request := range commit.QEntry.Requests {
			ns.Hasher.Write(request.Digest)
			ns.Length++
		}

		if commit.Checkpoint {
			results = append(results, &tpb.Checkpoint{
				SeqNo: commit.QEntry.SeqNo,
				Value: ns.Hasher.Sum(nil),
			})
		}

		ns.LastCommittedSeqNo = commit.QEntry.SeqNo
	}

	k := 0
	for j := i; j < len(ns.OutstandingCommits); j++ {
		ns.OutstandingCommits[k] = ns.OutstandingCommits[j]
		k++
	}
	ns.OutstandingCommits = ns.OutstandingCommits[:k]

	ns.Value = ns.Hasher.Sum(nil)

	return results

}

type ClientConfig struct {
	ID          []byte
	TxLatency   uint64
	MaxInFlight int
	Total       uint64
}

type Recorder struct {
	NetworkConfig *pb.NetworkConfig
	NodeConfigs   []*tpb.NodeConfig
	ClientConfigs []*ClientConfig
	Manglers      []Mangler
	Logger        *zap.Logger
	Hasher        Hasher
	RandomSeed    int64
}

func (r *Recorder) Recording() (*Recording, error) {
	eventLog := &EventLog{
		InitialConfig: r.NetworkConfig,
		NodeConfigs:   r.NodeConfigs,
	}

	player, err := NewPlayer(eventLog, r.Logger)
	if err != nil {
		return nil, errors.WithMessage(err, "could not construct player")
	}

	nodes := make([]*RecorderNode, len(r.NodeConfigs))
	for i, nodeConfig := range r.NodeConfigs {
		nodeID := uint64(i)
		eventLog.InsertTick(nodeID, uint64(nodeConfig.TickInterval))
		nodes[i] = &RecorderNode{
			State: &NodeState{
				Hasher: r.Hasher(),
			},
			PlaybackNode: player.Nodes[i],
			Config:       nodeConfig,
		}
	}

	clients := make([]*RecorderClient, len(r.ClientConfigs))
	for i, clientConfig := range r.ClientConfigs {
		client := &RecorderClient{
			Config:            clientConfig,
			LastNodeReqNoSend: make([]uint64, len(nodes)),
		}

		clients[i] = client

		for i := 1; i <= clientConfig.MaxInFlight; i++ {
			req := client.RequestByReqNo(uint64(i))
			if req == nil {
				continue
			}
			for j := range nodes {
				client.LastNodeReqNoSend[uint64(j)] = uint64(i)
				eventLog.InsertPropose(uint64(j), req, client.Config.TxLatency)
			}
		}
	}

	return &Recording{
		Hasher:   r.Hasher,
		EventLog: player.EventLog,
		Player:   player,
		Nodes:    nodes,
		Clients:  clients,
		Manglers: r.Manglers,
		Rand:     rand.New(rand.NewSource(r.RandomSeed)),
	}, nil
}

type Mangler interface {
	BeforeStep(random int, el *EventLog)
}

type Recording struct {
	Hasher   Hasher
	EventLog *EventLog
	Player   *Player
	Nodes    []*RecorderNode
	Clients  []*RecorderClient
	Manglers []Mangler
	Rand     *rand.Rand
}

func (r *Recording) Step() error {
	if r.EventLog.NextEventLogEntry == nil {
		return errors.Errorf("event log is empty, nothing to do")
	}

	for _, mangler := range r.Manglers {
		mangler.BeforeStep(r.Rand.Int(), r.EventLog)
	}

	err := r.Player.Step()
	if err != nil {
		return errors.WithMessagef(err, "could not step recorder's underlying player")
	}

	lastEvent := r.Player.LastEvent
	if lastEvent.Dropped {
		return nil
	}

	node := r.Nodes[int(lastEvent.Target)]
	nodeConfig := node.Config
	playbackNode := node.PlaybackNode
	nodeState := node.State

	switch lastEvent.Type.(type) {
	case *tpb.Event_Apply_:
		nodeStatus := node.PlaybackNode.Status
		for _, rw := range nodeStatus.RequestWindows {
			for _, client := range r.Clients {
				if !bytes.Equal(client.Config.ID, rw.ClientID) {
					continue
				}

				for i := client.LastNodeReqNoSend[lastEvent.Target] + 1; i <= rw.HighWatermark; i++ {
					req := client.RequestByReqNo(i)
					if req == nil {
						continue
					}
					client.LastNodeReqNoSend[lastEvent.Target] = i
					r.EventLog.InsertPropose(lastEvent.Target, req, client.Config.TxLatency)
				}
			}
		}
	case *tpb.Event_Receive_:
	case *tpb.Event_Process_:
		if !node.AwaitingProcessEvent {
			return errors.Errorf("node %d was not awaiting a processing message, but got one", lastEvent.Target)
		}
		node.AwaitingProcessEvent = false
		processing := playbackNode.Processing
		for _, msg := range processing.Broadcast {
			for i := range r.Player.Nodes {
				if uint64(i) == lastEvent.Target {
					// We've already sent it to ourselves
					continue
				}
				r.EventLog.InsertRecv(uint64(i), lastEvent.Target, msg, uint64(nodeConfig.LinkLatency))
			}
		}

		for _, unicast := range processing.Unicast {
			if unicast.Target == lastEvent.Target {
				// We've already sent it to ourselves
				continue
			}

			r.EventLog.InsertRecv(unicast.Target, lastEvent.Target, unicast.Msg, uint64(nodeConfig.LinkLatency))
		}

		apply := &tpb.Event_Apply{
			Preprocessed: make([]*tpb.Request, len(processing.Preprocess)),
			Processed:    make([]*tpb.Batch, len(processing.Process)),
		}

		for i, preprocess := range processing.Preprocess {
			hasher := r.Hasher()
			hasher.Write(preprocess.ClientRequest.ClientId)
			hasher.Write(uint64ToBytes(preprocess.ClientRequest.ReqNo))
			hasher.Write(preprocess.ClientRequest.Data)
			hasher.Write(preprocess.ClientRequest.Signature)

			apply.Preprocessed[i] = &tpb.Request{
				ClientId:  preprocess.ClientRequest.ClientId,
				ReqNo:     preprocess.ClientRequest.ReqNo,
				Data:      preprocess.ClientRequest.Data,
				Signature: preprocess.ClientRequest.Signature,
				Digest:    hasher.Sum(nil),
			}
		}

		for i, process := range processing.Process {
			hasher := r.Hasher()
			requests := make([]*pb.Request, len(process.Requests))
			for i, request := range process.Requests {
				hasher.Write(request.Digest)
				requests[i] = &pb.Request{
					ClientId: request.RequestData.ClientId,
					ReqNo:    request.RequestData.ReqNo,
					Digest:   request.Digest,
				}
			}

			apply.Processed[i] = &tpb.Batch{
				Source:   process.Source,
				Epoch:    process.Epoch,
				SeqNo:    process.SeqNo,
				Digest:   hasher.Sum(nil),
				Requests: requests,
			}
		}

		apply.Checkpoints = nodeState.Commit(processing.Commits, lastEvent.Target)

		r.EventLog.InsertApply(lastEvent.Target, apply, uint64(nodeConfig.ReadyLatency))
	case *tpb.Event_Propose_:
	case *tpb.Event_Tick_:
		r.EventLog.InsertTick(lastEvent.Target, uint64(nodeConfig.TickInterval))
	}

	if playbackNode.Processing == nil &&
		!playbackNode.Actions.IsEmpty() &&
		!node.AwaitingProcessEvent {
		r.EventLog.InsertProcess(lastEvent.Target, uint64(nodeConfig.ProcessLatency))
		node.AwaitingProcessEvent = true
	}

	return nil
}

// DrainClients will execute the recording until all client requests have committed.
// It will return with an error if the (real) execution time takes longer than the
// specified timeout.  If any step returns an error, this function returns that error.
func (r *Recording) DrainClients(timeout time.Duration) (int, error) {
	start := time.Now()
	totalReqs := uint64(0)
	for _, client := range r.Clients {
		totalReqs += client.Config.Total
	}

	count := 0
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

		if time.Since(start) > timeout {
			return 0, errors.Errorf("timed out after %d entries", r.EventLog.Count())
		}
	}
}

func BasicRecorder(nodeCount, clientCount int, reqsPerClient uint64) *Recorder {
	networkConfig := mirbft.StandardInitialNetworkConfig(nodeCount)

	var nodeConfigs []*tpb.NodeConfig
	for i := 0; i < nodeCount; i++ {
		nodeConfigs = append(nodeConfigs, &tpb.NodeConfig{
			Id:                   uint64(i),
			HeartbeatTicks:       2,
			SuspectTicks:         4,
			NewEpochTimeoutTicks: 8,
			TickInterval:         500,
			LinkLatency:          100,
			ReadyLatency:         50,
			ProcessLatency:       10,
		})
	}

	var clientConfigs []*ClientConfig
	for i := 0; i < clientCount; i++ {
		clientConfigs = append(clientConfigs, &ClientConfig{
			ID:          []byte(fmt.Sprintf("%d", i)),
			MaxInFlight: int(networkConfig.CheckpointInterval / 2),
			Total:       reqsPerClient,
		})
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	return &Recorder{
		NetworkConfig: networkConfig,
		NodeConfigs:   nodeConfigs,
		Logger:        logger,
		Hasher:        sha256.New,
		ClientConfigs: clientConfigs,
	}
}
