/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"context"
	"encoding/binary"
	"hash"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"

	"github.com/pkg/errors"
)

type Hasher func() hash.Hash

func uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

func bytesToUint64(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}

type RecorderNode struct {
	PlaybackNode         *PlaybackNode
	State                *NodeState
	Config               *tpb.NodeConfig
	AwaitingProcessEvent bool
}

type NodeState struct {
	LastCommittedSeqNo uint64
	OutstandingCommits []*mirbft.Commit
	Hasher             hash.Hash
	Value              []byte
	Length             uint64
}

func (ns *NodeState) Commit(commits []*mirbft.Commit) []*tpb.Checkpoint {
	for _, commit := range commits {
		index := commit.QEntry.SeqNo - ns.LastCommittedSeqNo
		for index >= uint64(len(ns.OutstandingCommits)) {
			ns.OutstandingCommits = append(ns.OutstandingCommits, nil)
		}
		ns.OutstandingCommits[index-1] = commit
	}

	var results []*tpb.Checkpoint

	i := 0
	for _, commit := range ns.OutstandingCommits {
		i++
		if commit == nil {
			break
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

type Recorder struct {
	Hasher   Hasher
	EventLog *EventLog
	Player   *Player
	Nodes    []*RecorderNode
}

func NewRecorder(player *Player, hasher Hasher) *Recorder {
	nodes := make([]*RecorderNode, len(player.Nodes))
	for i, nodeConfig := range player.EventLog.NodeConfigs {
		nodeID := uint64(i)
		player.EventLog.InsertTick(nodeID, uint64(nodeConfig.TickInterval))
		nodes[i] = &RecorderNode{
			State: &NodeState{
				Hasher: hasher(),
			},
			PlaybackNode: player.Nodes[i],
			Config:       nodeConfig,
		}
	}

	return &Recorder{
		Hasher:   hasher,
		EventLog: player.EventLog,
		Player:   player,
		Nodes:    nodes,
	}
}

func (r *Recorder) Step() error {
	err := r.Player.Step()
	if err != nil {
		return errors.WithMessagef(err, "could not step recorder's underlying player")
	}

	lastEvent := r.Player.LastEvent
	node := r.Nodes[int(lastEvent.Target)]
	nodeConfig := node.Config
	playbackNode := node.PlaybackNode
	nodeState := node.State

	switch lastEvent.Type.(type) {
	case *tpb.Event_Apply_:
	case *tpb.Event_Receive_:
	case *tpb.Event_Process_:
		if !node.AwaitingProcessEvent {
			return errors.Errorf("node %d was not awaiting a processing message, but got one", lastEvent.Target)
		}
		node.AwaitingProcessEvent = false
		processing := playbackNode.Processing
		for _, msg := range processing.Broadcast {
			for i := range r.Player.Nodes {
				if uint64(i) != lastEvent.Target {
					r.EventLog.InsertRecv(uint64(i), lastEvent.Target, msg, uint64(nodeConfig.LinkLatency))
				} else {
					// Send it to ourselves with no latency
					err := playbackNode.Node.Step(context.Background(), lastEvent.Target, msg)
					if err != nil {
						return errors.WithMessagef(err, "node %d could not step message to self", lastEvent.Target)
					}
				}
			}
		}

		for _, unicast := range processing.Unicast {
			if unicast.Target != lastEvent.Target {
				r.EventLog.InsertRecv(unicast.Target, lastEvent.Target, unicast.Msg, uint64(nodeConfig.LinkLatency))
			} else {
				// Send it to ourselves with no latency
				err := playbackNode.Node.Step(context.Background(), lastEvent.Target, unicast.Msg)
				if err != nil {
					return errors.WithMessagef(err, "node %d could not step message to self", lastEvent.Target)
				}
			}
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

		apply.Checkpoints = nodeState.Commit(processing.Commits)

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
