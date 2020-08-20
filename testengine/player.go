/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"context"
	"fmt"
	"io"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/mock"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type PlaybackNode struct {
	Node            *mirbft.Node
	Processing      *mirbft.Actions
	Actions         *mirbft.Actions
	Status          *mirbft.Status
	ClientProposers map[string]*mirbft.ClientProposer
}

type Player struct {
	LastEvent *tpb.Event
	EventLog  *EventLog
	Nodes     []*PlaybackNode
	DoneC     chan struct{}
}

func NewPlayer(el *EventLog, logger *zap.Logger) (*Player, error) {
	doneC := make(chan struct{})
	var nodes []*PlaybackNode
	for i, nodeConfig := range el.NodeConfigs {
		if uint64(i) != nodeConfig.Id {
			return nil, errors.Errorf("nodeConfig.Id did not appear in order, expected %d, got %d", i, nodeConfig.Id)
		}

		storage := &mock.Storage{}
		storage.LoadReturnsOnCall(0, &pb.Persistent{
			Type: &pb.Persistent_CEntry{
				CEntry: &pb.CEntry{
					SeqNo:           0,
					CheckpointValue: []byte("fake-initial-value"),
					NetworkState:    el.InitialState,
					EpochConfig: &pb.EpochConfig{
						Number:            0,
						Leaders:           el.InitialState.Config.Nodes,
						PlannedExpiration: 0,
					},
				},
			},
		}, nil)
		storage.LoadReturnsOnCall(1, &pb.Persistent{
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
		}, nil)
		storage.LoadReturnsOnCall(2, nil, io.EOF)

		node, err := mirbft.StartNode(
			&mirbft.Config{
				ID:     nodeConfig.Id,
				Logger: logger.Named(fmt.Sprintf("node%d", nodeConfig.Id)),
				BatchParameters: mirbft.BatchParameters{
					BatchSize: 1,
				},
				SuspectTicks:         uint32(nodeConfig.SuspectTicks),
				NewEpochTimeoutTicks: uint32(nodeConfig.NewEpochTimeoutTicks),
				HeartbeatTicks:       uint32(nodeConfig.HeartbeatTicks),
				BufferSize:           uint32(nodeConfig.BufferSize),
			},
			doneC,
			storage,
		)
		if err != nil {
			return nil, errors.WithMessagef(err, "could not create mir node %d", nodeConfig.Id)
		}

		status, err := node.Status(context.Background())
		if err != nil {
			return nil, errors.WithMessagef(err, "could no get initial status for mir node %d", nodeConfig.Id)
		}

		nodes = append(nodes, &PlaybackNode{
			Node:            node,
			Actions:         &mirbft.Actions{},
			Status:          status,
			ClientProposers: map[string]*mirbft.ClientProposer{},
		})
	}

	return &Player{
		EventLog: el,
		Nodes:    nodes,
		DoneC:    doneC,
	}, nil
}

func (p *Player) Step() error {
	event := p.EventLog.ConsumeAndAdvance()
	if event == nil {
		return errors.Errorf("event log has no more events")
	}
	p.LastEvent = event

	if event.Dropped {
		// We allow the log to encode events which were set to be processed, but were
		// deliberatley dropped by some mangler.  This makes it easier to review event logs
		// identifying why tests failed.
		return nil
	}

	if event.Target >= uint64(len(p.Nodes)) {
		return errors.Errorf("event log referenced a node %d which does not exist", event.Target)
	}

	node := p.Nodes[int(event.Target)]

	switch et := event.Type.(type) {
	case *tpb.Event_Apply_:
		if node.Processing == nil {
			return errors.Errorf("node %d is not currently processing but got an apply event", event.Target)
		}

		node.Processing = nil

		apply := et.Apply
		actionResults := &mirbft.ActionResults{
			Digests:     make([]*mirbft.HashResult, len(apply.Digests)),
			Checkpoints: make([]*mirbft.CheckpointResult, len(apply.Checkpoints)),
		}

		for i, hashResult := range apply.Digests {

			actionResults.Digests[i] = &mirbft.HashResult{
				Digest: hashResult.Digest,
			}

			switch result := hashResult.Type.(type) {
			case *tpb.HashResult_Request:
				actionResults.Digests[i].Request = &mirbft.HashRequest{
					Origin: &pb.HashResult{
						Type: &pb.HashResult_Request_{
							Request: &pb.HashResult_Request{
								Source:  result.Request.Source,
								Request: result.Request.Request,
							},
						},
					},
				}
			case *tpb.HashResult_Batch:
				actionResults.Digests[i].Request = &mirbft.HashRequest{
					Origin: &pb.HashResult{
						Type: &pb.HashResult_Batch_{
							Batch: &pb.HashResult_Batch{
								Source:      result.Batch.Source,
								SeqNo:       result.Batch.SeqNo,
								Epoch:       result.Batch.Epoch,
								RequestAcks: result.Batch.RequestAcks,
							},
						},
					},
				}
			case *tpb.HashResult_EpochChange:
				actionResults.Digests[i].Request = &mirbft.HashRequest{
					Origin: &pb.HashResult{
						Type: &pb.HashResult_EpochChange_{
							EpochChange: &pb.HashResult_EpochChange{
								Source:      result.EpochChange.Source,
								Origin:      result.EpochChange.Origin,
								EpochChange: result.EpochChange.EpochChange,
							},
						},
					},
				}
			case *tpb.HashResult_VerifyBatch:
				actionResults.Digests[i].Request = &mirbft.HashRequest{
					Origin: &pb.HashResult{
						Type: &pb.HashResult_VerifyBatch_{
							VerifyBatch: &pb.HashResult_VerifyBatch{
								Source:         result.VerifyBatch.Source,
								SeqNo:          result.VerifyBatch.SeqNo,
								RequestAcks:    result.VerifyBatch.RequestAcks,
								ExpectedDigest: result.VerifyBatch.ExpectedDigest,
							},
						},
					},
				}
			case *tpb.HashResult_VerifyRequest:
				actionResults.Digests[i].Request = &mirbft.HashRequest{
					Origin: &pb.HashResult{
						Type: &pb.HashResult_VerifyRequest_{
							VerifyRequest: &pb.HashResult_VerifyRequest{
								Source:         result.VerifyRequest.Source,
								Request:        result.VerifyRequest.Request,
								ExpectedDigest: result.VerifyRequest.ExpectedDigest,
							},
						},
					},
				}
			default:
				return errors.Errorf("unimplemented hash result type: %T", hashResult.Type)
			}
		}

		for i, cr := range apply.Checkpoints {
			actionResults.Checkpoints[i] = &mirbft.CheckpointResult{
				Commit: &mirbft.Commit{
					QEntry:       cr.QEntry,
					NetworkState: cr.NetworkState,
					EpochConfig:  cr.EpochConfig,
				},
				Value: cr.Value,
			}
		}

		node.Node.AddResults(*actionResults)
	case *tpb.Event_Receive_:
		receive := et.Receive
		err := node.Node.Step(context.Background(), receive.Source, receive.Msg)
		if err != nil {
			return errors.WithMessagef(err, "node %d could not step msg from %d", event.Target, receive.Source)
		}
	case *tpb.Event_Process_:
		if node.Processing != nil {
			return errors.Errorf("node %d is currently processing but got a second process event", event.Target)
		}

		for _, msg := range node.Actions.Broadcast {
			err := node.Node.Step(context.Background(), event.Target, msg)
			if err != nil {
				return errors.WithMessagef(err, "node %d could not step message to self", event.Target)
			}
		}

		for _, unicast := range node.Actions.Unicast {
			if unicast.Target != event.Target {
				continue
			}
			// It's a bit weird to unicast to ourselves, but let's handle it.
			err := node.Node.Step(context.Background(), event.Target, unicast.Msg)
			if err != nil {
				return errors.WithMessagef(err, "node %d could not step message to self", event.Target)
			}
		}

		node.Processing = node.Actions
		node.Actions = &mirbft.Actions{}
		return nil
	case *tpb.Event_Propose_:
		request := et.Propose.Request
		clientProposer, ok := node.ClientProposers[string(request.ClientId)]
		if !ok {
			var err error
			clientProposer, err = node.Node.ClientProposer(context.Background(), request.ClientId, mirbft.WaitForRoom(false))
			if err != nil {
				return errors.WithMessagef(err, "node %d could not create client proposer for client %x", event.Target, request.ClientId)
			}
			node.ClientProposers[string(request.ClientId)] = clientProposer
		}
		err := clientProposer.Propose(context.Background(), request)
		if err != nil {
			return errors.WithMessagef(err, "node %d could not propose msg from client %x with reqno %d", event.Target, request.ClientId, request.ReqNo)
		}
	case *tpb.Event_Tick_:
		node.Node.Tick()
	}

	select {
	case actions := <-node.Node.Ready():
		node.Actions.Append(&actions)
	case <-node.Node.Err():
		_, err := node.Node.Status(context.Background())
		return errors.WithMessagef(err, "node %d is in an errored state -- last event: %T", event.Target, event.Type)
	}

	status, _ := node.Node.Status(context.Background())
	node.Status = status

	return nil
}
