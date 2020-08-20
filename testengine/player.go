/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type PlaybackNode struct {
	StateMachine *mirbft.StateMachine
	Processing   *mirbft.Actions
	Actions      *mirbft.Actions
	Status       *mirbft.Status
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

		sm := &mirbft.StateMachine{
			Logger: logger.Named(fmt.Sprintf("node%d", nodeConfig.Id)),
		}

		sm.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_Initialize{
				Initialize: &pb.StateEvent_InitialParameters{
					Id:                   nodeConfig.Id,
					BatchSize:            1,
					SuspectTicks:         uint32(nodeConfig.SuspectTicks),
					NewEpochTimeoutTicks: uint32(nodeConfig.NewEpochTimeoutTicks),
					HeartbeatTicks:       uint32(nodeConfig.HeartbeatTicks),
					BufferSize:           uint32(nodeConfig.BufferSize),
				},
			},
		})

		sm.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_LoadEntry{
				LoadEntry: &pb.StateEvent_PersistedEntry{
					Entry: &pb.Persistent{
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
					},
				},
			},
		})

		sm.ApplyEvent(&pb.StateEvent{
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
		})

		sm.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_CompleteInitialization{},
		})

		nodes = append(nodes, &PlaybackNode{
			StateMachine: sm,
			Actions:      &mirbft.Actions{},
			Status:       sm.Status(),
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

	actions := &mirbft.Actions{}

	switch et := event.Type.(type) {
	case *tpb.Event_Apply_:
		if node.Processing == nil {
			return errors.Errorf("node %d is not currently processing but got an apply event", event.Target)
		}

		node.Processing = nil

		apply := et.Apply
		actionResults := &pb.StateEvent_ActionResults{
			Digests:     make([]*pb.HashResult, len(apply.Digests)),
			Checkpoints: make([]*pb.CheckpointResult, len(apply.Checkpoints)),
		}

		for i, hashResult := range apply.Digests {

			actionResults.Digests[i] = &pb.HashResult{
				Digest: hashResult.Digest,
			}

			switch result := hashResult.Type.(type) {
			case *tpb.HashResult_Request:
				actionResults.Digests[i].Type = &pb.HashResult_Request_{
					Request: &pb.HashResult_Request{
						Source:  result.Request.Source,
						Request: result.Request.Request,
					},
				}
			case *tpb.HashResult_Batch:
				actionResults.Digests[i].Type = &pb.HashResult_Batch_{
					Batch: &pb.HashResult_Batch{
						Source:      result.Batch.Source,
						SeqNo:       result.Batch.SeqNo,
						Epoch:       result.Batch.Epoch,
						RequestAcks: result.Batch.RequestAcks,
					},
				}
			case *tpb.HashResult_EpochChange:
				actionResults.Digests[i].Type = &pb.HashResult_EpochChange_{
					EpochChange: &pb.HashResult_EpochChange{
						Source:      result.EpochChange.Source,
						Origin:      result.EpochChange.Origin,
						EpochChange: result.EpochChange.EpochChange,
					},
				}
			case *tpb.HashResult_VerifyBatch:
				actionResults.Digests[i].Type = &pb.HashResult_VerifyBatch_{
					VerifyBatch: &pb.HashResult_VerifyBatch{
						Source:         result.VerifyBatch.Source,
						SeqNo:          result.VerifyBatch.SeqNo,
						RequestAcks:    result.VerifyBatch.RequestAcks,
						ExpectedDigest: result.VerifyBatch.ExpectedDigest,
					},
				}
			case *tpb.HashResult_VerifyRequest:
				actionResults.Digests[i].Type = &pb.HashResult_VerifyRequest_{
					VerifyRequest: &pb.HashResult_VerifyRequest{
						Source:         result.VerifyRequest.Source,
						Request:        result.VerifyRequest.Request,
						ExpectedDigest: result.VerifyRequest.ExpectedDigest,
					},
				}
			default:
				return errors.Errorf("unimplemented hash result type: %T", hashResult.Type)
			}
		}

		for i, cr := range apply.Checkpoints {
			actionResults.Checkpoints[i] = &pb.CheckpointResult{
				SeqNo:        cr.QEntry.SeqNo,
				NetworkState: cr.NetworkState,
				EpochConfig:  cr.EpochConfig,
				Value:        cr.Value,
			}
		}

		actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_AddResults{
				AddResults: actionResults,
			},
		}))
	case *tpb.Event_Receive_:
		receive := et.Receive
		actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_Step{
				Step: &pb.StateEvent_InboundMsg{
					Source: receive.Source,
					Msg:    receive.Msg,
				},
			},
		}))
	case *tpb.Event_Process_:
		if node.Processing != nil {
			return errors.Errorf("node %d is currently processing but got a second process event", event.Target)
		}

		for _, msg := range node.Actions.Broadcast {
			actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Step{
					Step: &pb.StateEvent_InboundMsg{
						Source: event.Target,
						Msg:    msg,
					},
				},
			}))
		}

		for _, unicast := range node.Actions.Unicast {
			if unicast.Target != event.Target {
				continue
			}

			// It's a bit weird to unicast to ourselves, but let's handle it.
			actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Step{
					Step: &pb.StateEvent_InboundMsg{
						Source: event.Target,
						Msg:    unicast.Msg,
					},
				},
			}))
		}

		node.Processing = node.Actions
		node.Actions = actions
		return nil
	case *tpb.Event_Propose_:
		request := et.Propose.Request
		actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_Propose{
				Propose: &pb.StateEvent_Proposal{
					Request: request,
				},
			},
		}))
	case *tpb.Event_Tick_:
		actions.Append(node.StateMachine.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_Tick{},
		}))
	}

	node.Actions.Append(actions)

	node.Status = node.StateMachine.Status()

	return nil
}
