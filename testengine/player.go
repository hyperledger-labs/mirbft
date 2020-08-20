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

	switch et := event.Type.(type) {
	case *tpb.Event_StateEvent:
		se := et.StateEvent
		if _, ok := se.Type.(*pb.StateEvent_AddResults); ok {
			if node.Processing == nil {
				return errors.Errorf("node %d is not currently processing but got an apply event", event.Target)
			} else {
				node.Processing = nil
			}
		}

		node.Actions.Append(node.StateMachine.ApplyEvent(se))
	case *tpb.Event_Process_:
		actions := &mirbft.Actions{}

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
	}

	node.Status = node.StateMachine.Status()

	return nil
}
