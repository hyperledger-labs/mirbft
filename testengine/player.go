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
	Logger    *zap.Logger
	Nodes     map[uint64]*PlaybackNode
}

func NewPlayer(el *EventLog, logger *zap.Logger) (*Player, error) {
	return &Player{
		EventLog: el,
		Logger:   logger,
		Nodes:    map[uint64]*PlaybackNode{},
	}, nil
}

func (p *Player) Node(id uint64) *PlaybackNode {
	node, ok := p.Nodes[id]
	if ok {
		return node
	}

	sm := &mirbft.StateMachine{
		Logger: p.Logger.Named(fmt.Sprintf("node%d", id)),
	}

	node = &PlaybackNode{
		StateMachine: sm,
		Actions:      &mirbft.Actions{},
		Status:       sm.Status(),
	}

	p.Nodes[id] = node

	return node
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

	node := p.Node(event.Target)

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
