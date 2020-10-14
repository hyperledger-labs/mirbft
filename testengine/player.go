/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"

	"github.com/IBM/mirbft"
	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type EventSource interface {
	ReadEvent() (*rpb.RecordedEvent, error)
}

type PlaybackNode struct {
	ID           uint64
	StateMachine *mirbft.StateMachine
	Processing   *mirbft.Actions
	Actions      *mirbft.Actions
	Status       *status.StateMachine
}

type Player struct {
	LastEvent   *rpb.RecordedEvent
	EventSource EventSource
	Logger      *zap.Logger
	Nodes       map[uint64]*PlaybackNode
}

func NewPlayer(es EventSource, logger *zap.Logger) (*Player, error) {
	return &Player{
		EventSource: es,
		Logger:      logger,
		Nodes:       map[uint64]*PlaybackNode{},
	}, nil
}

func (p *Player) Node(id uint64) *PlaybackNode {
	node, ok := p.Nodes[id]
	if ok {
		return node
	}

	node = &PlaybackNode{
		ID:      id,
		Actions: &mirbft.Actions{},
		Status: &status.StateMachine{
			NodeID: id,
		},
	}

	p.Nodes[id] = node

	return node
}

func (p *Player) Step() error {
	event, err := p.EventSource.ReadEvent()
	if event == nil || err != nil {
		return errors.WithMessage(err, "event log has no more events")
	}
	p.LastEvent = event

	node := p.Node(event.NodeId)

	switch event.StateEvent.Type.(type) {
	case *pb.StateEvent_Initialize:
		sm := &mirbft.StateMachine{
			Logger: p.Logger.Named(fmt.Sprintf("node%d", node.ID)),
		}
		node.StateMachine = sm
		node.Actions = &mirbft.Actions{}
		node.Status = sm.Status()
		node.Processing = nil
	case *pb.StateEvent_AddResults:
		if node.Processing == nil {
			return errors.Errorf("node %d is not currently processing but got an apply event", event.NodeId)
		}

		node.Processing = nil
	case *pb.StateEvent_ActionsReceived:
		if node.Processing != nil {
			return errors.Errorf("node %d is currently processing but got a second process event", event.NodeId)
		}

		node.Processing = node.Actions
		node.Actions = &mirbft.Actions{}

		return nil
	}

	newActions := node.StateMachine.ApplyEvent(event.StateEvent)
	node.Actions.Send = append(node.Actions.Send, newActions.Send...)
	node.Actions.Hash = append(node.Actions.Hash, newActions.Hash...)
	node.Actions.Commits = append(node.Actions.Commits, newActions.Commits...)
	node.Actions.WriteAhead = append(node.Actions.WriteAhead, newActions.WriteAhead...)
	node.Actions.ForwardRequests = append(node.Actions.ForwardRequests, newActions.ForwardRequests...)
	node.Actions.StoreRequests = append(node.Actions.StoreRequests, newActions.StoreRequests...)

	node.Status = node.StateMachine.Status()

	return nil
}
