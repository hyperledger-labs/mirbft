/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"
	"io"

	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/pkg/eventlog/recorderpb"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"
	"github.com/pkg/errors"
)

type EventSource interface {
	ReadEvent() (*rpb.RecordedEvent, error)
}

type PlaybackNode struct {
	ID               uint64
	StateMachine     *statemachine.StateMachine
	Processing       *pb.StateEventResult
	Actions          *pb.StateEventResult
	ClientProcessing *pb.StateEventResult
	ClientActions    *pb.StateEventResult
	Status           *status.StateMachine
}

type Player struct {
	LastEvent   *rpb.RecordedEvent
	EventSource EventSource
	LogOutput   io.Writer
	Nodes       map[uint64]*PlaybackNode
}

func NewPlayer(es EventSource, logOutput io.Writer) (*Player, error) {
	return &Player{
		EventSource: es,
		LogOutput:   logOutput,
		Nodes:       map[uint64]*PlaybackNode{},
	}, nil
}

func (p *Player) Node(id uint64) *PlaybackNode {
	node, ok := p.Nodes[id]
	if ok {
		return node
	}

	node = &PlaybackNode{
		ID:            id,
		Actions:       &pb.StateEventResult{},
		ClientActions: &pb.StateEventResult{},
		Status: &status.StateMachine{
			NodeID: id,
		},
	}

	p.Nodes[id] = node

	return node
}

type NamedLogger struct {
	Level  statemachine.LogLevel
	Name   string
	Output io.Writer
}

func (nl NamedLogger) Log(level statemachine.LogLevel, msg string, args ...interface{}) {
	if level < nl.Level {
		return
	}

	fmt.Fprint(nl.Output, nl.Name)
	fmt.Fprint(nl.Output, ": ")
	fmt.Fprint(nl.Output, msg)
	for i := 0; i < len(args); i++ {
		if i+1 < len(args) {
			switch args[i+1].(type) {
			case []byte:
				fmt.Fprintf(nl.Output, " %s=%x", args[i], args[i+1])
			default:
				fmt.Fprintf(nl.Output, " %s=%v", args[i], args[i+1])
			}
			i++
		} else {
			fmt.Fprintf(nl.Output, " %s=%%MISSING%%", args[i])
		}
	}
	fmt.Fprintf(nl.Output, "\n")
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
		sm := &statemachine.StateMachine{
			Logger: NamedLogger{
				Output: p.LogOutput,
				Level:  statemachine.LevelInfo,
				Name:   fmt.Sprintf("node%d", node.ID),
			},
		}
		node.StateMachine = sm
		node.Actions = &pb.StateEventResult{}
		node.Status = sm.Status()
		node.Processing = nil
	case *pb.StateEvent_Transfer:
	case *pb.StateEvent_AddResults:
		if node.Processing == nil {
			return errors.Errorf("node %d is not currently processing but got an apply event", event.NodeId)
		}

		node.Processing = nil
	case *pb.StateEvent_AddClientResults:
		if node.ClientProcessing == nil {
			return errors.Errorf("node %d is not currently client processing but got a client apply event", event.NodeId)
		}

		node.ClientProcessing = nil
	case *pb.StateEvent_ClientActionsReceived:
		if node.ClientProcessing != nil {
			return errors.Errorf("node %d is currently client processing but got a second client process event", event.NodeId)
		}

		node.ClientProcessing = node.ClientActions
		node.ClientActions = &pb.StateEventResult{}
	case *pb.StateEvent_ActionsReceived:
		if node.Processing != nil {
			return errors.Errorf("node %d is currently processing but got a second process event", event.NodeId)
		}

		node.Processing = node.Actions
		node.Actions = &pb.StateEventResult{}
	}

	newActions := node.StateMachine.ApplyEvent(event.StateEvent)
	node.Actions.Send = append(node.Actions.Send, newActions.Send...)
	node.Actions.Hash = append(node.Actions.Hash, newActions.Hash...)
	node.Actions.Commits = append(node.Actions.Commits, newActions.Commits...)
	node.Actions.WriteAhead = append(node.Actions.WriteAhead, newActions.WriteAhead...)
	node.ClientActions.AllocatedRequests = append(node.ClientActions.AllocatedRequests, newActions.AllocatedRequests...)
	node.ClientActions.CorrectRequests = append(node.ClientActions.CorrectRequests, newActions.CorrectRequests...)
	node.ClientActions.ForwardRequests = append(node.ClientActions.ForwardRequests, newActions.ForwardRequests...)
	if newActions.StateTransfer != nil {
		if node.Actions.StateTransfer != nil {
			return errors.Errorf("node %d has requested state transfer twice without resolution", event.NodeId)
		}

		node.Actions.StateTransfer = newActions.StateTransfer
	}

	node.Status = node.StateMachine.Status()

	return nil
}
