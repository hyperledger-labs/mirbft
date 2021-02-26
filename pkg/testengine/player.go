/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"
	"io"

	"github.com/IBM/mirbft/pkg/pb/recording"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"
	"github.com/pkg/errors"
)

type EventSource interface {
	ReadEvent() (*recording.Event, error)
}

type PlaybackNode struct {
	ID               uint64
	StateMachine     *statemachine.StateMachine
	Processing       *statemachine.ActionList
	Actions          *statemachine.ActionList
	ClientProcessing *statemachine.ActionList
	ClientActions    *statemachine.ActionList
	Status           *status.StateMachine
}

type Player struct {
	LastEvent   *recording.Event
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
		Actions:       &statemachine.ActionList{},
		ClientActions: &statemachine.ActionList{},
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
	case *state.Event_Initialize:
		sm := &statemachine.StateMachine{
			Logger: NamedLogger{
				Output: p.LogOutput,
				Level:  statemachine.LevelInfo,
				Name:   fmt.Sprintf("node%d", node.ID),
			},
		}
		node.StateMachine = sm
		node.Actions = &statemachine.ActionList{}
		node.Status = sm.Status()
		node.Processing = nil
	case *state.Event_StateTransferComplete:
	case *state.Event_StateTransferFailed:
	case *state.Event_HashResult:
	case *state.Event_CheckpointResult:
	case *state.Event_RequestPersisted:
	case *state.Event_ActionsReceived:
		node.Processing = node.Actions
		node.Actions = &statemachine.ActionList{}
	}

	node.Actions.PushBackList(node.StateMachine.ApplyEvent(event.StateEvent))

	// Note, we don't strictly need to poll status after each event, as
	// we never consume it.  It's a nice test that status works, but,
	// it adds 60% or more to the execution time.
	node.Status = node.StateMachine.Status()

	return nil
}
