/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type Step struct {
	Source uint64
	Msg    *pb.Msg
}

// Serializer provides a single threaded way to access the Mir state machine
// and passes work to/from the state machine.
type Serializer struct {
	ActionsC chan consumer.Actions
	DoneC    <-chan struct{}
	PropC    chan []byte
	ResultsC chan consumer.ActionResults
	StepC    chan Step

	StateMachine *StateMachine
}

func NewSerializer(stateMachine *StateMachine, doneC <-chan struct{}) *Serializer {
	s := &Serializer{
		ActionsC:     make(chan consumer.Actions),
		DoneC:        doneC,
		PropC:        make(chan []byte),
		ResultsC:     make(chan consumer.ActionResults),
		StepC:        make(chan Step),
		StateMachine: stateMachine,
	}
	go s.run()
	return s
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *Serializer) run() {
	actions := &consumer.Actions{}
	var actionsC chan<- consumer.Actions
	for {
		if actions.IsEmpty() {
			actionsC = nil
		} else {
			actionsC = s.ActionsC
		}
		s.StateMachine.Config.Logger.Debug("serializer waiting for consumer", zap.Bool("actions", actionsC != nil))

		select {
		case data := <-s.PropC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "proposal"))
			actions.Append(s.StateMachine.Propose(data))
		case step := <-s.StepC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "step"))
			actions.Append(s.StateMachine.Step(NodeID(step.Source), step.Msg))
		case actionsC <- *actions:
			s.StateMachine.Config.Logger.Debug("serializer sent actions")
			actions.Clear()
		case results := <-s.ResultsC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "results"))
			actions.Append(s.StateMachine.ProcessResults(results))
		case <-s.DoneC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "done"))
			return
		}
	}
}
