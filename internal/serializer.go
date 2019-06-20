/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"fmt"

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
	StatusC  chan StatusReq
	StepC    chan Step
	TickC    chan struct{}

	StateMachine *StateMachine
}

type StatusReq struct {
	JSON   bool
	ReplyC chan string
}

func NewSerializer(stateMachine *StateMachine, doneC <-chan struct{}) *Serializer {
	s := &Serializer{
		ActionsC:     make(chan consumer.Actions),
		DoneC:        doneC,
		PropC:        make(chan []byte),
		ResultsC:     make(chan consumer.ActionResults),
		StatusC:      make(chan StatusReq),
		StepC:        make(chan Step),
		TickC:        make(chan struct{}),
		StateMachine: stateMachine,
	}
	go s.run()
	return s
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *Serializer) run() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(s.StateMachine.Status().Pretty())
			panic(r)
		}
	}()

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
		case statusReq := <-s.StatusC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "status"))
			status := s.StateMachine.Status()
			var statusStr string
			if statusReq.JSON {
				statusStr = status.JSON()
			} else {
				statusStr = status.Pretty()
			}
			select {
			case statusReq.ReplyC <- statusStr:
			case <-s.DoneC:
			}
		case <-s.TickC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "tick"))
			actions.Append(s.StateMachine.Tick())
		case <-s.DoneC:
			s.StateMachine.Config.Logger.Debug("serializer receiving", zap.String("type", "done"))
			return
		}
	}
}
