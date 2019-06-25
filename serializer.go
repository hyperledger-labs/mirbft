/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type step struct {
	Source uint64
	Msg    *pb.Msg
}

// serializer provides a single threaded way to access the Mir state machine
// and passes work to/from the state machine.
type serializer struct {
	actionsC chan Actions
	doneC    <-chan struct{}
	propC    chan []byte
	resultsC chan ActionResults
	statusC  chan chan<- *Status
	stepC    chan step
	tickC    chan struct{}

	stateMachine *stateMachine
}

func newSerializer(stateMachine *stateMachine, doneC <-chan struct{}) *serializer {
	s := &serializer{
		actionsC:     make(chan Actions),
		doneC:        doneC,
		propC:        make(chan []byte),
		resultsC:     make(chan ActionResults),
		statusC:      make(chan chan<- *Status),
		stepC:        make(chan step),
		tickC:        make(chan struct{}),
		stateMachine: stateMachine,
	}
	go s.run()
	return s
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *serializer) run() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(s.stateMachine.status().Pretty())
			panic(r)
		}
	}()

	actions := &Actions{}
	var actionsC chan<- Actions
	for {
		if actions.IsEmpty() {
			actionsC = nil
		} else {
			actionsC = s.actionsC
		}
		s.stateMachine.myConfig.Logger.Debug("serializer waiting for consumer", zap.Bool("actions", actionsC != nil))

		select {
		case data := <-s.propC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "proposal"))
			actions.Append(s.stateMachine.propose(data))
		case step := <-s.stepC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "step"))
			actions.Append(s.stateMachine.step(NodeID(step.Source), step.Msg))
		case actionsC <- *actions:
			s.stateMachine.myConfig.Logger.Debug("serializer sent actions")
			actions.Clear()
		case results := <-s.resultsC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "results"))
			actions.Append(s.stateMachine.processResults(results))
		case statusReq := <-s.statusC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "status"))
			select {
			case statusReq <- s.stateMachine.status():
			case <-s.doneC:
			}
		case <-s.tickC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "tick"))
			actions.Append(s.stateMachine.tick())
		case <-s.doneC:
			s.stateMachine.myConfig.Logger.Debug("serializer receiving", zap.String("type", "done"))
			return
		}
	}
}
