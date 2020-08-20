/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"io"
	"sync"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
	// "go.uber.org/zap"
)

type clientReq struct {
	clientID uint64
	replyC   chan *clientWaiter
}

// serializer provides a single threaded way to access the Mir state machine
// and passes work to/from the state machine.
type serializer struct {
	actionsC chan Actions
	doneC    <-chan struct{}
	clientC  chan *clientReq
	propC    chan *pb.StateEvent_Proposal
	resultsC chan *pb.StateEvent_ActionResults
	statusC  chan chan<- *Status
	stepC    chan *pb.StateEvent_Step
	tickC    chan struct{}
	errC     chan struct{}

	exitMutex    sync.Mutex
	exitErr      error
	exitStatus   *Status
	stateMachine *StateMachine
}

func newSerializer(myConfig *Config, storage Storage, doneC <-chan struct{}) (*serializer, error) {
	sm := &StateMachine{}
	sm.initialize(myConfig)

	var index uint64
	for {
		data, err := storage.Load(index)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Errorf("failed to load persisted from Storage: %s", err)
		}

		sm.ApplyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_LoadEntry{
				LoadEntry: &pb.StateEvent_PersistedEntry{
					Entry: data,
				},
			},
		})
		index++
	}

	sm.ApplyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_CompleteInitialization{},
	})

	s := &serializer{
		actionsC:     make(chan Actions),
		doneC:        doneC,
		propC:        make(chan *pb.StateEvent_Proposal),
		clientC:      make(chan *clientReq),
		resultsC:     make(chan *pb.StateEvent_ActionResults),
		statusC:      make(chan chan<- *Status),
		stepC:        make(chan *pb.StateEvent_Step),
		tickC:        make(chan struct{}),
		errC:         make(chan struct{}),
		stateMachine: sm,
	}
	go s.run()
	return s, nil
}

func (s *serializer) getExitErr() error {
	s.exitMutex.Lock()
	defer s.exitMutex.Unlock()
	return s.exitErr
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *serializer) run() {
	defer func() {
		s.exitMutex.Lock()
		defer s.exitMutex.Unlock()
		close(s.errC)
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				s.exitErr = errors.Wrapf(err, "serializer caught panic")
			} else {
				s.exitErr = errors.Errorf("panic in statemachine: %v", r)
			}
		} else {
			s.exitErr = ErrStopped
		}
		s.exitStatus = s.stateMachine.Status()
	}()

	// TODO, at some point, these can change, need to be recalculated
	replicas := make([]Replica, len(s.stateMachine.networkConfig.Nodes))
	for i, node := range s.stateMachine.networkConfig.Nodes {
		replicas[i] = Replica{
			ID: node,
		}
	}

	actions := &Actions{
		Replicas: replicas,
	}
	var actionsC chan<- Actions
	for {
		var stateEvent *pb.StateEvent

		select {
		case data := <-s.propC:
			stateEvent = &pb.StateEvent{
				Type: &pb.StateEvent_Propose{
					Propose: data,
				},
			}
		case req := <-s.clientC:
			req.replyC <- s.stateMachine.clientWaiter(req.clientID)
		case step := <-s.stepC:
			stateEvent = &pb.StateEvent{
				Type: step,
			}
		case actionsC <- *actions:
			actions.Clear()
			actionsC = nil
			continue
		case results := <-s.resultsC:
			stateEvent = &pb.StateEvent{
				Type: &pb.StateEvent_AddResults{
					AddResults: results,
				},
			}
		case statusReq := <-s.statusC:
			select {
			case statusReq <- s.stateMachine.Status():
			case <-s.doneC:
			}
		case <-s.tickC:
			stateEvent = &pb.StateEvent{
				Type: &pb.StateEvent_Tick{
					Tick: &pb.StateEvent_TickElapsed{},
				},
			}
		case <-s.doneC:
			return
		}

		if stateEvent != nil {
			actions.Append(s.stateMachine.ApplyEvent(stateEvent))
		}

		// We unconditionally re-enable the actions channel after any event is injected into the system
		// which will mean some zero-length actions get sent to the consumer.  This isn't optimal,
		// but, I've convinced myself that's okay for a couple reasons:
		// 1) Under stress, processing the actions will take enough time for the actions channel to
		// become available again anyway.
		// 2) For tests and visualizations, it's very nice being able to guarantee that we have
		// the latest set of actions (in the other model, it's unclear whether a call to the actions
		// channel will ever unblock.
		actionsC = s.actionsC
	}
}
