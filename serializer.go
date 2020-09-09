/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"io"
	"sync"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"

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
	statusC  chan chan<- *status.StateMachine
	stepC    chan *pb.StateEvent_Step
	tickC    chan struct{}
	errC     chan struct{}

	interceptor EventInterceptor

	exitMutex    sync.Mutex
	exitErr      error
	exitStatus   *status.StateMachine
	stateMachine *StateMachine
}

func newSerializer(myConfig *Config, storage Storage, doneC <-chan struct{}) (*serializer, error) {
	sm := &StateMachine{
		Logger: myConfig.Logger,
	}

	applyEvent := func(stateEvent *pb.StateEvent) error {
		if myConfig.EventInterceptor != nil {
			myConfig.EventInterceptor.Intercept(stateEvent)
		}

		actions := sm.ApplyEvent(stateEvent)
		if !actions.isEmpty() {
			return errors.Errorf("did not expect any actions in response to initialization")
		}

		return nil
	}

	err := applyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_Initialize{
			Initialize: &pb.StateEvent_InitialParameters{
				Id:                   myConfig.ID,
				BatchSize:            myConfig.BatchSize,
				HeartbeatTicks:       myConfig.HeartbeatTicks,
				SuspectTicks:         myConfig.SuspectTicks,
				NewEpochTimeoutTicks: myConfig.NewEpochTimeoutTicks,
				BufferSize:           myConfig.BufferSize,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	for {
		data, err := storage.LoadNext()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Errorf("failed to load persisted from Storage: %s", err)
		}

		err = applyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_LoadEntry{
				LoadEntry: &pb.StateEvent_PersistedEntry{
					Entry: data,
				},
			},
		})
		if err != nil {
			return nil, err
		}
	}

	err = applyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_CompleteInitialization{
			CompleteInitialization: &pb.StateEvent_LoadCompleted{},
		},
	})
	if err != nil {
		return nil, err
	}

	s := &serializer{
		actionsC:     make(chan Actions),
		doneC:        doneC,
		propC:        make(chan *pb.StateEvent_Proposal),
		clientC:      make(chan *clientReq),
		resultsC:     make(chan *pb.StateEvent_ActionResults),
		statusC:      make(chan chan<- *status.StateMachine),
		stepC:        make(chan *pb.StateEvent_Step),
		tickC:        make(chan struct{}),
		errC:         make(chan struct{}),
		interceptor:  myConfig.EventInterceptor,
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

	actions := &Actions{}
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
			actions.clear()
			actionsC = nil
			stateEvent = &pb.StateEvent{
				Type: &pb.StateEvent_ActionsReceived{
					ActionsReceived: &pb.StateEvent_Ready{},
				},
			}
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
			if s.interceptor != nil {
				s.interceptor.Intercept(stateEvent)
			}
			actions.concat(s.stateMachine.ApplyEvent(stateEvent))
		}

		if !actions.isEmpty() {
			actionsC = s.actionsC
		}
	}
}
