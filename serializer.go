/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"sync"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"

	"github.com/pkg/errors"
)

// serializer provides a single threaded way to access the Mir state machine
// and passes work to/from the state machine.
type serializer struct {
	actionsC  chan Actions
	doneC     chan struct{}
	propC     chan *pb.StateEvent_Proposal
	resultsC  chan *pb.StateEvent_ActionResults
	transferC chan *pb.StateEvent_Transfer
	statusC   chan chan<- *status.StateMachine
	stepC     chan *pb.StateEvent_Step
	tickC     chan struct{}
	errC      chan struct{}

	myConfig   *Config
	walStorage WALStorage

	exitMutex  sync.Mutex
	exitErr    error
	exitStatus *status.StateMachine
}

func newSerializer(myConfig *Config, walStorage WALStorage) (*serializer, error) {

	s := &serializer{
		actionsC:   make(chan Actions),
		doneC:      make(chan struct{}),
		propC:      make(chan *pb.StateEvent_Proposal),
		resultsC:   make(chan *pb.StateEvent_ActionResults),
		transferC:  make(chan *pb.StateEvent_Transfer),
		statusC:    make(chan chan<- *status.StateMachine),
		stepC:      make(chan *pb.StateEvent_Step),
		tickC:      make(chan struct{}),
		errC:       make(chan struct{}),
		myConfig:   myConfig,
		walStorage: walStorage,
	}
	go s.run()
	return s, nil
}

func (s *serializer) stop() {
	s.exitMutex.Lock()
	select {
	case <-s.doneC:
	default:
		close(s.doneC)
	}
	s.exitMutex.Unlock()
	<-s.errC
}

func (s *serializer) getExitErr() error {
	s.exitMutex.Lock()
	defer s.exitMutex.Unlock()
	return s.exitErr
}

type logAdapter struct {
	Logger
}

func (la logAdapter) Log(level statemachine.LogLevel, msg string, args ...interface{}) {
	la.Logger.Log(LogLevel(level), msg, args...)
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *serializer) run() (exitErr error) {
	sm := &statemachine.StateMachine{
		Logger: logAdapter{Logger: s.myConfig.Logger},
	}

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
			s.exitErr = exitErr
		}
		s.exitStatus = sm.Status()
	}()

	actions := &Actions{}

	applyEvent := func(stateEvent *pb.StateEvent) error {
		if s.myConfig.EventInterceptor != nil {
			err := s.myConfig.EventInterceptor.Intercept(stateEvent)
			if err != nil {
				return errors.WithMessage(err, "event interceptor error")
			}
		}

		actions.concat((&actionSet{StateEventResult: *sm.ApplyEvent(stateEvent)}).toActions())
		return nil
	}

	err := applyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_Initialize{
			Initialize: &pb.StateEvent_InitialParameters{
				Id:                   s.myConfig.ID,
				BatchSize:            s.myConfig.BatchSize,
				HeartbeatTicks:       s.myConfig.HeartbeatTicks,
				SuspectTicks:         s.myConfig.SuspectTicks,
				NewEpochTimeoutTicks: s.myConfig.NewEpochTimeoutTicks,
				BufferSize:           s.myConfig.BufferSize,
			},
		},
	})
	if err != nil {
		return err
	}

	err = s.walStorage.LoadAll(func(i uint64, p *pb.Persistent) {
		if _, ok := s.walStorage.(*dummyWAL); ok {
			// This was our own startup/bootstrap WAL,
			// we need to get these entries persisted into the real one.
			actions.persist(i, p)
		}

		applyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_LoadEntry{
				LoadEntry: &pb.StateEvent_PersistedEntry{
					Index: i,
					Data:  p,
				},
			},
		})
	})

	if err != nil {
		return errors.WithMessage(err, "failed to load persisted from WALStorage")
	}

	err = applyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_CompleteInitialization{
			CompleteInitialization: &pb.StateEvent_LoadCompleted{},
		},
	})
	if err != nil {
		return err
	}

	var actionsC chan<- Actions
	for {
		var err error
		select {
		case data := <-s.propC:
			err = applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Propose{
					Propose: data,
				},
			})
		case step := <-s.stepC:
			err = applyEvent(&pb.StateEvent{
				Type: step,
			})
		case actionsC <- *actions:
			actions.clear()
			actionsC = nil
			err = applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_ActionsReceived{
					ActionsReceived: &pb.StateEvent_Ready{},
				},
			})
		case transfer := <-s.transferC:
			err = applyEvent(&pb.StateEvent{
				Type: transfer,
			})
		case results := <-s.resultsC:
			err = applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_AddResults{
					AddResults: results,
				},
			})
		case statusReq := <-s.statusC:
			select {
			case statusReq <- sm.Status():
			case <-s.doneC:
			}
		case <-s.tickC:
			err = applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Tick{
					Tick: &pb.StateEvent_TickElapsed{},
				},
			})
		case <-s.doneC:
			return ErrStopped
		}

		if !actions.isEmpty() {
			actionsC = s.actionsC
		}

		if err != nil {
			return err
		}
	}
}
