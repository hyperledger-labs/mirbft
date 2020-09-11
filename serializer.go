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

	myConfig   *Config
	walStorage WALStorage
	reqStorage RequestStorage

	exitMutex  sync.Mutex
	exitErr    error
	exitStatus *status.StateMachine
}

func newSerializer(myConfig *Config, walStorage WALStorage, reqStorage RequestStorage, doneC <-chan struct{}) (*serializer, error) {

	s := &serializer{
		actionsC:   make(chan Actions),
		doneC:      doneC,
		propC:      make(chan *pb.StateEvent_Proposal),
		clientC:    make(chan *clientReq),
		resultsC:   make(chan *pb.StateEvent_ActionResults),
		statusC:    make(chan chan<- *status.StateMachine),
		stepC:      make(chan *pb.StateEvent_Step),
		tickC:      make(chan struct{}),
		errC:       make(chan struct{}),
		myConfig:   myConfig,
		walStorage: walStorage,
		reqStorage: reqStorage,
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
	sm := &StateMachine{
		Logger: s.myConfig.Logger,
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
			s.exitErr = ErrStopped
		}
		s.exitStatus = sm.Status()
	}()

	actions := &Actions{}

	applyEvent := func(stateEvent *pb.StateEvent) {
		if s.myConfig.EventInterceptor != nil {
			s.myConfig.EventInterceptor.Intercept(stateEvent)
		}

		actions.concat(sm.ApplyEvent(stateEvent))
	}

	applyEvent(&pb.StateEvent{
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

	for {
		data, err := s.walStorage.LoadNext()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(errors.WithMessage(err, "failed to load persisted from WALStorage"))
		}

		applyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_LoadEntry{
				LoadEntry: &pb.StateEvent_PersistedEntry{
					Entry: data,
				},
			},
		})
	}

	applyEvent(&pb.StateEvent{
		Type: &pb.StateEvent_CompleteInitialization{
			CompleteInitialization: &pb.StateEvent_LoadCompleted{},
		},
	})

	err := s.reqStorage.Uncommitted(func(ack *pb.RequestAck) {
		applyEvent(&pb.StateEvent{
			Type: &pb.StateEvent_Step{
				Step: &pb.StateEvent_InboundMsg{
					Source: s.myConfig.ID,
					Msg: &pb.Msg{
						Type: &pb.Msg_RequestAck{
							RequestAck: ack,
						},
					},
				},
			},
		})
	})

	if err != nil {
		panic(errors.WithMessage(err, "encounterer error reading uncommitted requests"))
	}

	var actionsC chan<- Actions
	for {
		select {
		case data := <-s.propC:
			applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Propose{
					Propose: data,
				},
			})
		case req := <-s.clientC:
			req.replyC <- sm.clientWaiter(req.clientID)
		case step := <-s.stepC:
			applyEvent(&pb.StateEvent{
				Type: step,
			})
		case actionsC <- *actions:
			actions.clear()
			actionsC = nil
			applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_ActionsReceived{
					ActionsReceived: &pb.StateEvent_Ready{},
				},
			})
		case results := <-s.resultsC:
			applyEvent(&pb.StateEvent{
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
			applyEvent(&pb.StateEvent{
				Type: &pb.StateEvent_Tick{
					Tick: &pb.StateEvent_TickElapsed{},
				},
			})
		case <-s.doneC:
			return
		}

		if !actions.isEmpty() {
			actionsC = s.actionsC
		}
	}
}
