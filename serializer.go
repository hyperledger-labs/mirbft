/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"sync"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"

	"github.com/pkg/errors"
)

// serializer provides a single threaded way to access the Mir state machine
// and passes work to/from the state machine.
type serializer struct {
	doneC    chan struct{}
	actionsC chan *statemachine.ActionList
	eventsC  chan *statemachine.EventList
	statusC  chan chan<- *status.StateMachine
	errC     chan struct{}

	myConfig   *Config
	walStorage WALStorage

	exitMutex  sync.Mutex
	exitErr    error
	exitStatus *status.StateMachine
}

func newSerializer(myConfig *Config, walStorage WALStorage) (*serializer, error) {

	s := &serializer{
		actionsC:   make(chan *statemachine.ActionList),
		doneC:      make(chan struct{}),
		eventsC:    make(chan *statemachine.EventList),
		statusC:    make(chan chan<- *status.StateMachine),
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

// TODO, add assertion in tests that log levels match
type logAdapter struct {
	Logger
}

func (la logAdapter) Log(level statemachine.LogLevel, msg string, args ...interface{}) {
	la.Logger.Log(LogLevel(level), msg, args...)
}

// run must be single threaded and is therefore hidden to prevent accidental capture
// of other go routines.
func (s *serializer) run() (exitErr error) {

	// Create a new state machine.
	sm := &statemachine.StateMachine{
		Logger: logAdapter{Logger: s.myConfig.Logger},
	}

	// Handle panics produced by the serializer.
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

	// Create list to store actions requested by the state machine.
	actions := &statemachine.ActionList{}

	// Define function for applying an event to the state machine.
	applyEvent := func(stateEvent *state.Event) error {

		// The applied event may be intercepted,
		// e.g. for logging and replay purposes.
		if s.myConfig.EventInterceptor != nil {
			if err := s.myConfig.EventInterceptor.Intercept(stateEvent); err != nil {
				return errors.WithMessage(err, "event interceptor error")
			}
		}

		// Apply event and save any potential resulting actions.
		actions.PushBackList(sm.ApplyEvent(stateEvent))
		return nil
	}

	// The state machine initialization event is always applied first.
	err := applyEvent(&state.Event{
		Type: &state.Event_Initialize{
			Initialize: &state.EventInitialParameters{
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

	// Bring state machine up to date with any data already present in the WAL
	// by replaying all entries in the WAL to the state machine.
	// This way, the state machine can continue executing at an arbitrary state,
	// e.g. after recovering from a crash.
	err = s.walStorage.LoadAll(func(i uint64, p *msgs.Persistent) {
		if _, ok := s.walStorage.(*dummyWAL); ok {
			// This was our own startup/bootstrap WAL,
			// we need to get these entries persisted into the real one.
			actions.Persist(i, p)
		}

		applyEvent(&state.Event{
			Type: &state.Event_LoadPersistedEntry{
				LoadPersistedEntry: &state.EventLoadPersistedEntry{
					Index: i,
					Entry: p,
				},
			},
		})
	})
	if err != nil {
		return errors.WithMessage(err, "failed to load persisted from WALStorage")
	}

	// Explicitly apply an event denoting the end of initialization.
	err = applyEvent(&state.Event{
		Type: &state.Event_CompleteInitialization{
			CompleteInitialization: &state.EventLoadCompleted{},
		},
	})
	if err != nil {
		return err
	}

	// This variable is nil (and thus writing to it blocks) all the time
	// except for when there are actions ready for the consumer.
	// When actions are ready, the channel interfacing with the consumer
	// (i.e. the one also returned by Node.Actions()) is stored here
	// until the action list is successfully written to the channel
	// (i.e. when the consumer reads the actions, as the channel is unbuffered).
	var actionsC chan<- *statemachine.ActionList

	// Main serializer loop.
	for {
		var err error
		select {
		case actionsC <- actions:
			// Actions successfully pushed to the consumer.

			// Reset action list and set actionsC to nil,
			// in order to prevent empty action lists to be
			// passed to the consumer on subsequent iterations.
			actions = &statemachine.ActionList{}
			actionsC = nil

			// Create a state machine event capturing the fact that
			// actions have been pushed to the consumer.
			// This is only useful for replaying/debugging purposes.
			// The state machine ignores this event.
			err = applyEvent(&state.Event{
				Type: &state.Event_ActionsReceived{
					ActionsReceived: &state.EventActionsReceived{},
				},
			})
		case events := <-s.eventsC:
			// External events (such as action results or incoming network messages)
			// have been presented to the node by the consumer.

			// Apply all presented events to the state machine.
			iter := events.Iterator()
			for event := iter.Next(); event != nil; event = iter.Next() {
				if err = applyEvent(event); err != nil {
					break
				}
			}
		case statusReq := <-s.statusC:
			// State machine status has been requested by the consumer.

			// s.statusC is a channel of channels, where each of these channels
			// represents a status request. The serializer writes the response
			// (i.e. the status) to the channel representing the request.
			select {
			case statusReq <- sm.Status():
			case <-s.doneC:
			}
		case <-s.doneC:
			// The serializer has been stopped
			// (stopping the serializer closes this channel).
			return ErrStopped
		}

		// If an error occurred, return it.
		if err != nil {
			return err
		}

		// If any actions have been produced, make them available to the consumer.
		if actions.Len() > 0 {
			actionsC = s.actionsC
		}
	}
}
