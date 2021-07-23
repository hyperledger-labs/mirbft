/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"github.com/hyperledger-labs/mirbft/pkg/clients"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/pkg/errors"
)

// Input and output channels for the modules within the Node.
// the Node.process() method reads and writes events
// to and from these channels to rout them between the Node's modules.
type workChans struct {
	clientIn        chan *statemachine.EventList
	clientOut       chan *statemachine.EventList
	stateMachineIn  chan *statemachine.EventList
	stateMachineOut chan *statemachine.EventList
	walIn           chan *statemachine.EventList
	walOut          chan *statemachine.EventList
	hashIn          chan *statemachine.EventList
	hashOut         chan *statemachine.EventList
	netIn           chan *statemachine.EventList
	netOut          chan *statemachine.EventList
	appIn           chan *statemachine.EventList
	appOut          chan *statemachine.EventList
	reqStoreIn      chan *statemachine.EventList
	reqStoreOut     chan *statemachine.EventList

	externalEvents chan *statemachine.EventList
}

// Allocate and return a new workChans structure.
func newWorkChans() workChans {
	return workChans{
		clientIn:        make(chan *statemachine.EventList),
		clientOut:       make(chan *statemachine.EventList),
		stateMachineIn:  make(chan *statemachine.EventList),
		stateMachineOut: make(chan *statemachine.EventList),
		walIn:           make(chan *statemachine.EventList),
		walOut:          make(chan *statemachine.EventList),
		hashIn:          make(chan *statemachine.EventList),
		hashOut:         make(chan *statemachine.EventList),
		netIn:           make(chan *statemachine.EventList),
		netOut:          make(chan *statemachine.EventList),
		appIn:           make(chan *statemachine.EventList),
		appOut:          make(chan *statemachine.EventList),
		reqStoreIn:      make(chan *statemachine.EventList),
		reqStoreOut:     make(chan *statemachine.EventList),

		externalEvents: make(chan *statemachine.EventList),
	}
}

// A function type used for performing the work of a module.
// It usually reads events from a work channel and writes the output to another work channel.
// Any error that occurs while performing the work is returned.
// When the exitC channel is closed the function should return ErrStopped
// TODO: Consider unifying the reading from and writing to channels
//       (currently repeated inside each workFunc) outside of the workFunc.
type workFunc func(exitC <-chan struct{}) error

// Calls the passed work function repeatedly in an infinite loop until the work function returns an non-nil error.
// doUntilErr then sets the error in the Node's workErrNotifier and returns.
func (n *Node) doUntilErr(work workFunc) {
	for {
		err := work(n.workErrNotifier.ExitC())
		if err != nil {
			n.workErrNotifier.Fail(err)
			return
		}
	}
}

// Reads a single list of WAL input events from the corresponding work channel and processes its contents.
// If any results are generated for further processing,
// writes a list of those results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doWALWork(exitC <-chan struct{}) error {
	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.walIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processWALEvents(n.modules.WAL, eventsIn)
	if err != nil {
		return errors.WithMessage(err, "could not process WAL events")
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.walOut <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of Client input events from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doClientWork(exitC <-chan struct{}) error {
	var inputEvents *statemachine.EventList

	// Read input.
	select {
	case inputEvents = <-n.workChans.clientIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	outputEvents, err := processClientEvents(n.clientTracker, inputEvents)
	if err != nil {
		return errors.WithMessage(err, "could not process client events")
	}

	// Return if no output was generated.
	if outputEvents.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.clientOut <- outputEvents:
	case <-exitC:
		return ErrStopped
	}
	return nil
}

// Reads a single list of hash events (hashes to be computed) from the corresponding work channel,
// processes its contents (computes the hashes) and writes a list of hash results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doHashWork(exitC <-chan struct{}) error {
	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.hashIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processHashEvents(n.modules.Hasher, eventsIn)
	if err != nil {
		return errors.WithMessage(err, "could not process hash events")
	}

	// Write output.
	select {
	case n.workChans.hashOut <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of send events from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doNetWork(exitC <-chan struct{}) error {
	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.netIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processNetEvents(n.ID, n.modules.Net, eventsIn)
	if err != nil {
		return errors.WithMessage(err, "could not process net events")
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.netOut <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of app input events from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doAppWork(exitC <-chan struct{}) error {
	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.appIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processAppEvents(n.modules.App, eventsIn)
	if err != nil {
		return errors.WithMessage(err, "could not process app events")
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.appOut <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of request store events from the corresponding work channel and processes its contents.
// If any results are generated for further processing,
// writes a list of those results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doReqStoreWork(exitC <-chan struct{}) error {
	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.reqStoreIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processReqStoreEvents(n.modules.RequestStore, eventsIn)
	if err != nil {
		return errors.WithMessage(err, "could not process reqstore events")
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.reqStoreOut <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of state machine events from the corresponding work channel and processes its contents.
// If any new events are generated by the state machine,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
// On returning, sets the exit status of the state machine in the work error notifier.
func (n *Node) doStateMachineWork(exitC <-chan struct{}) (err error) {
	defer func() {
		if err != nil {
			s, err := n.modules.StateMachine.Status()
			n.workErrNotifier.SetExitStatus(s, err)
		}
	}()

	var eventsIn *statemachine.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.stateMachineIn:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processStateMachineEvents(n.modules.StateMachine, n.modules.Interceptor, eventsIn)
	if err != nil {
		return err
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.stateMachineOut <- eventsOut:
		// Log a special event marking the reception of the generated events from the state machine by the Node.
		if err := n.modules.Interceptor.Intercept(statemachine.EventActionsReceived()); err != nil {
			return err
		}
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// TODO: Document the functions below.

func processWALEvents(wal modules.WAL, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {
	EventsOut := &statemachine.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch event.Type.(type) {
		//case *state.Action_Send:
		//	netActions.PushBack(action)
		//case *state.Action_AppendWriteAhead:
		//	write := t.AppendWriteAhead
		//	if err := wal.Write(write.Index, write.Data); err != nil {
		//		return nil, errors.WithMessagef(err, "failed to write entry to WAL at index %d", write.Index)
		//	}
		//case *state.Action_TruncateWriteAhead:
		//	truncate := t.TruncateWriteAhead
		//	if err := wal.Truncate(truncate.Index); err != nil {
		//		return nil, errors.WithMessagef(err, "failed to truncate WAL to index %d", truncate.Index)
		//	}
		default:
			return nil, errors.Errorf("unexpected type for WAL event: %T", event.Type)
		}
	}

	// Then we sync the WAL
	if err := wal.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failed to sync WAL")
	}

	return EventsOut, nil
}

func processClientEvents(c *clients.ClientTracker, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {

	eventsOut := &statemachine.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		newEvents, err := safeApplyClientEvent(c, event)
		if err != nil {
			return nil, errors.WithMessage(err, "err applying client event")
		}
		eventsOut.PushBackList(newEvents)
	}

	return eventsOut, nil
}

func processHashEvents(hasher modules.Hasher, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {
	eventsOut := &statemachine.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch event.Type.(type) {
		//case *state.Action_Hash:
		//	h := hasher.New()
		//	for _, data := range t.Hash.Data {
		//		h.Write(data)
		//	}
		//
		//	events.HashResult(h.Sum(nil), t.Hash.Origin)
		default:
			return nil, errors.Errorf("unexpected type for Hash event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processNetEvents(selfID uint64, net modules.Net, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {
	eventsOut := &statemachine.EventList{}

	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch event.Type.(type) {
		//case *state.Action_Send:
		//	for _, replica := range t.Send.Targets {
		//		if replica == selfID {
		//			events.Step(replica, t.Send.Msg)
		//		} else {
		//			net.Send(replica, t.Send.Msg)
		//		}
		//	}
		default:
			return nil, errors.Errorf("unexpected type for Net event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processAppEvents(app modules.App, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {
	eventsOut := &statemachine.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		switch event.Type.(type) {
		//case *state.Action_Commit:
		//	if err := app.Apply(t.Commit.Batch); err != nil {
		//		return nil, errors.WithMessage(err, "app failed to commit")
		//	}
		//case *state.Action_Checkpoint:
		//	cp := t.Checkpoint
		//	value, pendingReconf, err := app.Snapshot(cp.NetworkConfig, cp.ClientStates)
		//	if err != nil {
		//		return nil, errors.WithMessage(err, "app failed to generate snapshot")
		//	}
		//	events.CheckpointResult(value, pendingReconf, cp)
		//case *state.Action_StateTransfer:
		//	stateTarget := t.StateTransfer
		//	appState, err := app.TransferTo(stateTarget.SeqNo, stateTarget.Value)
		//	if err != nil {
		//		events.StateTransferFailed(stateTarget)
		//	} else {
		//		events.StateTransferComplete(appState, stateTarget)
		//	}
		default:
			return nil, errors.Errorf("unexpected type for Hash event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processReqStoreEvents(reqStore modules.RequestStore, events *statemachine.EventList) (*statemachine.EventList, error) {
	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func processStateMachineEvents(sm modules.StateMachine, i modules.EventInterceptor, eventsIn *statemachine.EventList) (*statemachine.EventList, error) {
	eventsOut := &statemachine.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		if i != nil {
			err := i.Intercept(event)
			if err != nil {
				return nil, errors.WithMessage(err, "err intercepting event")
			}
		}
		newEvents, err := safeApplySMEvent(sm, event)
		if err != nil {
			return nil, errors.WithMessage(err, "err applying state machine event")
		}
		eventsOut.PushBackList(newEvents)
	}
	if i != nil {
		err := i.Intercept(statemachine.EventActionsReceived())
		if err != nil {
			return nil, errors.WithMessage(err, "err intercepting close event")
		}
	}

	return eventsOut, nil
}

func safeApplySMEvent(sm modules.StateMachine, event *state.Event) (result *statemachine.EventList, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = errors.WithMessage(rErr, "panic in state machine")
			} else {
				err = errors.Errorf("panic in state machine: %v", r)
			}
		}
	}()

	return sm.ApplyEvent(event), nil
}

func safeApplyClientEvent(c *clients.ClientTracker, event *state.Event) (result *statemachine.EventList, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = errors.WithMessage(rErr, "panic in client tracker")
			} else {
				err = errors.Errorf("panic in client tracker: %v", r)
			}
		}
	}()

	return c.ApplyEvent(event), nil
}
