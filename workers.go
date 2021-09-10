/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/clients"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/pkg/errors"
)

// Input and output channels for the modules within the Node.
// the Node.process() method reads and writes events
// to and from these channels to rout them between the Node's modules.
type workChans struct {

	// There is one channel per module to feed events into the module.
	clients  chan *events.EventList
	protocol chan *events.EventList
	wal      chan *events.EventList
	hash     chan *events.EventList
	net      chan *events.EventList
	app      chan *events.EventList
	reqStore chan *events.EventList

	// All modules write their output events in a common channel, from where the node processor reads and redistributes
	// the events to their respective workItems buffers.
	// External events are also funneled through this channel towards the workItems buffers.
	workItemInput chan *events.EventList
}

// Allocate and return a new workChans structure.
func newWorkChans() workChans {
	return workChans{
		clients:  make(chan *events.EventList),
		protocol: make(chan *events.EventList),
		wal:      make(chan *events.EventList),
		hash:     make(chan *events.EventList),
		net:      make(chan *events.EventList),
		app:      make(chan *events.EventList),
		reqStore: make(chan *events.EventList),

		workItemInput: make(chan *events.EventList),
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
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.wal:
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
	case n.workChans.workItemInput <- eventsOut:
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
	var inputEvents *events.EventList

	// Read input.
	select {
	case inputEvents = <-n.workChans.clients:
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
	case n.workChans.workItemInput <- outputEvents:
	case <-exitC:
		return ErrStopped
	}
	return nil
}

// Reads a single list of hash events (hashes to be computed) from the corresponding work channel,
// processes its contents (computes the hashes) and writes a list of hash results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doHashWork(exitC <-chan struct{}) error {
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.hash:
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
	case n.workChans.workItemInput <- eventsOut:
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
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.net:
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
	case n.workChans.workItemInput <- eventsOut:
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
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.app:
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
	case n.workChans.workItemInput <- eventsOut:
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
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.reqStore:
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
	case n.workChans.workItemInput <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of protocol events from the corresponding work channel and processes its contents.
// If any new events are generated by the protocol state machine,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
// On returning, sets the exit status of the protocol state machine in the work error notifier.
func (n *Node) doProtocolWork(exitC <-chan struct{}) (err error) {
	defer func() {
		if err != nil {
			s, err := n.modules.Protocol.Status()
			n.workErrNotifier.SetExitStatus(s, err)
		}
	}()

	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.protocol:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processProtocolEvents(n.modules.Protocol, eventsIn)
	if err != nil {
		return err
	}

	// Return if no output was generated.
	if eventsOut.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.workItemInput <- eventsOut:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// TODO: Document the functions below.

func processWALEvents(wal modules.WAL, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()

	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		// Perform the necessary action based on event type.
		switch event.Type.(type) {
		case *eventpb.Event_PersistDummyBatch:
			if err := wal.Append(event); err != nil {
				return nil, fmt.Errorf("could not persist dummy batch: %w", err)
			}

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

	return eventsOut, nil
}

func processClientEvents(c *clients.ClientTracker, eventsIn *events.EventList) (*events.EventList, error) {

	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		newEvents, err := safeApplyClientEvent(c, event)
		if err != nil {
			return nil, errors.WithMessage(err, "err applying client event")
		}
		eventsOut.PushBackList(newEvents)
	}

	return eventsOut, nil
}

func processHashEvents(hasher modules.Hasher, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		switch e := event.Type.(type) {
		case *eventpb.Event_HashRequest:
			// HashRequest is the only event understood by the hasher module.
			// Hash all the data and create a hashResult event.
			h := hasher.New()
			for _, data := range e.HashRequest.Data {
				h.Write(data)
			}
			eventsOut.PushBack(events.HashResult(h.Sum(nil), e.HashRequest.Origin))
		default:
			// Complain about all other incoming event types.
			return nil, errors.Errorf("unexpected type for Hash event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processNetEvents(selfID uint64, net modules.Net, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}

	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		switch e := event.Type.(type) {
		case *eventpb.Event_SendMessage:
			for _, destId := range e.SendMessage.Destinations {
				if destId == selfID {
					eventsOut.PushBack(events.MessageReceived(selfID, e.SendMessage.Msg))
				} else {
					net.Send(destId, e.SendMessage.Msg)
				}
			}

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

func processAppEvents(app modules.App, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		switch e := event.Type.(type) {
		case *eventpb.Event_AnnounceDummyBatch:
			if err := app.Apply(e.AnnounceDummyBatch.Batch); err != nil {
				return nil, fmt.Errorf("app error: %w", err)
			}
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

func processReqStoreEvents(reqStore modules.RequestStore, events *events.EventList) (*events.EventList, error) {

	// TODO: IMPLEMENT THIS!

	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func processProtocolEvents(sm modules.Protocol, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		newEvents, err := safeApplySMEvent(sm, event)
		if err != nil {
			return nil, errors.WithMessage(err, "error applying protocol event")
		}
		eventsOut.PushBackList(newEvents)
	}

	return eventsOut, nil
}

func safeApplySMEvent(sm modules.Protocol, event *eventpb.Event) (result *events.EventList, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = errors.WithMessage(rErr, "panic in protocol state machine")
			} else {
				err = errors.Errorf("panic in protocol state machine: %v", r)
			}
		}
	}()

	return sm.ApplyEvent(event), nil
}

func safeApplyClientEvent(c *clients.ClientTracker, event *eventpb.Event) (result *events.EventList, err error) {
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
