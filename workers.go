/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"github.com/pkg/errors"
	"runtime/debug"
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
	crypto   chan *events.EventList
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
		crypto:   make(chan *events.EventList),
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
	outputEvents, err := processClientEvents(n.modules.ClientTracker, inputEvents)
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

// Reads a single list of crypto events (e.g. signatures to be computed or verified)
// from the corresponding work channel, processes its contents
// and writes a list of resulting events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doCryptoWork(exitC <-chan struct{}) error {
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.crypto:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processCryptoEvents(n.modules.Crypto, eventsIn)
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

// Reads a single list of send events from the corresponding work channel and processes its contents
// by sending the contained messages over the network.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doSendingWork(exitC <-chan struct{}) error {
	var eventsIn *events.EventList

	// Read input.
	select {
	case eventsIn = <-n.workChans.net:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	eventsOut, err := processSendEvents(n.ID, n.modules.Net, eventsIn)
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
			n.workErrNotifier.SetExitStatus(&statuspb.NodeStatus{Protocol: s}, err)
			// TODO: Clean up status-related code.
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
		switch e := event.Type.(type) {
		case *eventpb.Event_WalAppend:
			if err := wal.Append(e.WalAppend.Event, t.WALRetIndex(e.WalAppend.RetentionIndex)); err != nil {
				return nil, fmt.Errorf("could not persist event (retention index %d) to WAL: %w",
					e.WalAppend.RetentionIndex, err)
			}
		case *eventpb.Event_PersistDummyBatch:
			if err := wal.Append(event, 0); err != nil {
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
			return nil, errors.Errorf("unexpected type of WAL event: %T", event.Type)
		}
	}

	// Then we sync the WAL
	if err := wal.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failed to sync WAL")
	}

	return eventsOut, nil
}

func processClientEvents(c modules.ClientTracker, eventsIn *events.EventList) (*events.EventList, error) {

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
			return nil, errors.Errorf("unexpected type of Hash event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processCryptoEvents(crypto modules.Crypto, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		switch e := event.Type.(type) {
		case *eventpb.Event_VerifyRequestSig:
			// Verify client request signature.
			// The signature is only computed (and verified) over the digest of a request.
			// The other fields can safely be ignored.

			// Convenience variable
			reqRef := e.VerifyRequestSig.RequestRef

			// Verify signature.
			err := crypto.VerifyClientSig(
				[][]byte{reqRef.Digest},
				e.VerifyRequestSig.Signature,
				t.ClientID(reqRef.ClientId))

			// Create result event, depending on verification outcome.
			if err == nil {
				eventsOut.PushBack(events.RequestSigVerified(reqRef, true, ""))
			} else {
				eventsOut.PushBack(events.RequestSigVerified(reqRef, false, err.Error()))
			}
		default:
			// Complain about all other incoming event types.
			return nil, errors.Errorf("unexpected type of Crypto event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processSendEvents(selfID t.NodeID, net modules.Net, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}

	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		switch e := event.Type.(type) {
		case *eventpb.Event_SendMessage:
			for _, destId := range e.SendMessage.Destinations {
				if t.NodeID(destId) == selfID {
					eventsOut.PushBack(events.MessageReceived(selfID, e.SendMessage.Msg))
				} else {
					net.Send(t.NodeID(destId), e.SendMessage.Msg)
				}
			}
		default:
			return nil, errors.Errorf("unexpected type of Net event: %T", event.Type)
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
		case *eventpb.Event_Deliver:
			if err := app.Apply(e.Deliver.Batch); err != nil {
				return nil, fmt.Errorf("app batch delivery error: %w", err)
			}
		case *eventpb.Event_AppSnapshotRequest:
			if data, err := app.Snapshot(); err != nil {
				return nil, fmt.Errorf("app snapshot error: %w", err)
			} else {
				return (&events.EventList{}).PushBack(events.AppSnapshot(t.SeqNr(e.AppSnapshotRequest.Sn), data)), nil
			}
		default:
			return nil, errors.Errorf("unexpected type of App event: %T", event.Type)
		}
	}

	return eventsOut, nil
}

func processReqStoreEvents(reqStore modules.RequestStore, eventsIn *events.EventList) (*events.EventList, error) {
	eventsOut := &events.EventList{}
	iter := eventsIn.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {

		// Remove the follow-up events from event and add them directly to the output.
		eventsOut.PushBackList(events.Strip(event))

		// Process event based on its type.
		switch e := event.Type.(type) {
		case *eventpb.Event_StoreVerifiedRequest:
			storeEvent := e.StoreVerifiedRequest

			// Store request data.
			if err := reqStore.PutRequest(storeEvent.RequestRef, storeEvent.Data); err != nil {
				return nil, fmt.Errorf("cannot store request (c%dr%d) data: %w",
					storeEvent.RequestRef.ClientId,
					storeEvent.RequestRef.ReqNo,
					err)
			}

			// Mark request as authenticated.
			if err := reqStore.SetAuthenticated(storeEvent.RequestRef); err != nil {
				return nil, fmt.Errorf("cannot mark request (c%dr%d) as authenticated: %w",
					storeEvent.RequestRef.ClientId,
					storeEvent.RequestRef.ReqNo,
					err)
			}

			// Store request authenticator.
			if err := reqStore.PutAuthenticator(storeEvent.RequestRef, storeEvent.Authenticator); err != nil {
				return nil, fmt.Errorf("cannot store authenticator (c%dr%d) of request: %w",
					storeEvent.RequestRef.ClientId,
					storeEvent.RequestRef.ReqNo,
					err)
			}

		case *eventpb.Event_StoreDummyRequest:
			storeEvent := e.StoreDummyRequest // Helper variable for convenience

			// Store request data.
			if err := reqStore.PutRequest(storeEvent.RequestRef, storeEvent.Data); err != nil {
				return nil, fmt.Errorf("cannot store dummy request data: %w", err)
			}

			// Mark request as authenticated.
			if err := reqStore.SetAuthenticated(storeEvent.RequestRef); err != nil {
				return nil, fmt.Errorf("cannot mark dummy request as authenticated: %w", err)
			}

			// Associate a dummy authenticator with the request
			if err := reqStore.PutAuthenticator(storeEvent.RequestRef, []byte{0}); err != nil {
				return nil, fmt.Errorf("cannot store authenticator of dummy request: %w", err)
			}

			eventsOut.PushBack(events.RequestReady(storeEvent.RequestRef))
		}
	}

	// Then sync the request store, ensuring that all updates to its state are persisted.
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return eventsOut, nil
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
				err = fmt.Errorf("panic in protocol state machine: %w\nStack trace:\n%s", rErr, string(debug.Stack()))
			} else {
				err = fmt.Errorf("panic in protocol state machine: %v\nStack trace:\n%s", r, string(debug.Stack()))
			}
		}
	}()

	return sm.ApplyEvent(event), nil
}

func safeApplyClientEvent(c modules.ClientTracker, event *eventpb.Event) (result *events.EventList, err error) {

	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = fmt.Errorf("panic in client tracker: %w\nStack trace:\n%s", rErr, string(debug.Stack()))
			} else {
				err = fmt.Errorf("panic in client tracker: %v\nStack trace:\n%s", r, string(debug.Stack()))
			}
		}
	}()

	return c.ApplyEvent(event), nil
}
