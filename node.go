/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"sync"
	"time"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

// Node is the local instance of MirBFT and the application's interface to the mirbft library.
type Node struct {
	ID     t.NodeID    // Protocol-level node ID
	Config *NodeConfig // Node-level (protocol-independent) configuration, like buffer sizes, logging, ...

	// Implementations of networking, hashing, request store, WAL, etc.
	// The state machine is also a module of the node.
	modules *modules.Modules

	// A buffer for storing outstanding events that need to be processed by the node.
	// It contains a separate sub-buffer for each type of event.
	workItems *workItems

	// Channels for routing work items between modules.
	// Whenever workItems contains events, those events will be written (by the process() method)
	// to the corresponding channel in workChans. When processed by the corresponding module,
	// the result of that processing (also a list of events) will also be written
	// to the appropriate channel in workChans, from which the process() method moves them to the workItems buffer.
	workChans workChans

	// Used to synchronize the exit of the node's worker go routines.
	workErrNotifier *workErrNotifier

	// Channel for receiving status requests.
	// A status request is itself represented as a channel,
	// to which the state machine status needs to be written once the status is obtained.
	// TODO: Implement obtaining and writing the status (Currently no one reads from this channel).
	statusC chan chan *statuspb.NodeStatus
}

// NewNode creates a new node with numeric ID id.
// The config parameter specifies Node-level (protocol-independent) configuration, like buffer sizes, logging, ...
// The modules parameter must contain initialized, ready-to-use modules that the new Node will use.
func NewNode(
	id t.NodeID,
	config *NodeConfig,
	m *modules.Modules,
) (*Node, error) {

	// Create default modules for those not specified by the user.
	modulesWithDefaults, err := modules.Defaults(*m)
	if err != nil {
		return nil, err
	}

	// Return a new Node.
	return &Node{
		ID:     id,
		Config: config,

		workChans: newWorkChans(),
		modules:   modulesWithDefaults,

		workItems:       newWorkItems(),
		workErrNotifier: newWorkErrNotifier(),

		statusC: make(chan chan *statuspb.NodeStatus),
	}, nil
}

// Status returns a static snapshot in time of the internal state of the Node.
// TODO: Currently a call to Status blocks until the node is stopped, as obtaining status is not yet implemented.
//       Also change the return type to be a protobuf object that contains a field for each module
//       with module-specific contents.
func (n *Node) Status(ctx context.Context) (*statuspb.NodeStatus, error) {

	// Submit status request for processing by the process() function.
	// A status request is represented as a channel (statusC)
	// to which the state machine status needs to be written once the status is obtained.
	// Return an error if the node shuts down before the request is read or if the context ends.
	statusC := make(chan *statuspb.NodeStatus, 1)
	select {
	case n.statusC <- statusC:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.ExitStatus()
	}

	// Read the obtained status and return it.
	// Return an error if the node shuts down before the request is read or if the context ends.
	select {
	case s := <-statusC:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.ExitStatus()
	}
}

// Step inserts a new incoming message msg in the Node.
// The source parameter specifies the numeric ID of the sender of the message.
// The Node assumes the message to be authenticated and it is the caller's responsibility
// to make sure that msg has indeed been sent by source,
// for example by using an authenticated communication channel (e.g. TLS) with the source node.
func (n *Node) Step(ctx context.Context, source t.NodeID, msg *messagepb.Message) error {

	// Pre-process the incoming message and return an error if pre-processing fails.
	// TODO: Re-enable pre-processing.
	//err := preProcess(msg)
	//if err != nil {
	//	return errors.WithMessage(err, "pre-processing message failed")
	//}

	// Create a MessageReceived event
	e := (&events.EventList{}).PushBack(events.MessageReceived(source, msg))

	// Enqueue event in a work channel to be handled by the processing thread.
	select {
	case n.workChans.workItemInput <- e:
		return nil
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitRequest submits a new client request to the Node.
// clientID and reqNo uniquely identify the request.
// data constitutes the (opaque) payload of the request.
// SubmitRequest is safe to be called concurrently by multiple threads.
func (n *Node) SubmitRequest(
	ctx context.Context,
	clientID t.ClientID,
	reqNo t.ReqNo,
	data []byte,
	authenticator []byte) error {

	// Enqueue the generated events in a work channel to be handled by the processing thread.
	select {
	case n.workChans.workItemInput <- (&events.EventList{}).PushBack(
		events.ClientRequest(clientID, reqNo, data, authenticator),
	):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.workErrNotifier.ExitC():
		return n.workErrNotifier.Err()
	}
}

// Run starts the Node.
// First, it loads the contents of the WAL and enqueues all its contents for processing.
// This makes sure that the WAL events end up first in all the modules' processing queues.
// Then it adds an Init event to the work items, giving the modules the possibility
// to perform additional initialization based on the state recovered from the WAL.
// Run then launches the processing of incoming messages, time ticks, and internal events.
// The node stops when exitC is closed.
// Logical time ticks need to be written to tickC by the calling code.
// The function call is blocking and only returns when the node stops.
func (n *Node) Run(exitC <-chan struct{}, tickC <-chan time.Time) error {

	// If a WAL implementation is available,
	// load the contents of the WAL and enqueue it for processing.
	if n.modules.WAL != nil {
		if err := n.processWAL(); err != nil {
			n.workErrNotifier.Fail(err)
			n.workErrNotifier.SetExitStatus(nil, fmt.Errorf("node not started"))
			return fmt.Errorf("could not process WAL: %w", err)
		}
	}

	// Submit the Init event to the modules.
	if err := n.workItems.AddEvents((&events.EventList{}).PushBack(events.Init())); err != nil {
		n.workErrNotifier.Fail(err)
		n.workErrNotifier.SetExitStatus(nil, fmt.Errorf("node not started"))
		return fmt.Errorf("failed to add init event: %w", err)
	}

	// Start processing of events.
	return n.process(exitC, tickC)
}

// Loads all events stored in the WAL and enqueues them in the node's processing queues.
func (n *Node) processWAL() error {

	// Create empty EventList to hold all the WAL events.
	walEvents := &events.EventList{}

	// Add all events from the WAL to the new EventList.
	if err := n.modules.WAL.LoadAll(func(retIdx t.WALRetIndex, event *eventpb.Event) {
		walEvents.PushBack(events.WALEntry(event, retIdx))
	}); err != nil {
		return fmt.Errorf("could not load WAL events: %w", err)
	}

	// Enqueue all events to the workItems buffers.
	if err := n.workItems.AddEvents(walEvents); err != nil {
		return fmt.Errorf("could not enqueue WAL events for processing: %w", err)
	}

	// If we made it all the way here, no error occurred.
	return nil
}

// Performs all internal work of the node,
// which mostly consists of routing events between the node's modules.
// Stops and returns when exitC is closed.
// Logical time ticks need to be written to tickC by the calling code.
func (n *Node) process(exitC <-chan struct{}, tickC <-chan time.Time) error {

	var wg sync.WaitGroup // Synchronizes all the worker functions
	defer wg.Wait()       // Watch out! If process() terminates unexpectedly (e.g. by panicking), this might get stuck!

	// Start all worker functions in separate threads.
	// Those functions mostly read events from their respective channels in n.workChans,
	// process them correspondingly, and write the results (also represented as events) in the appropriate channels.
	// Each workFunc reads a single work item, processes it and writes its results.
	// The looping behavior is implemented in doUntilErr.
	for _, work := range []workFunc{
		n.doWALWork,
		n.doClientWork,
		n.doHashWork, // TODO (Jason), spawn more of these
		n.doSendingWork,
		n.doAppWork,
		n.doReqStoreWork,
		n.doProtocolWork,
		n.doCryptoWork,
	} {
		// Each function is executed by a separate thread.
		// The wg is waited on before n.process() returns.
		wg.Add(1)
		go func(work workFunc) {
			defer wg.Done()
			n.doUntilErr(work)
		}(work)
	}

	// These variables hold the respective channels into which events consumed by the respective modules are written.
	// Those variables are set to nil by default, making any writes to them block,
	// preventing them from being selected in the big select statement below.
	// When there are outstanding events to be written in a workChan,
	// the workChan is saved in the corresponding channel variable, making it available to the select statement.
	// When writing to the workChan (saved in one of these variables) is selected and the events written,
	// the variable is set to nil again until new events are ready.
	// This complicated construction is necessary to prevent writing empty event lists to the workChans
	// when no events are pending.
	var (
		walEvents,
		clientEvents,
		hashEvents,
		cryptoEvents,
		netEvents,
		appEvents,
		reqStoreEvents,
		protocolEvents chan<- *events.EventList
	)

	// This loop shovels events between the appropriate channels, until a stopping condition is satisfied.
	for {

		// Wait until any events are ready and write them to the appropriate location.
		select {

		// Write pending events to module input channels.
		// This is also the only place events are (potentially) intercepted.
		// Since only a single goroutine executes this loop, the exact sequence of the intercepted events
		// can be replayed later.

		case protocolEvents <- n.workItems.Protocol():
			n.workItems.ClearProtocol()
			protocolEvents = nil
		case walEvents <- n.workItems.WAL():
			n.workItems.ClearWAL()
			walEvents = nil
		case clientEvents <- n.workItems.Client():
			n.workItems.ClearClient()
			clientEvents = nil
		case hashEvents <- n.workItems.Hash():
			n.workItems.ClearHash()
			hashEvents = nil
		case cryptoEvents <- n.workItems.Crypto():
			n.workItems.ClearCrypto()
			cryptoEvents = nil
		case netEvents <- n.workItems.Net():
			n.workItems.ClearNet()
			netEvents = nil
		case appEvents <- n.workItems.App():
			n.workItems.ClearApp()
			appEvents = nil
		case reqStoreEvents <- n.workItems.ReqStore():
			n.workItems.ClearReqStore()
			reqStoreEvents = nil

		// Handle messages received over the network, as obtained by the Net module.

		case receivedMessage := <-n.modules.Net.ReceiveChan():
			if err := n.workItems.AddEvents((&events.EventList{}).
				PushBack(events.MessageReceived(receivedMessage.Sender, receivedMessage.Msg))); err != nil {
				n.workErrNotifier.Fail(err)
			}

		// Add events produced by modules to the workItems buffers and handle logical time.

		case newEvents := <-n.workChans.workItemInput:
			if err := n.workItems.AddEvents(newEvents); err != nil {
				n.workErrNotifier.Fail(err)
			}
		case <-tickC:
			if err := n.workItems.AddEvents((&events.EventList{}).PushBack(events.Tick())); err != nil {
				n.workErrNotifier.Fail(err)
			}

		// Handle termination of the node.

		case <-n.workErrNotifier.ExitC():
			return n.workErrNotifier.Err()
		case <-exitC:
			n.workErrNotifier.Fail(ErrStopped)
		}

		// If any events have been added to the work items,
		// update the corresponding channel variables accordingly.

		if protocolEvents == nil && n.workItems.Protocol().Len() > 0 {
			protocolEvents = n.workChans.protocol
		}
		if walEvents == nil && n.workItems.WAL().Len() > 0 {
			walEvents = n.workChans.wal
		}
		if clientEvents == nil && n.workItems.Client().Len() > 0 {
			clientEvents = n.workChans.clients
		}
		if hashEvents == nil && n.workItems.Hash().Len() > 0 {
			hashEvents = n.workChans.hash
		}
		if cryptoEvents == nil && n.workItems.Crypto().Len() > 0 {
			cryptoEvents = n.workChans.crypto
		}
		if netEvents == nil && n.workItems.Net().Len() > 0 {
			netEvents = n.workChans.net
		}
		if appEvents == nil && n.workItems.App().Len() > 0 {
			appEvents = n.workChans.app
		}
		if reqStoreEvents == nil && n.workItems.ReqStore().Len() > 0 {
			reqStoreEvents = n.workChans.reqStore
		}
	}
}

// If the interceptor module is present, passes events to it. Otherwise, does nothing.
// If an error occurs passing events to the interceptor, notifies the node by means of the workErrorNotifier.
// Note: The passed Events should be free of any follow-up Events,
// as those will be intercepted separately when processed.
// Make sure to call the Strip method of the EventList before passing it to interceptEvents.
func (n *Node) interceptEvents(events *events.EventList) {
	if n.modules.Interceptor != nil {
		if err := n.modules.Interceptor.Intercept(events); err != nil {
			n.workErrNotifier.Fail(err)
		}
	}
}
