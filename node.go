/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/clients"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"github.com/pkg/errors"
	"sync"
	"time"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

// Node is the local instance of MirBFT and the application's interface to the mirbft library.
type Node struct {
	ID     uint64      // Protocol-level node ID
	Config *NodeConfig // Node-level (protocol-independent) configuration, like buffer sizes, logging, ...

	// Implementations of networking, hashing, request store, WAL, etc.
	// The state machine is also a module of the node.
	modules *modules.Modules

	// A buffer for storing outstanding actions and events that need to be processed by the node.
	// It contains a separate sub-buffer for each type of action / event.
	workItems *WorkItems

	// Channels for routing work items between modules.
	// Whenever workItems contains actions or events, those actions/events will be written (by the process() method)
	// to the corresponding channel in workChans. When processed by the corresponding module,
	// the result of that processing (also a list of actions / events) will also be written
	// to the appropriate channel in workChans, from which the process() method moves them to the workItems buffer.
	workChans workChans

	// Used to synchronize the exit of the node's worker go routines.
	workErrNotifier *workErrNotifier

	// Clients that this node considers to be part of the system.
	// TODO: Generalize this to be part of a general "system state", along with the App state.
	clients modules.Clients

	// Channel for receiving status requests.
	// A status request is itself represented as a channel,
	// to which the state machine status needs to be written once the status is obtained.
	// TODO: Implement obtaining and writing the status (Currently no one reads from this channel).
	statusC chan chan *status.StateMachine
}

// NewNode creates a new node with numeric ID id.
// The config parameter specifies Node-level (protocol-independent) configuration, like buffer sizes, logging, ...
// The modules parameter must contain initialized, ready-to-use modules that the new Node will use.
func NewNode(
	id uint64,
	config *NodeConfig,
	modules *modules.Modules,
) (*Node, error) {
	return &Node{
		ID:     id,
		Config: config,

		workChans: newWorkChans(),
		modules:   modules,

		clients: &clients.Clients{
			RequestStore: modules.RequestStore,
			Hasher:       modules.Hasher,
		},

		workItems:       NewWorkItems(),
		workErrNotifier: newWorkErrNotifier(),

		statusC: make(chan chan *status.StateMachine),
	}, nil
}

// Status returns a static snapshot in time of the internal state of the state machine.
// This method necessarily exposes some of the internal architecture of the system, and
// especially while the library is in development, the data structures may change substantially.
// This method returns a nil status and an error if the context ends. If the serializer go routine
// exits for any other reason, then a best effort is made to return the last (and final) status.
// This final status may be relied upon if it is non-nil. If the serializer exited at the user's
// request (because the done channel was closed), then ErrStopped is returned.
func (n *Node) Status(ctx context.Context) (*status.StateMachine, error) {

	//
	statusC := make(chan *status.StateMachine, 1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.ExitStatus()
	case n.statusC <- statusC:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.ExitStatus()
	case s := <-statusC:
		return s, nil
	}
}

// Step inserts a new incoming message msg in the Node.
// The source parameter specifies the numeric ID of the sender of the message.
// The Node assumes the message to be authenticated and it is the caller's responsibility
// to make sure that msg has indeed been sent by source,
// for example by using an authenticated communication channel (e.g. TLS) with the source node.
func (n *Node) Step(ctx context.Context, source uint64, msg *msgs.Msg) error {

	// Pre-process the incoming message and return an error if pre-processing fails.
	err := preProcess(msg)
	if err != nil {
		return errors.WithMessage(err, "pre-processing message failed")
	}

	// Create a Step event
	e := (&statemachine.EventList{}).Step(source, msg)

	// Enqueue event in a work channel to be handled by the processing thread.
	select {
	case n.workChans.externalEvents <- e:
		return nil
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitRequest submits a new client request to the Node.
// clientID and reqNo uniquely identify the request the request.
// data constitutes the (opaque) payload of the request.
func (n *Node) SubmitRequest(ctx context.Context, clientID uint64, reqNo uint64, data []byte) error {

	// TODO: Create a "curried" version of this function
	//       with the client already looked up to avoid lookup on every request.
	c := n.clients.Client(clientID)

	// Insert the new request to the client module, generating events for further processing.
	clEvents, err := c.Propose(reqNo, data)
	if err != nil {
		return err
	}

	// Enqueue the generated events in a work channel to be handled by the processing thread.
	select {
	case n.workChans.clientEvents <- clEvents:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.workErrNotifier.ExitC():
		return n.workErrNotifier.Err()
	}
}

// Run starts the Node.
// It launches the processing of incoming messages, time ticks, and internal events.
// The node stops when exitC is closed.
// Logical time ticks need to be written to tickC by the calling code.
// The function call is blocking and only returns when the node stops.
func (n *Node) Run(exitC <-chan struct{}, tickC <-chan time.Time) error {

	// TODO: Get rid of ProcessAsNewNode and RestartProcessing.
	//       Instead, check the WAL for potential necessary initialization here.
	//       Perform recovery from the WAL here.

	return n.process(exitC, tickC)
}

// Performs all internal work of the node,
// which mostly consists of routing events and actions between the node's modules.
// Stops and returns when exitC is closed.
// Logical time ticks need to be written to tickC by the calling code.
func (n *Node) process(exitC <-chan struct{}, tickC <-chan time.Time) error {

	var wg sync.WaitGroup // Synchronizes all the worker functions
	defer wg.Wait()

	// Start all worker functions in separate threads.
	// Those functions mostly read actions/events from their respective channels in n.workChans,
	// process them correspondingly, and write the results in the appropriate channels.
	// Each workFunc reads a single work item, processes it and writes its results.
	// The looping behavior is implemented in doUntilErr.
	// TODO: Consider unifying the reading from and writing to channels
	//       (currently repeated inside each workFunc) outside of the workFunc.
	for _, work := range []workFunc{
		n.doWALWork,
		n.doClientWork,
		n.doHashWork, // TODO (Jason), spawn more of these
		n.doNetWork,
		n.doAppWork,
		n.doReqStoreWork,
		n.doStateMachineWork,
	} {
		// Each function is executed by a separate thread.
		// The wg is waited on before n.process() returns.
		wg.Add(1)
		go func(work workFunc) {
			wg.Done()
			n.doUntilErr(work)
		}(work)
	}

	// These variables hold the respective channels into which actions are written
	// whenever actions are requested by the node's respective modules.
	// Those variables are set to nil by default, making any writes to them block,
	// preventing them to be selected in the big select statement below.
	// When there are outstanding actions (produced by som module) to be written in a workChan,
	// the workChan is saved in the corresponding channel variable, making it available to the select statement.
	// When writing to the workChan (saved in one of these variables) is selected and the actions written,
	// The variable is set to nil again until new actions are ready.
	// This complicated construction is necessary to prevent writing empty action lists or nil values to the workChans
	// when no actions are pending.
	var walActionsC, clientActionsC, hashActionsC, netActionsC, appActionsC chan<- *statemachine.ActionList

	// Same as above for event channels.
	var reqStoreEventsC, resultEventsC chan<- *statemachine.EventList

	// This loop shovels actions and events between the appropriate channels, until a stopping condition is satisfied.
	for {

		// Wait until any actions/events are ready and write them to the appropriate location.
		select {
		case resultEventsC <- n.workItems.ResultEvents():
			n.workItems.ClearResultEvents()
			resultEventsC = nil
		case walActionsC <- n.workItems.WALActions():
			n.workItems.ClearWALActions()
			walActionsC = nil
		case walResultsC := <-n.workChans.walResults:
			n.workItems.AddWALResults(walResultsC)
		case clientActionsC <- n.workItems.ClientActions():
			n.workItems.ClearClientActions()
			clientActionsC = nil
		case hashActionsC <- n.workItems.HashActions():
			n.workItems.ClearHashActions()
			hashActionsC = nil
		case netActionsC <- n.workItems.NetActions():
			n.workItems.ClearNetActions()
			netActionsC = nil
		case appActionsC <- n.workItems.AppActions():
			n.workItems.ClearAppActions()
			appActionsC = nil
		case reqStoreEventsC <- n.workItems.ReqStoreEvents():
			n.workItems.ClearReqStoreEvents()
			reqStoreEventsC = nil
		case clientResults := <-n.workChans.clientEvents:
			n.workItems.AddClientResults(clientResults)
		case hashResults := <-n.workChans.hashResults:
			n.workItems.AddHashResults(hashResults)
		case netResults := <-n.workChans.netResults:
			n.workItems.AddNetResults(netResults)
		case appResults := <-n.workChans.appResults:
			n.workItems.AddAppResults(appResults)
		case reqStoreResults := <-n.workChans.reqStoreResults:
			n.workItems.AddReqStoreResults(reqStoreResults)
		case actions := <-n.workChans.resultResults:
			n.workItems.AddActions(actions)
		case stepEvents := <-n.workChans.externalEvents:
			// TODO, once request forwarding works, we'll
			// need to split this into the req store component
			// and 'other' that goes into the result events.
			n.workItems.ResultEvents().PushBackList(stepEvents)
		case <-n.workErrNotifier.ExitC():
			return n.workErrNotifier.Err()
		case <-tickC:
			n.workItems.ResultEvents().TickElapsed()
		case <-exitC:
			n.workErrNotifier.Fail(ErrStopped)
		}

		// If any actions/results have been added to the work items,
		// update the corresponding channel variables accordingly.

		if resultEventsC == nil && n.workItems.ResultEvents().Len() > 0 {
			resultEventsC = n.workChans.resultEvents
		}

		if walActionsC == nil && n.workItems.WALActions().Len() > 0 {
			walActionsC = n.workChans.walActions
		}

		if clientActionsC == nil && n.workItems.ClientActions().Len() > 0 {
			clientActionsC = n.workChans.clientActions
		}

		if hashActionsC == nil && n.workItems.HashActions().Len() > 0 {
			hashActionsC = n.workChans.hashActions
		}

		if netActionsC == nil && n.workItems.NetActions().Len() > 0 {
			netActionsC = n.workChans.netActions
		}

		if appActionsC == nil && n.workItems.AppActions().Len() > 0 {
			appActionsC = n.workChans.appActions
		}

		if reqStoreEventsC == nil && n.workItems.ReqStoreEvents().Len() > 0 {
			reqStoreEventsC = n.workChans.reqStoreEvents
		}
	}
}
