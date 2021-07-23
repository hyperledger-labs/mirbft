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

	// A buffer for storing outstanding events that need to be processed by the node.
	// It contains a separate sub-buffer for each type of event.
	workItems *WorkItems

	// Channels for routing work items between modules.
	// Whenever workItems contains events, those events will be written (by the process() method)
	// to the corresponding channel in workChans. When processed by the corresponding module,
	// the result of that processing (also a list of events) will also be written
	// to the appropriate channel in workChans, from which the process() method moves them to the workItems buffer.
	workChans workChans

	// Used to synchronize the exit of the node's worker go routines.
	workErrNotifier *workErrNotifier

	// Clients that this node considers to be part of the system.
	// TODO: Generalize this to be part of a general "system state", along with the App state.
	clientTracker *clients.ClientTracker

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

		clientTracker: &clients.ClientTracker{Hasher: modules.Hasher},
		//clients: &clients.Clients{
		//	RequestStore: modules.RequestStore,
		//	Hasher:       modules.Hasher,
		//},

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
	// TODO: Re-enable pre-processing.
	//err := preProcess(msg)
	//if err != nil {
	//	return errors.WithMessage(err, "pre-processing message failed")
	//}

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
// clientID and reqNo uniquely identify the request.
// data constitutes the (opaque) payload of the request.
func (n *Node) SubmitRequest(ctx context.Context, clientID uint64, reqNo uint64, data []byte) error {

	// Enqueue the generated events in a work channel to be handled by the processing thread.
	select {
	case n.workChans.clientIn <- (&statemachine.EventList{}).ClientRequest(clientID, reqNo, data):
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
// which mostly consists of routing events between the node's modules.
// Stops and returns when exitC is closed.
// Logical time ticks need to be written to tickC by the calling code.
func (n *Node) process(exitC <-chan struct{}, tickC <-chan time.Time) error {

	var wg sync.WaitGroup // Synchronizes all the worker functions
	defer wg.Wait()

	// Start all worker functions in separate threads.
	// Those functions mostly read events from their respective channels in n.workChans,
	// process them correspondingly, and write the results (also represented as events) in the appropriate channels.
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
		netEvents,
		appEvents,
		reqStoreEvents,
		stateMachineEvents chan<- *statemachine.EventList
	)

	// This loop shovels events between the appropriate channels, until a stopping condition is satisfied.
	for {

		// Wait until any events are ready and write them to the appropriate location.
		select {
		case stateMachineEvents <- n.workItems.StateMachine():
			n.workItems.ClearStateMachine()
			stateMachineEvents = nil
		case walEvents <- n.workItems.WAL():
			n.workItems.ClearWAL()
			walEvents = nil
		case clientEvents <- n.workItems.Client():
			n.workItems.ClearClient()
			clientEvents = nil
		case hashEvents <- n.workItems.Hash():
			n.workItems.ClearHash()
			hashEvents = nil
		case netEvents <- n.workItems.Net():
			n.workItems.ClearNet()
			netEvents = nil
		case appEvents <- n.workItems.App():
			n.workItems.ClearApp()
			appEvents = nil
		case reqStoreEvents <- n.workItems.ReqStore():
			n.workItems.ClearReqStore()
			reqStoreEvents = nil
		case clientOut := <-n.workChans.clientOut:
			n.workItems.AddEvents(clientOut)
		case walOut := <-n.workChans.walOut:
			n.workItems.AddEvents(walOut)
		case hashOut := <-n.workChans.hashOut:
			n.workItems.AddEvents(hashOut)
		case netOut := <-n.workChans.netOut:
			n.workItems.AddEvents(netOut)
		case appOut := <-n.workChans.appOut:
			n.workItems.AddEvents(appOut)
		case reqStoreOut := <-n.workChans.reqStoreOut:
			n.workItems.AddEvents(reqStoreOut)
		case stateMachineOut := <-n.workChans.stateMachineOut:
			n.workItems.AddEvents(stateMachineOut)
		case externalEvents := <-n.workChans.externalEvents:
			// TODO (Jason), once request forwarding works, we'll
			// need to split this into the req store component
			// and 'other' that goes into the result events.
			n.workItems.AddEvents(externalEvents)
		case <-n.workErrNotifier.ExitC():
			return n.workErrNotifier.Err()
		case <-tickC:
			n.workItems.AddEvents((&statemachine.EventList{}).TickElapsed())
		case <-exitC:
			n.workErrNotifier.Fail(ErrStopped)
		}

		// If any events have been added to the work items,
		// update the corresponding channel variables accordingly.

		if stateMachineEvents == nil && n.workItems.StateMachine().Len() > 0 {
			stateMachineEvents = n.workChans.stateMachineIn
		}

		if walEvents == nil && n.workItems.WAL().Len() > 0 {
			walEvents = n.workChans.walIn
		}

		if clientEvents == nil && n.workItems.Client().Len() > 0 {
			clientEvents = n.workChans.clientIn
		}

		if hashEvents == nil && n.workItems.Hash().Len() > 0 {
			hashEvents = n.workChans.hashIn
		}

		if netEvents == nil && n.workItems.Net().Len() > 0 {
			netEvents = n.workChans.netIn
		}

		if appEvents == nil && n.workItems.App().Len() > 0 {
			appEvents = n.workChans.appIn
		}

		if reqStoreEvents == nil && n.workItems.ReqStore().Len() > 0 {
			reqStoreEvents = n.workChans.reqStoreIn
		}
	}
}
