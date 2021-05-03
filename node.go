package mirbft

import (
	"context"
	"github.com/hyperledger-labs/mirbft/pkg/clients"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/processor"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"github.com/pkg/errors"
	"sync"
	"time"
)

// Node is the local instance of MirBFT and the application's interface to the mirbft library.
type Node struct {
	ID     uint64      // Protocol-level node ID
	Config *NodeConfig // Node-level (protocol-independent) configuration, like buffer sizes, logging, ...

	modules         *modules.Modules
	workItems       *WorkItems
	workChans       workChans
	workErrNotifier *workErrNotifier

	clients       modules.Clients
	replicas      *Replicas
	replicaEvents chan *statemachine.EventList

	stateMachine *statemachine.StateMachine

	statusC chan chan *status.StateMachine
}

// NewNode creates a new node.  The processor must be started either by invoking
// node.Processor.StartNewNode with the initial state or by invoking node.Processor.RestartNode.
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
		replicas:      &Replicas{},
		replicaEvents: make(chan *statemachine.EventList),
		stateMachine: &statemachine.StateMachine{
			Logger: logAdapter{Logger: config.Logger},
		},
		workItems:       NewWorkItems(),
		workErrNotifier: newWorkErrNotifier(),

		statusC: make(chan chan *status.StateMachine),
	}, nil
}

// Status returns a static snapshot in time of the internal state of the state machine.
// This method necessarily exposes some of the internal architecture of the system, and
// especially while the library is in development, the data structures may change substantially.
// This method returns a nil status and an error if the context ends.  If the serializer go routine
// exits for any other reason, then a best effort is made to return the last (and final) status.
// This final status may be relied upon if it is non-nil.  If the serializer exited at the user's
// request (because the done channel was closed), then ErrStopped is returned.
func (n *Node) Status(ctx context.Context) (*status.StateMachine, error) {
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

func (n *Node) Step(ctx context.Context, source uint64, msg *msgs.Msg) error {

	err := processor.PreProcess(msg)
	if err != nil {
		return errors.WithMessage(err, "pre-processing message failed")
	}

	e := (&statemachine.EventList{}).Step(source, msg)

	select {
	case n.replicaEvents <- e:
		return nil
	case <-n.workErrNotifier.ExitStatusC():
		return n.workErrNotifier.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) SubmitRequest(ctx context.Context, clientID uint64, reqNo uint64, data []byte) error {

	// TODO: Create a "curried" version of this function
	//       with the client already looked up to avoid lookup on every request.
	c := n.clients.Client(clientID)

	result, err := c.Propose(reqNo, data)
	if err != nil {
		return err
	}

	select {
	case n.workChans.clientResults <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.workErrNotifier.ExitC():
		return n.workErrNotifier.Err()
	}
}

func (n *Node) runtimeParms() *state.EventInitialParameters {
	return &state.EventInitialParameters{
		Id:                   n.ID,
		BatchSize:            n.Config.BatchSize,
		HeartbeatTicks:       n.Config.HeartbeatTicks,
		SuspectTicks:         n.Config.SuspectTicks,
		NewEpochTimeoutTicks: n.Config.NewEpochTimeoutTicks,
		BufferSize:           n.Config.BufferSize,
	}
}

func (n *Node) ProcessAsNewNode(
	exitC <-chan struct{},
	tickC <-chan time.Time,
	initialNetworkState *msgs.NetworkState,
	initialCheckpointValue []byte,
) error {
	events, err := IntializeWALForNewNode(n.modules.WAL, n.runtimeParms(), initialNetworkState, initialCheckpointValue)
	if err != nil {
		n.workErrNotifier.SetExitStatus(nil, errors.Errorf("state machine was not started"))
		return err
	}

	n.workItems.ResultEvents().PushBackList(events)
	return n.process(exitC, tickC)
}

func (n *Node) RestartProcessing(
	exitC <-chan struct{},
	tickC <-chan time.Time,
) error {
	events, err := RecoverWALForExistingNode(n.modules.WAL, n.runtimeParms())
	if err != nil {
		n.workErrNotifier.SetExitStatus(nil, errors.Errorf("state machine was not started"))
		return err
	}

	n.workItems.ResultEvents().PushBackList(events)
	return n.process(exitC, tickC)
}

func (n *Node) process(exitC <-chan struct{}, tickC <-chan time.Time) error {

	var wg sync.WaitGroup // Synchronizes all the worker functions

	// Start all worker functions in separate threads
	for _, work := range []workFunc{
		n.doWALWork,
		n.doClientWork,
		n.doHashWork, // TODO, spawn more of these
		n.doNetWork,
		n.doAppWork,
		n.doReqStoreWork,
		n.doStateMachineWork,
	} {
		wg.Add(1)
		go func(work workFunc) {
			wg.Done()
			n.doUntilErr(work)
		}(work)
	}
	defer wg.Wait()

	var walActionsC, clientActionsC, hashActionsC, netActionsC, appActionsC chan<- *statemachine.ActionList
	var reqStoreEventsC, resultEventsC chan<- *statemachine.EventList

	for {
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
		case clientResults := <-n.workChans.clientResults:
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
			n.workItems.AddStateMachineResults(actions)
		case stepEvents := <-n.replicaEvents:
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

func IntializeWALForNewNode(
	wal modules.WAL,
	runtimeParms *state.EventInitialParameters,
	initialNetworkState *msgs.NetworkState,
	initialCheckpointValue []byte,
) (*statemachine.EventList, error) {
	entries := []*msgs.Persistent{
		{
			Type: &msgs.Persistent_CEntry{
				CEntry: &msgs.CEntry{
					SeqNo:           0,
					CheckpointValue: initialCheckpointValue,
					NetworkState:    initialNetworkState,
				},
			},
		},
		{
			Type: &msgs.Persistent_FEntry{
				FEntry: &msgs.FEntry{
					EndsEpochConfig: &msgs.EpochConfig{
						Number:  0,
						Leaders: initialNetworkState.Config.Nodes,
					},
				},
			},
		},
	}

	events := &statemachine.EventList{}
	events.Initialize(runtimeParms)
	for i, entry := range entries {
		index := uint64(i + 1)
		events.LoadPersistedEntry(index, entry)
		if err := wal.Write(index, entry); err != nil {
			return nil, errors.WithMessagef(err, "failed to write entry to WAL at index %d", index)
		}
	}
	events.CompleteInitialization()

	if err := wal.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failted to sync WAL")
	}
	return events, nil
}

func RecoverWALForExistingNode(wal modules.WAL, runtimeParms *state.EventInitialParameters) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	events.Initialize(runtimeParms)
	if err := wal.LoadAll(func(index uint64, entry *msgs.Persistent) {
		events.LoadPersistedEntry(index, entry)
	}); err != nil {
		return nil, err
	}
	events.CompleteInitialization()
	return events, nil
}
