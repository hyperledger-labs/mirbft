/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/pkg/errors"
)

// Input and output channels for the modules within the Node.
// the Node.process() method reads and writes actions and events
// to and from these channels to rout them between the Node's modules.
// TODO: Get rid of actions and results and only use events as input and output of all modules.
// TODO: Rename these variables to be consistent with the action/event terminology.
type workChans struct {
	walActions chan *statemachine.ActionList
	walResults chan *statemachine.ActionList

	clientActions chan *statemachine.ActionList
	clientEvents  chan *statemachine.EventList

	hashActions chan *statemachine.ActionList
	hashResults chan *statemachine.EventList

	netActions chan *statemachine.ActionList
	netResults chan *statemachine.EventList

	appActions chan *statemachine.ActionList
	appResults chan *statemachine.EventList

	reqStoreEvents  chan *statemachine.EventList
	reqStoreResults chan *statemachine.EventList

	resultEvents  chan *statemachine.EventList
	resultResults chan *statemachine.ActionList

	externalEvents chan *statemachine.EventList
}

// Allocate and return a new workChans structure.
func newWorkChans() workChans {
	return workChans{
		walActions:      make(chan *statemachine.ActionList),
		walResults:      make(chan *statemachine.ActionList),
		clientActions:   make(chan *statemachine.ActionList),
		clientEvents:    make(chan *statemachine.EventList),
		hashActions:     make(chan *statemachine.ActionList),
		hashResults:     make(chan *statemachine.EventList),
		netActions:      make(chan *statemachine.ActionList),
		netResults:      make(chan *statemachine.EventList),
		appActions:      make(chan *statemachine.ActionList),
		appResults:      make(chan *statemachine.EventList),
		reqStoreEvents:  make(chan *statemachine.EventList),
		reqStoreResults: make(chan *statemachine.EventList),
		resultEvents:    make(chan *statemachine.EventList),
		resultResults:   make(chan *statemachine.ActionList),
		externalEvents:  make(chan *statemachine.EventList),
	}
}

// A function type used for performing the work of a module.
// It usually reads events/actions from a work channel and writes the output to another work channel.
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

// Reads a single list of WAL actions from the corresponding work channel and processes its contents.
// If any results are generated for further processing,
// writes a list of those results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doWALWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList

	// Read input.
	select {
	case actions = <-n.workChans.walActions:
	case <-exitC:
		return ErrStopped
	}

	// Process actions.
	walResults, err := processWALActions(n.modules.WAL, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform WAL actions")
	}

	// Return if no output was generated.
	if walResults.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.walResults <- walResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of Client actions from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doClientWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList

	// Read input.
	select {
	case actions = <-n.workChans.clientActions:
	case <-exitC:
		return ErrStopped
	}

	// Process actions.
	clientEvents, err := processClientActions(n.clients, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform client actions")
	}

	// Return if no output was generated.
	if clientEvents.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.clientEvents <- clientEvents:
	case <-exitC:
		return ErrStopped
	}
	return nil
}

// Reads a single list of hash actions (hashes to be computed) from the corresponding work channel,
// processes its contents (computes the hashes) and writes a list of hash results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doHashWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList

	// Read input.
	select {
	case actions = <-n.workChans.hashActions:
	case <-exitC:
		return ErrStopped
	}

	// Process actions.
	hashResults, err := processHashActions(n.modules.Hasher, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform hash actions")
	}

	// Write output.
	select {
	case n.workChans.hashResults <- hashResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of send actions from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doNetWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList

	// Read input.
	select {
	case actions = <-n.workChans.netActions:
	case <-exitC:
		return ErrStopped
	}

	// Process actions.
	netResults, err := processNetActions(n.ID, n.modules.Net, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform net actions")
	}

	// Return if no output was generated.
	if netResults.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.netResults <- netResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of app actions from the corresponding work channel and processes its contents.
// If any events are generated for further processing,
// writes a list of those events to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
func (n *Node) doAppWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList

	// Read input.
	select {
	case actions = <-n.workChans.appActions:
	case <-exitC:
		return ErrStopped
	}

	// Process actions.
	appResults, err := processAppActions(n.modules.App, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform app actions")
	}

	// Return if no output was generated.
	if appResults.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.appResults <- appResults:
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
	var events *statemachine.EventList

	// Read input.
	select {
	case events = <-n.workChans.reqStoreEvents:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	reqStoreResults, err := processReqStoreEvents(n.modules.RequestStore, events)
	if err != nil {
		return errors.WithMessage(err, "could not perform reqstore actions")
	}

	// Return if no output was generated.
	if reqStoreResults.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.reqStoreResults <- reqStoreResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// Reads a single list of state machine events from the corresponding work channel and processes its contents.
// If any actions are generated by the state machine,
// writes a list of those results to the corresponding work channel.
// If exitC is closed, returns ErrStopped.
// On returning, sets the exit status of the state machine in the work error notifier.
func (n *Node) doStateMachineWork(exitC <-chan struct{}) (err error) {
	defer func() {
		if err != nil {
			s, err := n.modules.StateMachine.Status()
			n.workErrNotifier.SetExitStatus(s, err)
		}
	}()

	var events *statemachine.EventList

	// Read input.
	select {
	case events = <-n.workChans.resultEvents:
	case <-exitC:
		return ErrStopped
	}

	// Process events.
	actions, err := processStateMachineEvents(n.modules.StateMachine, n.modules.Interceptor, events)
	if err != nil {
		return err
	}

	// Return if no output was generated.
	if actions.Len() == 0 {
		return nil
	}

	// Write output.
	select {
	case n.workChans.resultResults <- actions:
		// Log a special event marking the reception of the actions from the state machine by the Node.
		if err := n.modules.Interceptor.Intercept(statemachine.EventActionsReceived()); err != nil {
			return err
		}
	case <-exitC:
		return ErrStopped
	}

	return nil
}

// TODO: Document the functions below.

func processWALActions(wal modules.WAL, actions *statemachine.ActionList) (*statemachine.ActionList, error) {
	netActions := &statemachine.ActionList{}
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			netActions.PushBack(action)
		case *state.Action_AppendWriteAhead:
			write := t.AppendWriteAhead
			if err := wal.Write(write.Index, write.Data); err != nil {
				return nil, errors.WithMessagef(err, "failed to write entry to WAL at index %d", write.Index)
			}
		case *state.Action_TruncateWriteAhead:
			truncate := t.TruncateWriteAhead
			if err := wal.Truncate(truncate.Index); err != nil {
				return nil, errors.WithMessagef(err, "failed to truncate WAL to index %d", truncate.Index)
			}
		default:
			return nil, errors.Errorf("unexpected type for WAL action: %T", action.Type)
		}
	}

	// Then we sync the WAL
	if err := wal.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failed to sync WAL")
	}

	return netActions, nil
}

func processClientActions(c modules.Clients, actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_AllocatedRequest:
			r := t.AllocatedRequest
			client := c.Client(r.ClientId)
			digest, err := client.Allocate(r.ReqNo)
			if err != nil {
				return nil, err
			}

			if digest == nil {
				continue
			}

			events.RequestPersisted(&msgs.RequestAck{
				ClientId: r.ClientId,
				ReqNo:    r.ReqNo,
				Digest:   digest,
			})
		case *state.Action_CorrectRequest:
			client := c.Client(t.CorrectRequest.ClientId)
			err := client.AddCorrectDigest(t.CorrectRequest.ReqNo, t.CorrectRequest.Digest)
			if err != nil {
				return nil, err
			}
		case *state.Action_StateApplied:
			for _, client := range t.StateApplied.NetworkState.Clients {
				c.Client(client.Id).StateApplied(client)
			}
		default:
			return nil, errors.Errorf("unexpected type for client action: %T", action.Type)
		}
	}
	return events, nil
}

func processHashActions(hasher modules.Hasher, actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Hash:
			h := hasher.New()
			for _, data := range t.Hash.Data {
				h.Write(data)
			}

			events.HashResult(h.Sum(nil), t.Hash.Origin)
		default:
			return nil, errors.Errorf("unexpected type for Hash action: %T", action.Type)
		}
	}

	return events, nil
}

func processNetActions(selfID uint64, net modules.Net, actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}

	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			for _, replica := range t.Send.Targets {
				if replica == selfID {
					events.Step(replica, t.Send.Msg)
				} else {
					net.Send(replica, t.Send.Msg)
				}
			}
		default:
			return nil, errors.Errorf("unexpected type for Net action: %T", action.Type)
		}
	}

	return events, nil
}

func processAppActions(app modules.App, actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Commit:
			if err := app.Apply(t.Commit.Batch); err != nil {
				return nil, errors.WithMessage(err, "app failed to commit")
			}
		case *state.Action_Checkpoint:
			cp := t.Checkpoint
			value, pendingReconf, err := app.Snapshot(cp.NetworkConfig, cp.ClientStates)
			if err != nil {
				return nil, errors.WithMessage(err, "app failed to generate snapshot")
			}
			events.CheckpointResult(value, pendingReconf, cp)
		case *state.Action_StateTransfer:
			stateTarget := t.StateTransfer
			appState, err := app.TransferTo(stateTarget.SeqNo, stateTarget.Value)
			if err != nil {
				events.StateTransferFailed(stateTarget)
			} else {
				events.StateTransferComplete(appState, stateTarget)
			}
		default:
			return nil, errors.Errorf("unexpected type for Hash action: %T", action.Type)
		}
	}

	return events, nil
}

func processReqStoreEvents(reqStore modules.RequestStore, events *statemachine.EventList) (*statemachine.EventList, error) {
	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func processStateMachineEvents(sm modules.StateMachine, i modules.EventInterceptor, events *statemachine.EventList) (*statemachine.ActionList, error) {
	actions := &statemachine.ActionList{}
	iter := events.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		if i != nil {
			err := i.Intercept(event)
			if err != nil {
				return nil, errors.WithMessage(err, "err intercepting event")
			}
		}
		events, err := safeApplyEvent(sm, event)
		if err != nil {
			return nil, errors.WithMessage(err, "err applying state machine event")
		}
		actions.PushBackList(events)
	}
	if i != nil {
		err := i.Intercept(statemachine.EventActionsReceived())
		if err != nil {
			return nil, errors.WithMessage(err, "err intercepting close event")
		}
	}

	return actions, nil
}

func safeApplyEvent(sm modules.StateMachine, event *state.Event) (result *statemachine.ActionList, err error) {
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
