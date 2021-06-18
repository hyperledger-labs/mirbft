package mirbft

import (
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/pkg/errors"
)

type workChans struct {
	walActions      chan *statemachine.ActionList
	walResults      chan *statemachine.ActionList
	clientActions   chan *statemachine.ActionList
	clientResults   chan *statemachine.EventList
	hashActions     chan *statemachine.ActionList
	hashResults     chan *statemachine.EventList
	netActions      chan *statemachine.ActionList
	netResults      chan *statemachine.EventList
	appActions      chan *statemachine.ActionList
	appResults      chan *statemachine.EventList
	reqStoreEvents  chan *statemachine.EventList
	reqStoreResults chan *statemachine.EventList
	resultEvents    chan *statemachine.EventList
	resultResults   chan *statemachine.ActionList
}

func newWorkChans() workChans {
	return workChans{
		walActions:      make(chan *statemachine.ActionList),
		walResults:      make(chan *statemachine.ActionList),
		clientActions:   make(chan *statemachine.ActionList),
		clientResults:   make(chan *statemachine.EventList),
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
	}
}

type workFunc func(exitC <-chan struct{}) error

func (n *Node) doUntilErr(work workFunc) {
	for {
		err := work(n.workErrNotifier.ExitC())
		if err != nil {
			n.workErrNotifier.Fail(err)
			return
		}
	}
}

func (n *Node) doWALWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-n.workChans.walActions:
	case <-exitC:
		return ErrStopped
	}

	walResults, err := ProcessWALActions(n.modules.WAL, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform WAL actions")
	}

	if walResults.Len() == 0 {
		return nil
	}

	select {
	case n.workChans.walResults <- walResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (n *Node) doClientWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-n.workChans.clientActions:
	case <-exitC:
		return ErrStopped
	}

	clientResults, err := ProcessClientActions(n.clients, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform client actions")
	}

	if clientResults.Len() == 0 {
		return nil
	}

	select {
	case n.workChans.clientResults <- clientResults:
	case <-exitC:
		return ErrStopped
	}
	return nil
}

func (n *Node) doHashWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-n.workChans.hashActions:
	case <-exitC:
		return ErrStopped
	}

	hashResults, err := ProcessHashActions(n.modules.Hasher, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform hash actions")
	}

	select {
	case n.workChans.hashResults <- hashResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (n *Node) doNetWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-n.workChans.netActions:
	case <-exitC:
		return ErrStopped
	}

	netResults, err := ProcessNetActions(n.ID, n.modules.Net, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform net actions")
	}

	select {
	case n.workChans.netResults <- netResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (n *Node) doAppWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-n.workChans.appActions:
	case <-exitC:
		return ErrStopped
	}

	appResults, err := ProcessAppActions(n.modules.App, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform app actions")
	}

	select {
	case n.workChans.appResults <- appResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (n *Node) doReqStoreWork(exitC <-chan struct{}) error {
	var events *statemachine.EventList
	select {
	case events = <-n.workChans.reqStoreEvents:
	case <-exitC:
		return ErrStopped
	}

	reqStoreResults, err := ProcessReqStoreEvents(n.modules.RequestStore, events)
	if err != nil {
		return errors.WithMessage(err, "could not perform reqstore actions")
	}

	select {
	case n.workChans.reqStoreResults <- reqStoreResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (n *Node) doStateMachineWork(exitC <-chan struct{}) (err error) {
	defer func() {
		if err != nil {
			s, err := n.modules.StateMachine.Status()
			n.workErrNotifier.SetExitStatus(s, err)
		}
	}()

	var events *statemachine.EventList
	select {
	case events = <-n.workChans.resultEvents:
	case <-exitC:
		return ErrStopped
	}

	actions, err := ProcessStateMachineEvents(n.modules.StateMachine, n.modules.Interceptor, events)
	if err != nil {
		return err
	}

	if actions.Len() == 0 {
		return nil
	}

	select {
	case n.workChans.resultResults <- actions:
		n.modules.Interceptor.Intercept(statemachine.EventActionsReceived())
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func ProcessWALActions(wal modules.WAL, actions *statemachine.ActionList) (*statemachine.ActionList, error) {
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
		return nil, errors.WithMessage(err, "failted to sync WAL")
	}

	return netActions, nil
}

func ProcessClientActions(c modules.Clients, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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

func ProcessHashActions(hasher modules.Hasher, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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

func ProcessNetActions(selfID uint64, net modules.Net, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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

func ProcessAppActions(app modules.App, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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
			value, pendingReconf, err := app.Snap(cp.NetworkConfig, cp.ClientStates)
			if err != nil {
				return nil, errors.WithMessage(err, "app failed to generate snapshot")
			}
			events.CheckpointResult(value, pendingReconf, cp)
		case *state.Action_StateTransfer:
			stateTarget := t.StateTransfer
			state, err := app.TransferTo(stateTarget.SeqNo, stateTarget.Value)
			if err != nil {
				events.StateTransferFailed(stateTarget)
			} else {
				events.StateTransferComplete(state, stateTarget)
			}
		default:
			return nil, errors.Errorf("unexpected type for Hash action: %T", action.Type)
		}
	}

	return events, nil
}

func ProcessReqStoreEvents(reqStore modules.RequestStore, events *statemachine.EventList) (*statemachine.EventList, error) {
	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func ProcessStateMachineEvents(sm modules.StateMachine, i modules.EventInterceptor, events *statemachine.EventList) (*statemachine.ActionList, error) {
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
