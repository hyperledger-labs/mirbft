/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package processor

import (
	"hash"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/IBM/mirbft/pkg/eventlog"
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
)

var ErrStopped = errors.Errorf("stopped")

type Hasher interface {
	New() hash.Hash
}

type Link interface {
	Send(dest uint64, msg *msgs.Msg)
}

type App interface {
	Apply(*msgs.QEntry) error
	Snap(networkConfig *msgs.NetworkState_Config, clientsState []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error)
	TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error)
}

type RequestStore interface {
	GetAllocation(clientID, reqNo uint64) ([]byte, error)
	PutAllocation(clientID, reqNo uint64, digest []byte) error
	GetRequest(requestAck *msgs.RequestAck) ([]byte, error)
	PutRequest(requestAck *msgs.RequestAck, data []byte) error
	Sync() error
}

type WAL interface {
	Write(index uint64, entry *msgs.Persistent) error
	Truncate(index uint64) error
	Sync() error
	LoadAll(forEach func(index uint64, p *msgs.Persistent)) error
}

func ProcessReqStoreEvents(reqStore RequestStore, events *statemachine.EventList) (*statemachine.EventList, error) {
	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func IntializeWALForNewNode(
	wal WAL,
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

func RecoverWALForExistingNode(wal WAL, runtimeParms *state.EventInitialParameters) (*statemachine.EventList, error) {
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

func ProcessWALActions(wal WAL, actions *statemachine.ActionList) (*statemachine.ActionList, error) {
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

func ProcessNetActions(selfID uint64, link Link, actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}

	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			for _, replica := range t.Send.Targets {
				if replica == selfID {
					events.Step(replica, t.Send.Msg)
				} else {
					link.Send(replica, t.Send.Msg)
				}
			}
		default:
			return nil, errors.Errorf("unexpected type for Net action: %T", action.Type)
		}
	}

	return events, nil
}

func ProcessHashActions(hasher Hasher, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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

func ProcessAppActions(app App, actions *statemachine.ActionList) (*statemachine.EventList, error) {
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

type Config struct {
	NodeID       uint64
	Link         Link
	Hasher       Hasher
	App          App
	WAL          WAL
	RequestStore RequestStore
}

func (c *Config) Processor(interceptor *eventlog.Recorder, logger statemachine.Logger) *Processor {
	return &Processor{
		Config: c,
		Clients: &Clients{
			RequestStore: c.RequestStore,
			Hasher:       c.Hasher,
		},
		WorkItems: NewWorkItems(),
		StateMachine: &statemachine.StateMachine{
			Logger: logger,
		},

		workErrNotifier: newWorkErrNotifier(),
		interceptor:     interceptor,

		walActionsC:      make(chan *statemachine.ActionList),
		walResultsC:      make(chan *statemachine.ActionList),
		clientActionsC:   make(chan *statemachine.ActionList),
		clientResultsC:   make(chan *statemachine.EventList),
		hashActionsC:     make(chan *statemachine.ActionList),
		hashResultsC:     make(chan *statemachine.EventList),
		netActionsC:      make(chan *statemachine.ActionList),
		netResultsC:      make(chan *statemachine.EventList),
		appActionsC:      make(chan *statemachine.ActionList),
		appResultsC:      make(chan *statemachine.EventList),
		reqStoreEventsC:  make(chan *statemachine.EventList),
		reqStoreResultsC: make(chan *statemachine.EventList),
		ResultEventsC:    make(chan *statemachine.EventList),
		ResultResultsC:   make(chan *statemachine.ActionList),
	}
}

type workErrNotifier struct {
	mutex sync.Mutex
	err   error
	exitC chan struct{}
}

func newWorkErrNotifier() *workErrNotifier {
	return &workErrNotifier{
		exitC: make(chan struct{}),
	}
}

func (wen *workErrNotifier) Err() error {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	return wen.err
}

func (wen *workErrNotifier) Fail(err error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	if wen.err != nil {
		return
	}
	wen.err = err
	close(wen.exitC)
}

func (wen *workErrNotifier) ExitC() <-chan struct{} {
	return wen.exitC
}

type Processor struct {
	Config       *Config
	Clients      *Clients
	WorkItems    *WorkItems
	StateMachine *statemachine.StateMachine

	workErrNotifier *workErrNotifier
	interceptor     *eventlog.Recorder

	walActionsC      chan *statemachine.ActionList
	walResultsC      chan *statemachine.ActionList
	clientActionsC   chan *statemachine.ActionList
	clientResultsC   chan *statemachine.EventList
	hashActionsC     chan *statemachine.ActionList
	hashResultsC     chan *statemachine.EventList
	netActionsC      chan *statemachine.ActionList
	netResultsC      chan *statemachine.EventList
	appActionsC      chan *statemachine.ActionList
	appResultsC      chan *statemachine.EventList
	reqStoreEventsC  chan *statemachine.EventList
	reqStoreResultsC chan *statemachine.EventList

	// Exported as a temporary hack
	ResultEventsC  chan *statemachine.EventList
	ResultResultsC chan *statemachine.ActionList
}

func (p *Processor) Err() <-chan struct{} {
	return p.workErrNotifier.ExitC()
}

func (p *Processor) doWALWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-p.walActionsC:
	case <-exitC:
		return ErrStopped
	}

	walResults, err := ProcessWALActions(p.Config.WAL, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform WAL actions")
	}

	if walResults.Len() == 0 {
		return nil
	}

	select {
	case p.walResultsC <- walResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doClientWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-p.clientActionsC:
	case <-exitC:
		return ErrStopped
	}

	clientResults, err := p.Clients.ProcessClientActions(actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform client actions")
	}

	if clientResults.Len() == 0 {
		return nil
	}

	select {
	case p.clientResultsC <- clientResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doHashWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-p.hashActionsC:
	case <-exitC:
		return ErrStopped
	}

	hashResults, err := ProcessHashActions(p.Config.Hasher, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform hash actions")
	}

	select {
	case p.hashResultsC <- hashResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doNetWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-p.netActionsC:
	case <-exitC:
		return ErrStopped
	}

	netResults, err := ProcessNetActions(p.Config.NodeID, p.Config.Link, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform net actions")
	}

	select {
	case p.netResultsC <- netResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doAppWork(exitC <-chan struct{}) error {
	var actions *statemachine.ActionList
	select {
	case actions = <-p.appActionsC:
	case <-exitC:
		return ErrStopped
	}

	appResults, err := ProcessAppActions(p.Config.App, actions)
	if err != nil {
		return errors.WithMessage(err, "could not perform app actions")
	}

	select {
	case p.appResultsC <- appResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doReqStoreWork(exitC <-chan struct{}) error {
	var events *statemachine.EventList
	select {
	case events = <-p.reqStoreEventsC:
	case <-exitC:
		return ErrStopped
	}

	reqStoreResults, err := ProcessReqStoreEvents(p.Config.RequestStore, events)
	if err != nil {
		return errors.WithMessage(err, "could not perform reqstore actions")
	}

	select {
	case p.reqStoreResultsC <- reqStoreResults:
	case <-exitC:
		return ErrStopped
	}

	return nil
}

func (p *Processor) doStateMachineWork(exitC <-chan struct{}) (err error) {
	var events *statemachine.EventList
	select {
	case events = <-p.ResultEventsC:
	case <-exitC:
		return ErrStopped
	}

	actions := &statemachine.ActionList{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if rErr, ok := r.(error); ok {
					err = errors.WithMessage(rErr, "panic in state machine")
				} else {
					err = errors.Errorf("panic in state machine: %v", r)
				}
			}
		}()
		iter := events.Iterator()
		for event := iter.Next(); event != nil; event = iter.Next() {
			p.interceptor.Intercept(event)
			actions.PushBackList(p.StateMachine.ApplyEvent(event))
		}
	}()

	if actions.Len() == 0 {
		return nil
	}

	select {
	case p.ResultResultsC <- actions:
		p.interceptor.Intercept(statemachine.EventActionsReceived())
	case <-exitC:
		return ErrStopped
	}

	return nil
}

type workFunc func(exitC <-chan struct{}) error

func (p *Processor) doUntilErr(work workFunc) {
	for {
		err := work(p.workErrNotifier.ExitC())
		if err != nil {
			p.workErrNotifier.Fail(err)
			return
		}
	}
}

func (p *Processor) ProcessAsNewNode(
	exitC <-chan struct{},
	tickC <-chan time.Time,
	runtimeParms *state.EventInitialParameters,
	initialNetworkState *msgs.NetworkState,
	initialCheckpointValue []byte,
) error {
	events, err := IntializeWALForNewNode(p.Config.WAL, runtimeParms, initialNetworkState, initialCheckpointValue)
	if err != nil {
		return err
	}

	p.WorkItems.ResultEvents().PushBackList(events)
	return p.process(exitC, tickC)
}

func (p *Processor) RestartProcessing(
	exitC <-chan struct{},
	tickC <-chan time.Time,
	runtimeParms *state.EventInitialParameters,
) error {
	events, err := RecoverWALForExistingNode(p.Config.WAL, runtimeParms)
	if err != nil {
		return err
	}

	p.WorkItems.ResultEvents().PushBackList(events)
	return p.process(exitC, tickC)
}

func (p *Processor) process(exitC <-chan struct{}, tickC <-chan time.Time) error {
	var wg sync.WaitGroup
	for _, work := range []workFunc{
		p.doWALWork,
		p.doClientWork,
		p.doHashWork,
		p.doNetWork,
		p.doAppWork,
		p.doReqStoreWork,
		p.doStateMachineWork,
	} {
		wg.Add(1)
		go func(work workFunc) {
			wg.Done()
			p.doUntilErr(work)
		}(work)
	}
	defer wg.Wait()

	var walActionsC, clientActionsC, hashActionsC, netActionsC, appActionsC chan<- *statemachine.ActionList
	var reqStoreEventsC, resultEventsC chan<- *statemachine.EventList

	for {
		select {
		case resultEventsC <- p.WorkItems.ResultEvents():
			p.WorkItems.ClearResultEvents()
			resultEventsC = nil
		case walActionsC <- p.WorkItems.WALActions():
			p.WorkItems.ClearWALActions()
			walActionsC = nil
		case walResultsC := <-p.walResultsC:
			p.WorkItems.AddWALResults(walResultsC)
		case clientActionsC <- p.WorkItems.ClientActions():
			p.WorkItems.ClearClientActions()
			clientActionsC = nil
		case hashActionsC <- p.WorkItems.HashActions():
			p.WorkItems.ClearHashActions()
			hashActionsC = nil
		case netActionsC <- p.WorkItems.NetActions():
			p.WorkItems.ClearNetActions()
			netActionsC = nil
		case appActionsC <- p.WorkItems.AppActions():
			p.WorkItems.ClearAppActions()
			appActionsC = nil
		case reqStoreEventsC <- p.WorkItems.ReqStoreEvents():
			p.WorkItems.ClearReqStoreEvents()
			reqStoreEventsC = nil
		case clientResults := <-p.clientResultsC:
			p.WorkItems.AddClientResults(clientResults)
		case hashResults := <-p.hashResultsC:
			p.WorkItems.AddHashResults(hashResults)
		case netResults := <-p.netResultsC:
			p.WorkItems.AddNetResults(netResults)
		case appResults := <-p.appResultsC:
			p.WorkItems.AddAppResults(appResults)
		case reqStoreResults := <-p.reqStoreResultsC:
			p.WorkItems.AddReqStoreResults(reqStoreResults)
		case actions := <-p.ResultResultsC:
			p.WorkItems.AddStateMachineResults(actions)
		case <-p.workErrNotifier.ExitC():
			return p.workErrNotifier.Err()
		case <-tickC:
			p.WorkItems.ResultEvents().TickElapsed()
		case <-exitC:
			p.workErrNotifier.Fail(ErrStopped)
		}

		if resultEventsC == nil && p.WorkItems.ResultEvents().Len() > 0 {
			resultEventsC = p.ResultEventsC
		}

		if walActionsC == nil && p.WorkItems.WALActions().Len() > 0 {
			walActionsC = p.walActionsC
		}

		if clientActionsC == nil && p.WorkItems.ClientActions().Len() > 0 {
			clientActionsC = p.clientActionsC
		}

		if hashActionsC == nil && p.WorkItems.HashActions().Len() > 0 {
			hashActionsC = p.hashActionsC
		}

		if netActionsC == nil && p.WorkItems.NetActions().Len() > 0 {
			netActionsC = p.netActionsC
		}

		if appActionsC == nil && p.WorkItems.AppActions().Len() > 0 {
			appActionsC = p.appActionsC
		}

		if reqStoreEventsC == nil && p.WorkItems.ReqStoreEvents().Len() > 0 {
			reqStoreEventsC = p.reqStoreEventsC
		}
	}
}
