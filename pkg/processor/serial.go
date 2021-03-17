/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package processor

import (
	"hash"

	"github.com/pkg/errors"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
)

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
}

func ProcessReqStoreEvents(reqStore RequestStore, events *statemachine.EventList) (*statemachine.EventList, error) {
	// Then we sync the request store
	if err := reqStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	return events, nil
}

func ProcessWALActions(wal WAL, actions *statemachine.ActionList) (*statemachine.ActionList, error) {
	netActions := &statemachine.ActionList{}
	// First we'll handle everything that's not a network send
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
	// First we'll handle everything that's not a network send
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
	// First we'll handle everything that's not a network send
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

type State struct {
	Clients   Clients
	WorkItems *WorkItems
}

type Config struct {
	NodeID       uint64
	Link         Link
	Hasher       Hasher
	App          App
	WAL          WAL
	RequestStore RequestStore
}

func (c *Config) Serial() *Serial {
	return &Serial{
		Config: c,
		State: State{
			Clients: Clients{
				RequestStore: c.RequestStore,
				Hasher:       c.Hasher,
			},
			WorkItems: NewWorkItems(),
		},
	}
}

func (c *Config) Parallel() *Parallel {
	return &Parallel{
		Config: c,
		State: State{
			Clients: Clients{
				RequestStore: c.RequestStore,
				Hasher:       c.Hasher,
			},
			WorkItems: NewWorkItems(),
		},
	}
}

type Serial struct {
	State  State
	Config *Config
}

func (s *Serial) Process() error {
	walResults, err := ProcessWALActions(s.Config.WAL, s.State.WorkItems.WALActions())
	if err != nil {
		return errors.WithMessage(err, "could not perform WAL actions")
	}
	s.State.WorkItems.ClearWALActions()
	s.State.WorkItems.AddWALResults(walResults)

	clientResults, err := s.State.Clients.ProcessClientActions(s.State.WorkItems.ClientActions())
	if err != nil {
		return errors.WithMessage(err, "could not perform client actions")
	}
	s.State.WorkItems.ClearClientActions()
	s.State.WorkItems.AddClientResults(clientResults)

	hashResults, err := ProcessHashActions(s.Config.Hasher, s.State.WorkItems.HashActions())
	if err != nil {
		return errors.WithMessage(err, "could not perform hash actions")
	}
	s.State.WorkItems.ClearHashActions()
	s.State.WorkItems.AddHashResults(hashResults)

	netResults, err := ProcessNetActions(s.Config.NodeID, s.Config.Link, s.State.WorkItems.NetActions())
	if err != nil {
		return errors.WithMessage(err, "could not perform net actions")
	}
	s.State.WorkItems.ClearNetActions()
	s.State.WorkItems.AddNetResults(netResults)

	appResults, err := ProcessAppActions(s.Config.App, s.State.WorkItems.AppActions())
	if err != nil {
		return errors.WithMessage(err, "could not perform app actions")
	}
	s.State.WorkItems.ClearAppActions()
	s.State.WorkItems.AddAppResults(appResults)

	reqStoreResults, err := ProcessReqStoreEvents(s.Config.RequestStore, s.State.WorkItems.ReqStoreEvents())
	if err != nil {
		return errors.WithMessage(err, "could not perform reqstore actions")
	}
	s.State.WorkItems.ClearReqStoreEvents()
	s.State.WorkItems.AddReqStoreResults(reqStoreResults)

	return nil
}

type Parallel struct {
	State  State
	Config *Config
}
