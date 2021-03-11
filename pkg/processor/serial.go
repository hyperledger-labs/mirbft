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

type processor interface {
	queueStateEvents(events *statemachine.EventList)
}

type callbackHasher interface {
	data() [][]byte
	onHashResult(digest []byte)
}

type serialHasher struct {
	hasher Hasher
}

type eventHash struct {
	action    *state.ActionHashRequest
	processor processor
}

func (eh *eventHash) data() [][]byte {
	return eh.action.Data
}

func (eh *eventHash) onHashResult(digest []byte) {
	eh.processor.queueStateEvents(
		(&statemachine.EventList{}).HashResult(digest, eh.action.Origin),
	)
}

type State struct {
	clients       Clients
	ClientWork    ClientWork
	pendingEvents *statemachine.EventList
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
	}
}

type Serial struct {
	State  State
	Config *Config
}

func (s *Serial) queueStateEvents(events *statemachine.EventList) {
	if s.State.pendingEvents == nil {
		s.State.pendingEvents = events
		return
	}
	s.State.pendingEvents.PushBackList(events)
}

func (s *Serial) hash(ch callbackHasher) {
	h := s.Config.Hasher.New()
	for _, data := range ch.data() {
		h.Write(data)
	}

	ch.onHashResult(h.Sum(nil))
}

func (s *Serial) Process(actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	// First we'll handle everything that's not a network send
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			// Skip in the first round
		case *state.Action_Hash:
			s.hash(&eventHash{
				action:    t.Hash,
				processor: s,
			})
		case *state.Action_AppendWriteAhead:
			write := t.AppendWriteAhead
			if err := s.Config.WAL.Write(write.Index, write.Data); err != nil {
				return nil, errors.WithMessagef(err, "failed to write entry to WAL at index %d", write.Index)
			}
		case *state.Action_TruncateWriteAhead:
			truncate := t.TruncateWriteAhead
			if err := s.Config.WAL.Truncate(truncate.Index); err != nil {
				return nil, errors.WithMessagef(err, "failed to truncate WAL to index %d", truncate.Index)
			}
		case *state.Action_Commit:
			if err := s.Config.App.Apply(t.Commit.Batch); err != nil {
				return nil, errors.WithMessage(err, "app failed to commit")
			}
		case *state.Action_Checkpoint:
			cp := t.Checkpoint
			value, pendingReconf, err := s.Config.App.Snap(cp.NetworkConfig, cp.ClientStates)
			if err != nil {
				return nil, errors.WithMessage(err, "app failed to generate snapshot")
			}
			events.CheckpointResult(value, pendingReconf, cp)
		case *state.Action_AllocatedRequest:
			r := t.AllocatedRequest
			client := s.Client(r.ClientId)
			digest, err := client.allocate(r.ReqNo)
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
			client := s.Client(t.CorrectRequest.ClientId)
			err := client.addCorrectDigest(t.CorrectRequest.ReqNo, t.CorrectRequest.Digest)
			if err != nil {
				return nil, err
			}
		case *state.Action_StateApplied:
			// TODO, handle
		case *state.Action_ForwardRequest:
			// XXX address
			/*
			   requestData, err := p.RequestStore.Get(r.RequestAck)
			   if err != nil {
			           panic(fmt.Sprintf("could not store request, unsafe to continue: %s\n", err))
			   }

			   fr := &msgs.Msg{
			           Type: &msgs.Msg_ForwardRequest{
			                   &msgs.ForwardRequest{
			                           RequestAck:  r.RequestAck,
			                           RequestData: requestData,
			                   },
			           },
			   }
			   for _, replica := range r.Targets {
			           if replica == p.Node.Config.ID {
			                   p.Node.Step(context.Background(), replica, fr)
			           } else {
			                   p.Link.Send(replica, fr)
			           }
			   }
			*/
		case *state.Action_StateTransfer:
			stateTarget := t.StateTransfer
			state, err := s.Config.App.TransferTo(stateTarget.SeqNo, stateTarget.Value)
			if err != nil {
				events.StateTransferFailed(stateTarget)
			} else {
				events.StateTransferComplete(state, stateTarget)
			}
		}
	}

	// Then we sync the WAL
	if err := s.Config.WAL.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failted to sync WAL")
	}

	// Then we sync the request store
	if err := s.Config.RequestStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	// Now we transmit
	iter = actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			for _, replica := range t.Send.Targets {
				if replica == s.Config.NodeID {
					events.Step(replica, t.Send.Msg)
				} else {
					s.Config.Link.Send(replica, t.Send.Msg)
				}
			}
		default:
			// We've handled the other types already
		}
	}

	if s.State.pendingEvents != nil {
		events.PushBackList(s.State.pendingEvents)
		s.State.pendingEvents = &statemachine.EventList{}
	}

	return events, nil
}

func (s *Serial) Client(clientID uint64) *Client {
	return s.State.clients.client(clientID, func() *Client {
		return newClient(clientID, s.Config.Hasher, s.Config.RequestStore, &s.State.ClientWork)
	})
}
