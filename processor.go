/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

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

type Processor struct {
	NodeID       uint64
	Link         Link
	Hasher       Hasher
	App          App
	WAL          WAL
	RequestStore RequestStore
	ClientWork   ClientWork
	clients      Clients
}

func (p *Processor) Process(actions *statemachine.ActionList) (*statemachine.EventList, error) {
	events := &statemachine.EventList{}
	// First we'll handle everything that's not a network send
	iter := actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			// Skip in the first round
		case *state.Action_Hash:
			h := p.Hasher.New()
			for _, data := range t.Hash.Data {
				h.Write(data)
			}

			events.HashResult(h.Sum(nil), t.Hash.Origin)
		case *state.Action_AppendWriteAhead:
			write := t.AppendWriteAhead
			if err := p.WAL.Write(write.Index, write.Data); err != nil {
				return nil, errors.WithMessagef(err, "failed to write entry to WAL at index %d", write.Index)
			}
		case *state.Action_TruncateWriteAhead:
			truncate := t.TruncateWriteAhead
			if err := p.WAL.Truncate(truncate.Index); err != nil {
				return nil, errors.WithMessagef(err, "failed to truncate WAL to index %d", truncate.Index)
			}
		case *state.Action_Commit:
			if err := p.App.Apply(t.Commit.Batch); err != nil {
				return nil, errors.WithMessage(err, "app failed to commit")
			}
		case *state.Action_Checkpoint:
			cp := t.Checkpoint
			value, pendingReconf, err := p.App.Snap(cp.NetworkConfig, cp.ClientStates)
			if err != nil {
				return nil, errors.WithMessage(err, "app failed to generate snapshot")
			}
			events.CheckpointResult(value, pendingReconf, cp)
		case *state.Action_AllocatedRequest:
			r := t.AllocatedRequest
			client := p.Client(r.ClientId)
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
			// We handle this in the client processor... for now
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
			state, err := p.App.TransferTo(stateTarget.SeqNo, stateTarget.Value)
			if err != nil {
				events.StateTransferFailed(stateTarget)
			} else {
				events.StateTransferComplete(state, stateTarget)
			}
		}
	}

	// Then we sync the WAL
	if err := p.WAL.Sync(); err != nil {
		return nil, errors.WithMessage(err, "failted to sync WAL")
	}

	// Then we sync the request store
	if err := p.RequestStore.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync request store, unsafe to continue")
	}

	// Now we transmit
	iter = actions.Iterator()
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			for _, replica := range t.Send.Targets {
				if replica == p.NodeID {
					events.Step(replica, t.Send.Msg)
				} else {
					p.Link.Send(replica, t.Send.Msg)
				}
			}
		default:
			// We've handled the other types already
		}
	}

	return events, nil
}

func (p *Processor) Client(clientID uint64) *Client {
	return p.clients.client(clientID, func() *Client {
		return newClient(clientID, p.Hasher, p.RequestStore, &p.ClientWork)
	})
}
