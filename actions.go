/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
)

func toActions(a *statemachine.ActionList) (*statemachine.ActionList, *ClientActions) {
	iter := a.Iterator()
	aResult := &statemachine.ActionList{}
	caResult := &ClientActions{}
	for action := iter.Next(); action != nil; action = iter.Next() {
		switch t := action.Type.(type) {
		case *state.Action_Send:
			aResult.PushBack(action)
		case *state.Action_Hash:
			aResult.PushBack(action)
		case *state.Action_TruncateWriteAhead:
			aResult.PushBack(action)
		case *state.Action_AppendWriteAhead:
			aResult.PushBack(action)
		case *state.Action_AllocatedRequest:
			caResult.AllocatedRequests = append(caResult.AllocatedRequests, RequestSlot{
				ClientID: t.AllocatedRequest.ClientId,
				ReqNo:    t.AllocatedRequest.ReqNo,
			})
		case *state.Action_CorrectRequest:
			caResult.CorrectRequests = append(caResult.CorrectRequests, t.CorrectRequest)
		case *state.Action_ForwardRequest:
			caResult.ForwardRequests = append(caResult.ForwardRequests, Forward{
				Targets:    t.ForwardRequest.Targets,
				RequestAck: t.ForwardRequest.Ack,
			})
		case *state.Action_Commit:
			aResult.PushBack(action)
		case *state.Action_Checkpoint:
			aResult.PushBack(action)
		case *state.Action_StateTransfer:
			aResult.PushBack(action)
		default:
			panic(fmt.Sprintf("unhandled type: %T", t))
		}
	}

	return aResult, caResult
}

// Actions are the responsibility of the library user to fulfill.
// The user receives a set of Actions from a read of *Node.Ready(),
// and it is the user's responsibility to execute all actions, returning
// ActionResults to the *Node.AddResults call.
// TODO add details about concurrency
type Actions struct {
	// Send messages should be sent to every node in the cluster (including yourself).
	Send []Send

	// Hash is a set of requests to be hashed.  Hash can (and usually should) be done
	// in parallel with persisting to disk and performing network sends.
	Hash []*HashRequest

	// WriteAhead contains instructions for writing the write-ahead-log.
	// It is critical for safety that the write-ahead-log actions are taken before
	// transmitting data to the network.  The actions are either to persist a log
	// entry to the next index in the log, or, to truncate the write-ahead-log to
	// the given index.  It is essential that these operations take place in order
	// and in a stable way, as this log is replayed at startup to resume operation.
	WriteAhead []*Write

	// Commits is a set of batches which have achieved final order and are ready to commit.
	// They will have previously persisted via QEntries.  When the user processes a commit,
	// if that commit contains a checkpoint, the user must return a checkpoint result for
	// this commit.  Checkpoints must be persisted before further commits are reported as applied.
	Commits []*Commit

	// StateTransfer is set when the node has fallen sufficiently out of sync with the network
	// such that it must receive the current application and network state from another
	// replica in the network.  The consumer _must_ report success or failure but may
	// continue to process other actions in the interim.
	StateTransfer *StateTarget
}

func (a *Actions) persist(index uint64, p *msgs.Persistent) *Actions {
	a.WriteAhead = append(a.WriteAhead,
		&Write{
			Append: &WALEntry{
				Index: index,
				Data:  p,
			},
		})
	return a
}

// clear nils out all of the fields.
func (a *Actions) clear() {
	a.Send = nil
	a.Hash = nil
	a.WriteAhead = nil
	a.Commits = nil
	a.StateTransfer = nil
}

func (a *Actions) isEmpty() bool {
	return len(a.Send) == 0 &&
		len(a.Hash) == 0 &&
		len(a.WriteAhead) == 0 &&
		len(a.Commits) == 0 &&
		a.StateTransfer == nil
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) concat(o *Actions) *Actions {
	a.Send = append(a.Send, o.Send...)
	a.Commits = append(a.Commits, o.Commits...)
	a.Hash = append(a.Hash, o.Hash...)
	a.WriteAhead = append(a.WriteAhead, o.WriteAhead...)
	if o.StateTransfer != nil {
		if a.StateTransfer != nil {
			panic("attempted to concatenate two concurrent state transfer requests")
		}
		a.StateTransfer = o.StateTransfer
	}
	return a
}

type ClientActions struct {
	// AllocatedRequests is a set of client request numbers which are eligible for
	// clients to begin filling.  It is the responsibility of the consumer to ensure
	// that a client request has been allocated for a particular client's request number,
	// then it must validate the request, and finally pass the allocation information
	// along with a hash of the request back into the state machine via the Propose API.
	AllocatedRequests []RequestSlot

	// CorrectRequests is a list of requests and their identifying digests which are known
	// to be correct and should be stored if forwarded.  As a special case, if the request is
	// the null request (the digest is nil) then the consumer need not way for the request
	// to be forwarded and should immediately store a null request.
	CorrectRequests []*msgs.RequestAck

	// ForwardRequest is a list of requests which must be sent to another replica in the
	// network.  and their destinations.
	ForwardRequests []Forward
}

// clear nils out all of the fields.
func (ca *ClientActions) clear() {
	ca.AllocatedRequests = nil
	ca.CorrectRequests = nil
	ca.ForwardRequests = nil
}

func (ca *ClientActions) isEmpty() bool {
	return len(ca.AllocatedRequests) == 0 &&
		len(ca.CorrectRequests) == 0 &&
		len(ca.ForwardRequests) == 0
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (ca *ClientActions) concat(o *ClientActions) *ClientActions {
	ca.AllocatedRequests = append(ca.AllocatedRequests, o.AllocatedRequests...)
	ca.CorrectRequests = append(ca.CorrectRequests, o.CorrectRequests...)
	ca.ForwardRequests = append(ca.ForwardRequests, o.ForwardRequests...)
	return ca
}

type ClientActionResults struct {
	// PersistedRequests should be the set of new client requests that
	// have been persisted (because they are are known to be correct).
	PersistedRequests []*msgs.RequestAck
}

func (car *ClientActionResults) persisted(ack *msgs.RequestAck) *ClientActionResults {
	car.PersistedRequests = append(car.PersistedRequests, ack)
	return car
}

// Write either appends to the WAL or truncates it.  Exactly one operation will be non-nil.
type Write struct {
	Truncate *uint64
	Append   *WALEntry
}

type WALEntry struct {
	Index uint64
	Data  *msgs.Persistent
}

// Send is an action to send a message to a set of nodes
type Send struct {
	Targets []uint64
	Msg     *msgs.Msg
}

type RequestSlot struct {
	ClientID uint64
	ReqNo    uint64
}

// Forward is an action much like send, but requires the consumer to first
// fetch the desired request and form the message before sending.
type Forward struct {
	Targets    []uint64
	RequestAck *msgs.RequestAck
}

// HashRequest is a request from the state machine to the consumer to hash some data.
// The Data field is generally the only field the consumer should read.  One of the other fields
// e.g. Batch or Request, will be populated, while the remainder will be nil.  The consumer
// may wish to examine these fields for the purpose of debugging, metrics, etc. but it is not
// required.
type HashRequest struct {
	// Data is a series of byte slices which should be added to the hash
	Data [][]byte

	// Origin is the request that originated, encoded awkwardly as a HashResult
	// with an empty Digest field but a populated Type.
	Origin *state.HashOrigin
}

// Commit contains a either batch of requests which have achieved total order and are ready
// to be committed, or a request to perform a checkpoint.  Only one value will ever be set,
// never both.
type Commit struct {
	// Batch, if set, indicates the set of requests which are ready for the application
	// to commit.
	Batch *msgs.QEntry

	// Checkpoint indicates a request to the application to compute a checkpoint value
	// so that the state machine may eventually garbage collect the log once a sufficient
	// number of nodes have also produces this checkpoint.  It is the consumer's responsibility
	// to reply to the state machine with a CheckpointResult in response.
	Checkpoint *Checkpoint
}

// Checkpoint indicates a request for the consumer to produce and return a CheckpointResult.
// It contains the committed network state, both client state and network configuration as well as
// the epoch configuration under which it committed.  It is the user's responsibility to compute
// and return a checkpoint value for the given network state, and to include any reconfigurations
// which are pending as conveyed by the previous batches.  Note, it is important _not_ to include
// the epoch configuration in the checkpoint value, as different nodes may commit the same checkpoint
// under different epochs and all nodes must arrive at the same checkpoint value.  Also note that
// given this checkpoint value, a node must be able to perform state transfer, including reproducing
// the network state, and any pending reconfigurations, as well as any necessary application state.
// Typically, a data structure like a Merkle tree is useful to incrementally build such a state target.
// Once the checkpoint has been computed, it must be returned via a CheckpointResult to the state machine.
type Checkpoint struct {
	// SeqNo is the sequence this checkpoint corresponds to.
	SeqNo uint64

	// NetworkConfig contains the currently committed network configuration.  Notably
	// it does not contain any reconfigurations which may be pending as the previous batches
	// committed.  It is the caller's responsibility to include any pending reconfigurations
	// into the CheckpointResult.
	NetworkConfig *msgs.NetworkState_Config

	// ClientsState contains the set of clients, their current low watermarks, and what
	// request numbers within their watermarks have committed.
	ClientsState []*msgs.NetworkState_Client
}

// StateTarget is a correct value for some checkpoint generated by the network.
type StateTarget struct {
	// SeqNo is the sequence of the checkpoint corresponding to this state target.
	SeqNo uint64

	// Value is the value of the checkpoint corresponding to this state target.
	Value []byte
}

// ActionResults should be populated by the caller as a result of
// executing the actions, then returned to the state machine.
type ActionResults struct {
	Digests     []*HashResult
	Checkpoints []*CheckpointResult
}

// HashResult must be populated with the result of hashing the requested HashRequest.
type HashResult struct {
	// Digest is the resulting hash of the original request.
	Digest []byte

	// Request is the original HashRequest which resulted in the computed Digest.
	Request *HashRequest
}

// CheckpointResult gives the state machine a verifiable checkpoint for the network
// to return to, and allows it to prune previous entries from its state.
type CheckpointResult struct {
	// Checkpoint is the *Checkpoint request which generated this checkpoint.
	Checkpoint *Checkpoint

	// Value is a concise representation of the state of the application when
	// all entries less than or equal to (but not greater than) the sequence
	// have been applied.  Typically, this is a hash of the world state, usually
	// computed from a Merkle tree, hash chain, or other structure exihibiting
	// the properties of a strong hash function.
	Value []byte

	// Reconfigurations is an ordered list of reconfigurations which occurred in
	// the commits of this checkpoint.  The contents of this list only need to
	// be deterministically agreed upon by all correct nodes, there is no requirement
	// that particular messages were encoded over the wire, although it may be
	// convenient to serialize the reconfiguration protos into the application messages.
	// For instance, a series of reconfigurations which adds one node and removes another
	// may be collapsed into a single reconfiguration.  Note, it is critical with any
	// reconfiguration that the chosen configuration leaves the network in an operable state.
	// For instance, removing all clients would prevent any new messages from entering
	// the system, or, increasing 'f' to an unsatisfiable size would prevent quorums from forming.
	// In general, it is safest to modify one parameter, one value at a time (for instance, adding
	// a single node, reducing f by one, etc.) but sometimes more radical reconfigurations are
	// desirable, even if it forces a loss of quorum and requires new nodes to state transfer
	// before consenting.
	// Reconfiguration will be applied starting at the _next_ checkpoint.
	Reconfigurations []*msgs.Reconfiguration
}
