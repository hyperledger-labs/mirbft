/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

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

	// Persist contains data that should be persisted to persistent storage. It could
	// be of following types:
	// QEntry: Multiple QEntries may be persisted for the same SeqNo, but for different
	//         epochs and all must be retained.
	// PEntry: Any PEntry already in storage but with an older epoch may be discarded.
	Persist []*pb.Persistent

	// Commits is a set of batches which have achieved final order and are ready to commit.
	// They will have previously persisted via QEntries.  When the user processes a commit,
	// if that commit contains a checkpoint, the user must return a checkpoint result for
	// this commit.  Checkpoints must be persisted before further commits are reported as applied.
	Commits []*Commit

	// StoreRequests is a list of requests and their identifying digests which must be
	// stored prior to performing the network sends.  This may be performed in parallel
	// with the persisted log entries.
	StoreRequests []*pb.ForwardRequest

	// ForwardRequest is a list of requests which must be sent to another replica in the
	// network.  and their destinations.
	ForwardRequests []Forward
}

func (a *Actions) send(targets []uint64, msg *pb.Msg) *Actions {
	a.Send = append(a.Send, Send{
		Targets: targets,
		Msg:     msg,
	})

	return a
}

func (a *Actions) storeRequest(request *pb.ForwardRequest) *Actions {
	a.StoreRequests = append(a.StoreRequests, request)
	return a
}

func (a *Actions) forwardRequest(targets []uint64, requestAck *pb.RequestAck) *Actions {
	a.ForwardRequests = append(a.ForwardRequests, Forward{
		Targets:    targets,
		RequestAck: requestAck,
	})
	return a
}

func (a *Actions) persist(p *pb.Persistent) *Actions {
	a.Persist = append(a.Persist, p)
	return a
}

func (a *Actions) commit(commit *Commit) {
	a.Commits = append(a.Commits, commit)
}

// clear nils out all of the fields.
func (a *Actions) clear() {
	a.Send = nil
	a.Hash = nil
	a.Persist = nil
	a.Commits = nil
	a.StoreRequests = nil
	a.ForwardRequests = nil
}

func (a *Actions) isEmpty() bool {
	return len(a.Send) == 0 &&
		len(a.Hash) == 0 &&
		len(a.Persist) == 0 &&
		len(a.StoreRequests) == 0 &&
		len(a.ForwardRequests) == 0 &&
		len(a.Commits) == 0
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) concat(o *Actions) *Actions {
	a.Send = append(a.Send, o.Send...)
	a.Commits = append(a.Commits, o.Commits...)
	a.Hash = append(a.Hash, o.Hash...)
	a.Persist = append(a.Persist, o.Persist...)
	a.StoreRequests = append(a.StoreRequests, o.StoreRequests...)
	a.ForwardRequests = append(a.ForwardRequests, o.ForwardRequests...)
	return a
}

// Send is an action to send a message to a set of nodes
type Send struct {
	Targets []uint64
	Msg     *pb.Msg
}

// Forward is an action much like send, but requires the consumer to first
// fetch the desired request and form the message before sending.
type Forward struct {
	Targets    []uint64
	RequestAck *pb.RequestAck
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
	Origin *pb.HashResult
}

// Commit contains a either batch of requests which have achieved total order and are ready
// to be committed, or a request to perform a checkpoint.  Only one value will ever be set,
// never both.
type Commit struct {
	// Batch, if set, indicates the set of requests which are ready for the application
	// to commit.
	Batch *pb.QEntry

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
	NetworkConfig *pb.NetworkState_Config

	// ClientsState contains the set of clients, their current low watermarks, and what
	// request numbers within their watermarks have committed.
	ClientsState []*pb.NetworkState_Client

	// EpochConfig is the epoch configuration under which the most recent sequence
	// committed.  Note, that this state may vary from node to node and therefore must _not_
	// be included in the checkpoint value computation.  It is supplied because checkpoints
	// are used as garbage collection points for the log, and we must not lose the epoch
	// configuration when the log is pruned.  Most consumers may simply ignore this field.
	EpochConfig *pb.EpochConfig
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
	Reconfigurations []*pb.Reconfiguration
}
