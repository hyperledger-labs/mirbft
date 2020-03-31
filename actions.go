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
	// Broadcast messages should be sent to every node in the cluster (including yourself).
	Broadcast []*pb.Msg

	// Unicast messages should be sent only to the specified target.
	Unicast []Unicast

	// Preprocess is a set of messages (and their origins) for pre-processing.
	// For each item in the Preprocess list, the caller must AddResult with a PreprocessResult.
	// The source of the proposal is included in case the caller wishes to do more
	// validation on proposals originating from other nodes than proposals originating from
	// itself.
	Preprocess []*Request

	// Hash is a set of requests to be hashed.  Hash can (and usually should) be done
	// in parallel with persisting to disk and performing network sends.
	Hash []*HashRequest

	// QEntries should be persisted to persistent storage.  Multiple QEntries may be
	// persisted for the same SeqNo, but for different epochs and all must be retained.
	QEntries []*pb.QEntry

	// PEntries should be persisted to persistant storage.  Any PEntry already in storage
	// but with an older epoch may be discarded.
	PEntries []*pb.PEntry

	// Commits is a set of batches which have achieved final order and are ready to commit.
	// They will have previously persisted via QEntries.  When the user processes a commit,
	// if that commit contains a checkpoint, the user must return a checkpoint result for
	// this commit.  Checkpoints must be persisted before further commits are reported as applied.
	Commits []*Commit
}

// Clear nils out all of the fields.
func (a *Actions) Clear() {
	a.Broadcast = nil
	a.Unicast = nil
	a.Hash = nil
	a.QEntries = nil
	a.PEntries = nil
	a.Commits = nil
}

// IsEmpty returns whether every field is zero in length.
func (a *Actions) IsEmpty() bool {
	return len(a.Broadcast) == 0 &&
		len(a.Unicast) == 0 &&
		len(a.Commits) == 0 &&
		len(a.Hash) == 0 &&
		len(a.QEntries) == 0 &&
		len(a.PEntries) == 0
}

// Append takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) Append(o *Actions) {
	a.Broadcast = append(a.Broadcast, o.Broadcast...)
	a.Unicast = append(a.Unicast, o.Unicast...)
	a.Commits = append(a.Commits, o.Commits...)
	a.Hash = append(a.Hash, o.Hash...)
	a.QEntries = append(a.QEntries, o.QEntries...)
	a.PEntries = append(a.PEntries, o.PEntries...)
}

// HashRequest is a request from the state machine to the consumer to hash some data.
// The Data field is generally the only field the consumer should read.  One of the other fields
// e.g. Batch or Request, will be populated, while the remainder will be nil.  The consumer
// may wish to examine these fields for the purpose of debugging, metrics, etc. but it is not
// required.
type HashRequest struct {
	// Data is a series of byte slices which should be added to the hash
	Data [][]byte

	// Batch is internal state used to associate the result of this hash request
	// with the batch it originated at.  Consumers should usually not need to
	// reference this field.
	Batch *Batch

	// Request is the proposal which is being hashed.
	Request *Request
}

// Batch is a collection of proposals which has been allocated a sequence in a given epoch.
type Batch struct {
	Source   uint64
	SeqNo    uint64
	Epoch    uint64
	Requests []*pb.Request
}

type Request struct {
	Source      uint64
	RequestData *pb.RequestData
}

type HashResult struct {
	Digest  []byte
	Request *HashRequest
}

// Unicast is an action to send a message to a particular node.
type Unicast struct {
	Target uint64
	Msg    *pb.Msg
}

type Commit struct {
	QEntry     *pb.QEntry
	Checkpoint bool
}

// ActionResults should be populated by the caller as a result of
// executing the actions, then returned to the state machine.
type ActionResults struct {
	Digests     []*HashResult
	Checkpoints []*CheckpointResult
}

// CheckpointResult gives the state machine a verifiable checkpoint for the network
// to return to, and allows it to prune previous entries from its state.
type CheckpointResult struct {
	// SeqNo is the sequence number of this checkpoint.
	SeqNo uint64

	// Value is a concise representation of the state of the application when
	// all entries less than or equal to (but not greater than) the sequence
	// have been applied.  Typically, this is a hash of the world state, usually
	// computed from a Merkle tree, hash chain, or other structure exihibiting
	// the properties of a strong hash function.
	Value []byte
}
