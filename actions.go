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
}

func (a *Actions) send(targets []uint64, msg *pb.Msg) *Actions {
	a.Send = append(a.Send, Send{
		Targets: targets,
		Msg:     msg,
	})

	return a
}

// clear nils out all of the fields.
func (a *Actions) clear() {
	a.Send = nil
	a.Hash = nil
	a.Persist = nil
	a.Commits = nil
}

func (a *Actions) isEmpty() bool {
	return len(a.Send) == 0 &&
		len(a.Hash) == 0 &&
		len(a.Persist) == 0 &&
		len(a.Commits) == 0
}

// concat takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) concat(o *Actions) *Actions {
	a.Send = append(a.Send, o.Send...)
	a.Commits = append(a.Commits, o.Commits...)
	a.Hash = append(a.Hash, o.Hash...)
	a.Persist = append(a.Persist, o.Persist...)
	return a
}

// Send is an action to send a message to a set of nodes
type Send struct {
	Targets []uint64
	Msg     *pb.Msg
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

// Commit contains a batch of requests which have achieved total order and are ready
// to be committed.  Commits are delivered in order.  If this commit corresponds to a
// checkpoint (as indicated by the Checkpoint field), it is the consumer's responsibility
// to compute a CheckpointResult and return it in the ActionsResults.
type Commit struct {
	QEntry       *pb.QEntry
	Checkpoint   bool
	NetworkState *pb.NetworkState
	EpochConfig  *pb.EpochConfig
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
	// Commit is the *Commit which generated this checkpoint
	Commit *Commit

	// Value is a concise representation of the state of the application when
	// all entries less than or equal to (but not greater than) the sequence
	// have been applied.  Typically, this is a hash of the world state, usually
	// computed from a Merkle tree, hash chain, or other structure exihibiting
	// the properties of a strong hash function.
	Value []byte
}
