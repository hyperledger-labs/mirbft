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

	// Process should validate each batch, and return a digest and a validation status
	// for that batch.  Usually, if the batch originated at this node, validation may
	// be skipped.  For each item in the Process list the caller must Addresult with a
	// ProcessResult.
	Process []*Batch

	// QEntries should be persisted to persistent storage.  Multiple QEntries may be
	// persisted for the same SeqNo, but for different epochs and all must be retained.
	QEntries []*pb.QEntry

	// PEntries should be persisted to persistant storage.  Any PEntry already in storage
	// but with an older epoch may be discarded.
	PEntries []*pb.PEntry

	// Commit is a set of batches which have achieved final order and are ready to commit.
	// They will have previously committed via QEntries.
	Commit []*pb.QEntry

	// Checkpoint is a set of sequence numbers for which all previous sequence numbers in
	// all buckets have been sent to Commit.  It is the responsibility of the user to
	// ensure that all commits up to and including (but not past) this sequence number
	// have been applied.  For each Checkpoint in the list, it is the responsbility of
	// the caller to AddResult with a CheckpointResult.
	Checkpoint []uint64
}

// Clear nils out all of the fields.
func (a *Actions) Clear() {
	a.Broadcast = nil
	a.Unicast = nil
	a.Preprocess = nil
	a.Process = nil
	a.QEntries = nil
	a.PEntries = nil
	a.Commit = nil
	a.Checkpoint = nil
}

// IsEmpty returns whether every field is zero in length.
func (a *Actions) IsEmpty() bool {
	return len(a.Broadcast) == 0 &&
		len(a.Unicast) == 0 &&
		len(a.Preprocess) == 0 &&
		len(a.Process) == 0 &&
		len(a.Commit) == 0 &&
		len(a.QEntries) == 0 &&
		len(a.PEntries) == 0 &&
		len(a.Checkpoint) == 0
}

// Append takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) Append(o *Actions) {
	a.Broadcast = append(a.Broadcast, o.Broadcast...)
	a.Unicast = append(a.Unicast, o.Unicast...)
	a.Preprocess = append(a.Preprocess, o.Preprocess...)
	a.Process = append(a.Process, o.Process...)
	a.Commit = append(a.Commit, o.Commit...)
	a.QEntries = append(a.QEntries, o.QEntries...)
	a.PEntries = append(a.PEntries, o.PEntries...)
	a.Checkpoint = append(a.Checkpoint, o.Checkpoint...)
}

// Unicast is an action to send a message to a particular node.
type Unicast struct {
	Target uint64
	Msg    *pb.Msg
}

type Request struct {
	Source        uint64
	ClientRequest *pb.RequestData
}

// ActionResults should be populated by the caller as a result of
// executing the actions, then returned to the state machine.
type ActionResults struct {
	Processed    []*ProcessResult
	Preprocessed []*PreprocessResult
	Checkpoints  []*CheckpointResult
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

// Batch is a collection of proposals which has been allocated a sequence in a given epoch.
type Batch struct {
	Source   uint64
	SeqNo    uint64
	Epoch    uint64
	Requests []*PreprocessResult
}

// PreprocessResult gives the state machine a location which may be used
// to assign a proposal to a bucket for ordering.
type PreprocessResult struct {
	// Digest is the result of hashing this request.  The first 8 bytes
	// of this hash will be used to compute the bucket to assign the
	// request to.
	Digest []byte

	// Proposal is the proposal which was processed into this Preprocess result.
	RequestData *pb.RequestData
}

// ProcessResult gives the state machine a digest by which to refer to a particular entry.
// as well as an indication of whether the batch is valid.  If the batch is invalid, then
// depending on the configuration of the state machine (TODO), this node may still commit
// the entry, or may wait for state-transfer to kick off.
type ProcessResult struct {
	Batch   *Batch
	Digest  []byte
	Invalid bool
}
