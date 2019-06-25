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
	Preprocess []Proposal

	// Digest is a set of batches which have been proposed by a node in the network.
	// For each item in the Digest list, the caller must AddResult with a DigestResult.
	// The resulting digest is used as an alias for the underlying data, and it should
	// exhibit the properties of a strong hash function.
	Digest []*Entry

	// Validate is a set of batches which have been proposed by a node in the network.
	// For each item in the Validate list, the caller must AddResult with a ValidateResult.
	// The validation should check that every message in the batch is valid according to
	// the application logic.  Note, it is expected that all batches are valid, except
	// under byzantine behavior from a node.  An invalid batch will result in an epoch
	// change and or state transfer.
	Validate []*Entry

	// Commit is a set of batches which have achieved final order and are ready to commit.
	Commit []*Entry

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
	a.Digest = nil
	a.Validate = nil
	a.Commit = nil
	a.Checkpoint = nil
}

// IsEmpty returns whether every field is zero in length.
func (a *Actions) IsEmpty() bool {
	return len(a.Broadcast) == 0 &&
		len(a.Unicast) == 0 &&
		len(a.Preprocess) == 0 &&
		len(a.Digest) == 0 &&
		len(a.Validate) == 0 &&
		len(a.Commit) == 0 &&
		len(a.Checkpoint) == 0
}

// Append takes a set of actions and for each field, appends it to
// the corresponding field of itself.
func (a *Actions) Append(o *Actions) {
	a.Broadcast = append(a.Broadcast, o.Broadcast...)
	a.Unicast = append(a.Unicast, o.Unicast...)
	a.Preprocess = append(a.Preprocess, o.Preprocess...)
	a.Digest = append(a.Digest, o.Digest...)
	a.Validate = append(a.Validate, o.Validate...)
	a.Commit = append(a.Commit, o.Commit...)
	a.Checkpoint = append(a.Checkpoint, o.Checkpoint...)
}

// Unicast is an action to send a message to a particular node.
type Unicast struct {
	Target uint64
	Msg    *pb.Msg
}

// ActionResults should be populated by the caller as a result of
// executing the actions, then returned to the state machine.
type ActionResults struct {
	Digests      []DigestResult
	Validations  []ValidateResult
	Preprocesses []PreprocessResult
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

	// Attestation is non-repudiable evidence that this node agrees with the checkpoint
	// Value for this sequence number.  Typically, this is a signature from a private key
	// of a known public/private key pair, but other schemes are possible.
	Attestation []byte
}

// Proposal is data which is proposed to be included in a batch and appended to the log.
type Proposal struct {
	// Source is the node which originated the proposal.
	Source uint64

	// Data is the message of the proposal
	Data []byte
}

// DigestResult gives the state machine a digest by which to refer to a particular entry.
// The digest will be sent in place of the entry's batch, during the Prepare and Commit
// phases and should generally be computed using a strong hashing function.
type DigestResult struct {
	Entry  *Entry
	Digest []byte
}

// ValidateResult gives the state machine information about whether a
// particular entry should be considered valid.  Note, that indicating an entry
// is not valid implies that the leader who proposed it is behaving in a
// byzantine way.
type ValidateResult struct {
	Entry *Entry
	Valid bool
}

// PreprocessResult gives the state machine a location which may be used
// to assign a proposal to a bucket for ordering.
type PreprocessResult struct {
	// Cup is a 'smaller bucket', and should ideally be uniformly
	// distributed across the uint64 space.  The Cup is used to assign
	// proposals to a particular bucket (which will service all requests
	// assigned to this Cup as well as many others).
	Cup uint64

	// Proposal is the proposal which was processed into this Preprocess result.
	Proposal Proposal
}

// Entry represents a log entry which may be
type Entry struct {
	Epoch    uint64
	SeqNo    uint64
	BucketID uint64
	Batch    [][]byte
}
