/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
)

// Actions are the responsibility of the library user to fulfill.
// TODO add details about concurrency
type Actions struct {
	Broadcast  []*pb.Msg
	Unicast    []Unicast
	Preprocess []Proposal
	Digest     []*Entry
	Validate   []*Entry
	Commit     []*Entry
	Checkpoint []uint64
}

func (a *Actions) Clear() {
	a.Broadcast = nil
	a.Unicast = nil
	a.Preprocess = nil
	a.Digest = nil
	a.Validate = nil
	a.Commit = nil
	a.Checkpoint = nil
}

func (a *Actions) IsEmpty() bool {
	return len(a.Broadcast) == 0 &&
		len(a.Unicast) == 0 &&
		len(a.Preprocess) == 0 &&
		len(a.Digest) == 0 &&
		len(a.Validate) == 0 &&
		len(a.Commit) == 0 &&
		len(a.Checkpoint) == 0
}

func (a *Actions) Append(o *Actions) {
	a.Broadcast = append(a.Broadcast, o.Broadcast...)
	a.Unicast = append(a.Unicast, o.Unicast...)
	a.Preprocess = append(a.Preprocess, o.Preprocess...)
	a.Digest = append(a.Digest, o.Digest...)
	a.Validate = append(a.Validate, o.Validate...)
	a.Commit = append(a.Commit, o.Commit...)
	a.Checkpoint = append(a.Checkpoint, o.Checkpoint...)
}

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

type CheckpointResult struct {
	SeqNo       uint64
	Value       []byte
	Attestation []byte
}

type Proposal struct {
	Source uint64
	Data   []byte
}

type DigestResult struct {
	Entry  *Entry
	Digest []byte
}

type ValidateResult struct {
	Entry *Entry
	Valid bool
}

type PreprocessResult struct {
	Cup      uint64
	Proposal Proposal
}

type Entry struct {
	Epoch    uint64
	SeqNo    uint64
	BucketID uint64
	Batch    [][]byte
}
