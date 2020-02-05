/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sample

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type ValidatorFunc func([]byte) error

func (vf ValidatorFunc) Validate(data []byte) error {
	return vf(data)
}

type HasherFunc func([]byte) []byte

func (hf HasherFunc) Hash(data []byte) []byte {
	return hf(data)
}

type Validator interface {
	Validate(data []byte) error
}

type Link interface {
	Send(dest uint64, msg *pb.Msg)
}

type Hasher interface {
	Hash([]byte) []byte
}

type Log interface {
	Apply(*mirbft.Entry)
	Snap() (id []byte)
}

type SerialCommitter struct {
	Log                Log
	LastCommittedSeqNo uint64
	OutstandingSeqNos  map[uint64]*mirbft.Entry
}

func (sc *SerialCommitter) Commit(commits []*mirbft.Entry, checkpoints []uint64) []*mirbft.CheckpointResult {
	for _, commit := range commits {
		// Note, this pattern is easy to understand, but memory inefficient.
		// A ring buffer of size equal to the log size would produce far less
		// garbage.
		sc.OutstandingSeqNos[commit.SeqNo] = commit
	}

	results := []*mirbft.CheckpointResult{}

	// If a checkpoint is present, then all commits prior to that seqno must be present
	for _, checkpoint := range checkpoints {
		for {
			currentSeqNo := sc.LastCommittedSeqNo + 1
			entry, ok := sc.OutstandingSeqNos[currentSeqNo]
			if !ok {
				panic(fmt.Sprintf("all previous commits should be available if checkpoint requested, seqNo=%d, checkpoint=%d", currentSeqNo, checkpoint))
			}
			sc.Log.Apply(entry) // Apply the entry
			sc.LastCommittedSeqNo = currentSeqNo
			delete(sc.OutstandingSeqNos, currentSeqNo)

			if checkpoint == currentSeqNo {
				break
			}
		}

		value := sc.Log.Snap()
		results = append(results, &mirbft.CheckpointResult{
			SeqNo: sc.LastCommittedSeqNo,
			Value: value,
		})
	}

	return results
}

type SerialProcessor struct {
	Link      Link
	Validator Validator
	Hasher    Hasher
	Committer *SerialCommitter
	Node      *mirbft.Node
	PauseC    chan struct{}
	DoneC     <-chan struct{}
}

func (c *SerialProcessor) Process(actions *mirbft.Actions) *mirbft.ActionResults {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Printing state machine status")
			ctx, cancel := context.WithTimeout(context.TODO(), 50*time.Millisecond)
			defer cancel()
			status, err := c.Node.Status(ctx)
			if err != nil {
				fmt.Printf("Could not get status: %s", err)
			} else {
				fmt.Printf("\n%s\n", status.Pretty())
			}
			panic(r)
		}
	}()

	actionResults := &mirbft.ActionResults{
		Preprocesses: make([]mirbft.PreprocessResult, len(actions.Preprocess)),
		Digests:      make([]mirbft.DigestResult, len(actions.Digest)),
		Validations:  make([]mirbft.ValidateResult, len(actions.Validate)),
	}

	for _, broadcast := range actions.Broadcast {
		for _, replica := range c.Node.Replicas {
			if replica.ID == c.Node.Config.ID {
				c.Node.Step(context.TODO(), replica.ID, broadcast)
			} else {
				c.Link.Send(replica.ID, broadcast)
			}
		}
	}

	for _, unicast := range actions.Unicast {
		c.Link.Send(unicast.Target, unicast.Msg)
	}

	for i, proposal := range actions.Preprocess {
		hash := c.Hasher.Hash(proposal.Data)

		actionResults.Preprocesses[i] = mirbft.PreprocessResult{
			Proposal: proposal,
			Cup:      binary.LittleEndian.Uint64(hash[0:8]),
		}
	}

	for i, entry := range actions.Digest {
		hashes := []byte{}
		for _, data := range entry.Batch {
			// TODO this could be much more efficient
			// The assumption is that the hasher has already likely
			// computed the hashes of the data, so, if using a cached version
			// concatenating the hashes would be cheap
			hashes = append(hashes, c.Hasher.Hash(data)...)
		}

		actionResults.Digests[i] = mirbft.DigestResult{
			Entry:  entry,
			Digest: c.Hasher.Hash(hashes),
		}
	}

	for i, entry := range actions.Validate {
		valid := true
		for _, data := range entry.Batch {
			if err := c.Validator.Validate(data); err != nil {
				valid = false
				break
			}
		}

		actionResults.Validations[i] = mirbft.ValidateResult{
			Entry: entry,
			Valid: valid,
		}
	}

	actionResults.Checkpoints = c.Committer.Commit(actions.Commit, actions.Checkpoint)

	return actionResults
}

type FakeLink struct {
	Buffers map[uint64]chan *pb.Msg
}

func NewFakeLink(source uint64, nodes []*mirbft.Node, doneC <-chan struct{}) *FakeLink {
	buffers := map[uint64]chan *pb.Msg{}
	for _, node := range nodes {
		if node.Config.ID == source {
			continue
		}
		buffer := make(chan *pb.Msg, 1000)
		buffers[node.Config.ID] = buffer
		go func(node *mirbft.Node) {
			for {
				select {
				case msg := <-buffer:
					node.Step(context.TODO(), source, msg)
				case <-doneC:
					return
				}
			}
		}(node)
	}
	return &FakeLink{
		Buffers: buffers,
	}
}

func (fl *FakeLink) Send(dest uint64, msg *pb.Msg) {
	fl.Buffers[dest] <- msg
}
