/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sample

import (
	"context"
	"hash"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type Hasher func() hash.Hash

type Link interface {
	Send(dest uint64, msg *pb.Msg)
}

type Log interface {
	Apply(*pb.QEntry)
	Snap() (id []byte)
}

type SerialCommitter struct {
	Log                    Log
	LastCommittedSeqNo     uint64
	OutstandingSeqNos      map[uint64]*mirbft.Commit
	OutstandingCheckpoints map[uint64]struct{}
}

func (sc *SerialCommitter) Commit(commits []*mirbft.Commit) []*mirbft.CheckpointResult {
	for _, commit := range commits {
		// Note, this pattern is easy to understand, but memory inefficient.
		// A ring buffer of size equal to the log size would produce far less
		// garbage.
		sc.OutstandingSeqNos[commit.QEntry.SeqNo] = commit
	}

	var results []*mirbft.CheckpointResult

	for currentSeqNo := sc.LastCommittedSeqNo + 1; len(sc.OutstandingSeqNos) > 0; currentSeqNo++ {
		entry, ok := sc.OutstandingSeqNos[currentSeqNo]
		if !ok {
			break
		}
		sc.Log.Apply(entry.QEntry) // Apply the entry
		sc.LastCommittedSeqNo = currentSeqNo
		delete(sc.OutstandingSeqNos, currentSeqNo)

		if entry.Checkpoint {
			value := sc.Log.Snap()
			results = append(results, &mirbft.CheckpointResult{
				Commit: entry,
				Value:  value,
			})
		}
	}

	return results
}

type SerialProcessor struct {
	Link      Link
	Hasher    Hasher
	Committer *SerialCommitter
	Node      *mirbft.Node
}

func (c *SerialProcessor) Persist(actions *mirbft.Actions) {
	// TODO we need to persist the PSet, QSet, and some others here
}

func (c *SerialProcessor) Transmit(actions *mirbft.Actions) {
	for _, broadcast := range actions.Broadcast {
		for _, replica := range actions.Replicas {
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
}

func (c *SerialProcessor) Apply(actions *mirbft.Actions) *mirbft.ActionResults {
	actionResults := &mirbft.ActionResults{
		Digests: make([]*mirbft.HashResult, len(actions.Hash)),
	}

	for i, req := range actions.Hash {
		h := c.Hasher()
		for _, data := range req.Data {
			h.Write(data)
		}

		actionResults.Digests[i] = &mirbft.HashResult{
			Request: req,
			Digest:  h.Sum(nil),
		}
	}

	actionResults.Checkpoints = c.Committer.Commit(actions.Commits)

	return actionResults
}

func (c *SerialProcessor) Process(actions *mirbft.Actions) *mirbft.ActionResults {
	c.Persist(actions)
	c.Transmit(actions)
	return c.Apply(actions)
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
