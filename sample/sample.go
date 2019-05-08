/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sample

import (
	"context"
	"encoding/binary"

	"github.com/IBM/mirbft"
	"github.com/IBM/mirbft/consumer"
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

type SerialConsumer struct {
	Link      Link
	Validator Validator
	Hasher    Hasher
	Node      *mirbft.Node
	DoneC     <-chan struct{}
	CommitC   chan<- *consumer.Entry
}

func NewSerialConsumer(doneC <-chan struct{}, commitC chan<- *consumer.Entry, node *mirbft.Node, link Link, verifier Validator, hasher Hasher) *SerialConsumer {
	c := &SerialConsumer{
		Node:      node,
		Link:      link,
		Validator: verifier,
		Hasher:    hasher,
		DoneC:     doneC,
		CommitC:   commitC,
	}
	go c.process()
	return c
}

func (c *SerialConsumer) process() {
	for {
		select {
		case actions := <-c.Node.Ready():
			actionResults := &consumer.ActionResults{
				Preprocesses: make([]consumer.PreprocessResult, len(actions.Preprocess)),
				Digests:      make([]consumer.DigestResult, len(actions.Digest)),
				Validations:  make([]consumer.ValidateResult, len(actions.Validate)),
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

				actionResults.Preprocesses[i] = consumer.PreprocessResult{
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

				actionResults.Digests[i] = consumer.DigestResult{
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

				actionResults.Validations[i] = consumer.ValidateResult{
					Entry: entry,
					Valid: valid,
				}
			}

			for _, entry := range actions.Commit {
				select {
				case c.CommitC <- entry:
				case <-c.DoneC:
					return
				}
			}

			if err := c.Node.AddResults(*actionResults); err != nil {
				return
			}
		case <-c.DoneC:
			return
		}
	}
}
