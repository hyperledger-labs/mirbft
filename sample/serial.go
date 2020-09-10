/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sample

import (
	"context"
	"fmt"
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

type WAL interface {
	Append(entry *pb.Persistent) error
	Sync() error
}

type RequestStore interface {
	Store(requestAck *pb.RequestAck, data []byte) error
	Get(requestAck *pb.RequestAck) ([]byte, error)
}

type SerialProcessor struct {
	Link         Link
	Hasher       Hasher
	Log          Log
	WAL          WAL
	RequestStore RequestStore
	Node         *mirbft.Node
}

func (sp *SerialProcessor) Persist(actions *mirbft.Actions) {
	for _, r := range actions.StoreRequests {
		sp.RequestStore.Store(
			r.RequestAck,
			r.RequestData,
		)
	}

	for _, p := range actions.Persist {
		if err := sp.WAL.Append(p); err != nil {
			panic(fmt.Sprintf("could not persist entry: %s", err))
		}
	}

	if err := sp.WAL.Sync(); err != nil {
		panic(fmt.Sprintf("could not sync WAL: %s", err))
	}
}

func (sp *SerialProcessor) Transmit(actions *mirbft.Actions) {
	for _, send := range actions.Send {
		for _, replica := range send.Targets {
			if replica == sp.Node.Config.ID {
				sp.Node.Step(context.Background(), replica, send.Msg)
			} else {
				sp.Link.Send(replica, send.Msg)
			}
		}
	}

	for _, r := range actions.ForwardRequests {
		requestData, err := sp.RequestStore.Get(r.RequestAck)
		if err != nil {
			panic("io error? this should always return successfully")
		}
		fr := &pb.Msg{
			Type: &pb.Msg_ForwardRequest{
				&pb.ForwardRequest{
					RequestAck:  r.RequestAck,
					RequestData: requestData,
				},
			},
		}

		for _, replica := range r.Targets {
			if replica == sp.Node.Config.ID {
				sp.Node.Step(context.Background(), replica, fr)
			} else {
				sp.Link.Send(replica, fr)
			}
		}
	}
}

func (sp *SerialProcessor) Apply(actions *mirbft.Actions) *mirbft.ActionResults {
	actionResults := &mirbft.ActionResults{
		Digests: make([]*mirbft.HashResult, len(actions.Hash)),
	}

	for i, req := range actions.Hash {
		h := sp.Hasher()
		for _, data := range req.Data {
			h.Write(data)
		}

		actionResults.Digests[i] = &mirbft.HashResult{
			Request: req,
			Digest:  h.Sum(nil),
		}
	}

	for _, commit := range actions.Commits {
		sp.Log.Apply(commit.QEntry) // Apply the entry

		if commit.Checkpoint {
			value := sp.Log.Snap()
			actionResults.Checkpoints = append(actionResults.Checkpoints, &mirbft.CheckpointResult{
				Commit: commit,
				Value:  value,
			})
		}
	}

	return actionResults
}

func (sp *SerialProcessor) Process(actions *mirbft.Actions) *mirbft.ActionResults {
	sp.Persist(actions)
	sp.Transmit(actions)
	return sp.Apply(actions)
}
