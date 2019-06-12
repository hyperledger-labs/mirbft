/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"fmt"

	"github.com/IBM/mirbft/consumer"
	"github.com/IBM/mirbft/internal"
	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

type Replica struct {
	ID uint64
}

type Node struct {
	Config   *consumer.Config
	s        *internal.Serializer
	Replicas []Replica
}

func StartNewNode(config *consumer.Config, doneC <-chan struct{}, replicas []Replica) (*Node, error) {
	buckets := map[internal.BucketID]internal.NodeID{}
	nodes := []internal.NodeID{}
	for _, replica := range replicas {
		buckets[internal.BucketID(replica.ID)] = internal.NodeID(replica.ID)
		nodes = append(nodes, internal.NodeID(replica.ID))
	}
	if _, ok := buckets[internal.BucketID(config.ID)]; !ok {
		return nil, errors.Errorf("configured replica ID %d is not in the replica set", config.ID)
	}
	f := (len(replicas) - 1) / 3
	return &Node{
		Config:   config,
		Replicas: replicas,
		s: internal.NewSerializer(&internal.StateMachine{
			Config: config,
			CurrentEpoch: internal.NewEpoch(&internal.EpochConfig{
				MyConfig: config,
				Oddities: &internal.Oddities{
					Nodes: map[internal.NodeID]*internal.Oddity{},
				},
				Number:             0,
				CheckpointInterval: 5,
				HighWatermark:      50,
				LowWatermark:       0,
				F:                  f,
				Nodes:              nodes,
				Buckets:            buckets,
			}),
		}, doneC),
	}, nil
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	select {
	case n.s.PropC <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.DoneC:
		return ErrStopped
	}
}

func (n *Node) Step(ctx context.Context, source uint64, msg *pb.Msg) error {
	select {
	case n.s.StepC <- internal.Step{Source: source, Msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.DoneC:
		return ErrStopped
	}
}

func (n *Node) Status(ctx context.Context) (string, error) {
	statusC := make(chan string, 1)
	close(statusC)

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case n.s.StatusC <- statusC:
		select {
		case status := <-statusC:
			return status, nil
		case <-n.s.DoneC:
			return "", ErrStopped
		}
	case <-n.s.DoneC:
		return "", ErrStopped
	}
}

func (n *Node) Ready() <-chan consumer.Actions {
	return n.s.ActionsC
}

func (n *Node) AddResults(results consumer.ActionResults) error {
	select {
	case n.s.ResultsC <- results:
		return nil
	case <-n.s.DoneC:
		return ErrStopped
	}
}
