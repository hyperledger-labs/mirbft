/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

type BucketID uint64
type SeqNo uint64
type NodeID uint64

type Replica struct {
	ID uint64
}

type Node struct {
	Config   *Config
	s        *serializer
	Replicas []Replica
}

func StartNewNode(config *Config, doneC <-chan struct{}, replicas []Replica) (*Node, error) {
	buckets := map[BucketID]NodeID{}
	nodes := []NodeID{}
	for _, replica := range replicas {
		buckets[BucketID(replica.ID)] = NodeID(replica.ID)
		nodes = append(nodes, NodeID(replica.ID))
	}
	if _, ok := buckets[BucketID(config.ID)]; !ok {
		return nil, errors.Errorf("configured replica ID %d is not in the replica set", config.ID)
	}
	f := (len(replicas) - 1) / 3
	return &Node{
		Config:   config,
		Replicas: replicas,
		s: newSerializer(&stateMachine{
			myConfig: config,
			currentEpoch: newEpoch(&epochConfig{
				myConfig: config,
				oddities: &oddities{
					nodes: map[NodeID]*oddity{},
				},
				number:             0,
				checkpointInterval: 5,
				highWatermark:      50,
				lowWatermark:       0,
				f:                  f,
				nodes:              nodes,
				buckets:            buckets,
			}),
		}, doneC),
	}, nil
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	select {
	case n.s.propC <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.doneC:
		return ErrStopped
	}
}

func (n *Node) Step(ctx context.Context, source uint64, msg *pb.Msg) error {
	select {
	case n.s.stepC <- step{Source: source, Msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.doneC:
		return ErrStopped
	}
}

func (n *Node) Status(ctx context.Context) (*Status, error) {
	statusC := make(chan *Status, 1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.s.statusC <- statusC:
		select {
		case status := <-statusC:
			return status, nil
		case <-n.s.doneC:
			return nil, ErrStopped
		}
	case <-n.s.doneC:
		return nil, ErrStopped
	}
}

func (n *Node) Ready() <-chan Actions {
	return n.s.actionsC
}

func (n *Node) Tick() {
	n.s.tickC <- struct{}{}
}

func (n *Node) AddResults(results ActionResults) error {
	select {
	case n.s.resultsC <- results:
		return nil
	case <-n.s.doneC:
		return ErrStopped
	}
}
