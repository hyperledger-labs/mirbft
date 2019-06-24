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

type StatusEncoding int

const (
	ConsoleEncoding StatusEncoding = iota
	JSONEncoding
)

type Replica struct {
	ID uint64
}

type Node struct {
	Config   *Config
	s        *Serializer
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
		s: NewSerializer(&StateMachine{
			Config: config,
			CurrentEpoch: NewEpoch(&EpochConfig{
				MyConfig: config,
				Oddities: &Oddities{
					Nodes: map[NodeID]*Oddity{},
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
	case n.s.StepC <- Step{Source: source, Msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.DoneC:
		return ErrStopped
	}
}

func (n *Node) Status(ctx context.Context, encoding StatusEncoding) (string, error) {
	statusC := make(chan string, 1)

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case n.s.StatusC <- StatusReq{
		JSON:   encoding == JSONEncoding,
		ReplyC: statusC,
	}:
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

func (n *Node) Ready() <-chan Actions {
	return n.s.ActionsC
}

func (n *Node) Tick() {
	n.s.TickC <- struct{}{}
}

func (n *Node) AddResults(results ActionResults) error {
	select {
	case n.s.ResultsC <- results:
		return nil
	case <-n.s.DoneC:
		return ErrStopped
	}
}
