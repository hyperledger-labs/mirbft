/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package mirbft is a consensus library, implementing the Mir BFT consensus protocol.
//
// This library can be used by applications which desire distributed, byzantine fault
// tolerant consensus on message order.  Unlike many traditional consensus algorithms,
// Mir BFT is a multi-leader protocol, allowing throughput to scale with the number of
// nodes (even over WANs), where the performance of traditional consenus algorithms
// begin to degrade.
package mirbft

import (
	"context"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

// BucketID is the identifier for a bucket.  It is a simple alias to a uint64, but
// is used to help disambiguate function signatures which accept multiple uint64
// values with different meanings.
type BucketID uint64

// EpochNo represents an epoch number.  It is a simple alias to a uint64, but
// is used to help disambiguate function signatures which accept multiple uint64
// values with different meanings.
type EpochNo uint64

// NodeID represents the identifier assigned to a node.  It is a simple alias to a uint64, but
// is used to help disambiguate function signatures which accept multiple uint64
// values with different meanings.
type NodeID uint64

// Replica represents a node in the network.  Although network state is maintained internally
// after bootstrapping, the replica info must be supplied at boostrap.
type Replica struct {
	// ID is the NodeID for the replica.
	ID uint64
}

// Node is the local instance of the MirBFT state machine through which the calling application
// proposes new messages, receives delegated actions, and returns action results.
// The methods exposed on Node are all thread safe, though typically, a single loop handles
// reading Actions, writing results, and writing ticks, while other go routines Propose and Step.
type Node struct {
	Config   *Config
	s        *serializer
	Replicas []Replica
}

// StartNewNode creates a node to join a fresh network.  Eventually, this method will either
// be deprecated, or augmented with a RestartNode or similar.  For now, this method
// hard codes many of the parameters, but more will be exposed in the future.
func StartNewNode(config *Config, doneC <-chan struct{}, replicas []Replica) (*Node, error) {
	nodes := []uint64{}
	for _, replica := range replicas {
		nodes = append(nodes, replica.ID)
	}

	return &Node{
		Config:   config,
		Replicas: replicas,
		s: newSerializer(
			newStateMachine(
				&pb.NetworkConfig{
					Nodes:              nodes,
					F:                  int32((len(replicas) - 1) / 3),
					CheckpointInterval: int32(5 * len(nodes)),
					MaxEpochLength:     uint64(len(nodes)*5*10) * 100000,
					NumberOfBuckets:    int32(len(nodes)),
				},
				config,
			),
			doneC,
		),
	}, nil
}

// Propose injects a new message into the system.  The message may be
// forwarded to another node after pre-processing for ordering.  This method
// only returns an error if the context ends, or the node is stopped.
// In the case that the node is stopped it returns ErrStopped.
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

// Step takes authenticated messages from the other nodes in the network.  It
// is the responsibility of the caller to ensure that the message originated from
// the designed source.  This method returns an error if the context ends, the node
// stopped, or the message is not well formed (unknown proto fields, etc.).  In the
// case that the node is stopped, it returns ErrStopped.
func (n *Node) Step(ctx context.Context, source uint64, msg *pb.Msg) error {
	err := preProcess(msg)
	if err != nil {
		return err
	}

	select {
	case n.s.stepC <- step{Source: source, Msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.s.doneC:
		return ErrStopped
	}
}

// Status returns a static snapshot in time of the internal state of the state machine.
// This method necessarily exposes some of the internal architecture of the system, and
// especially while the library is in development, the data structures may change substantially.
// This method only returns an error if the context ends, or the node is stopped.
// In the case that the node is stopped, it returns ErrStopped.
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

// Ready returns a channel which will deliver Actions for the user to perform.
// See the documentation for Actions regarding the detailed responsibilities
// of the caller.
func (n *Node) Ready() <-chan Actions {
	return n.s.actionsC
}

// Tick injects a tick into the state machine.  Ticks inform the state machine that
// time has elapsed, and cause it to perform operations like emit no-op heartbeat
// batches, or transition into an epoch change.  Typically, a time.Ticker is used
// and selected on in the same select statement as Ready().
func (n *Node) Tick() {
	n.s.tickC <- struct{}{}
}

// AddResults is a callback from the consumer to the state machine, informing the
// state machine that Actions have been carried out, and the result of those
// Actions is applicable.  In the case that the node is stopped, it returns
// ErrStopped, otherwise nil is returned.
func (n *Node) AddResults(results ActionResults) error {
	select {
	case n.s.resultsC <- results:
		return nil
	case <-n.s.doneC:
		return ErrStopped
	}
}
