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
	"github.com/pkg/errors"
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

type ClientProposer struct {
	clientID     []byte
	clientWaiter *clientWaiter
	s            *serializer
}

func (cp *ClientProposer) Propose(ctx context.Context, blocking bool, requestData *pb.RequestData) error {
	for {
		if requestData.ReqNo < cp.clientWaiter.lowWatermark {
			return errors.Errorf("request %d below watermarks, lowWatermark=%d", requestData.ReqNo, cp.clientWaiter.lowWatermark)
		}

		if requestData.ReqNo <= cp.clientWaiter.highWatermark {
			break
		}

		if blocking {
			select {
			case <-cp.clientWaiter.expired:
			case <-ctx.Done():
				return ctx.Err()
			case <-cp.s.errC:
				return cp.s.getExitErr()
			}
		} else {
			select {
			case <-cp.clientWaiter.expired:
			case <-ctx.Done():
				return ctx.Err()
			case <-cp.s.errC:
				return cp.s.getExitErr()
			default:
				return errors.Errorf("request above watermarks, and not blocking for movement")
			}
		}

		replyC := make(chan *clientWaiter, 1)
		select {
		case cp.s.clientC <- &clientReq{
			clientID: requestData.ClientId,
			replyC:   replyC,
		}:
		case <-ctx.Done():
			return ctx.Err()
		case <-cp.s.errC:
			return cp.s.getExitErr()
		}

		select {
		case cp.clientWaiter = <-replyC:
		case <-ctx.Done():
			return ctx.Err()
		case <-cp.s.errC:
			return cp.s.getExitErr()
		}
	}

	select {
	case cp.s.propC <- requestData:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-cp.s.errC:
		return cp.s.getExitErr()
	}
}

func StandardInitialNetworkConfig(nodeCount int) *pb.NetworkConfig {
	nodes := []uint64{}
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, uint64(i))
	}

	numberOfBuckets := int32(nodeCount)
	checkpointInterval := numberOfBuckets * 5
	maxEpochLength := checkpointInterval * 10

	return &pb.NetworkConfig{
		Nodes:              nodes,
		F:                  int32((nodeCount - 1) / 3),
		NumberOfBuckets:    numberOfBuckets,
		CheckpointInterval: checkpointInterval,
		MaxEpochLength:     uint64(maxEpochLength),
	}
}

// StartNewNode creates a node to join a fresh network.  Eventually, this method will either
// be deprecated, or augmented with a RestartNode or similar.  For now, this method
// hard codes many of the parameters, but more will be exposed in the future.
func StartNewNode(config *Config, doneC <-chan struct{}, initialNetworkConfig *pb.NetworkConfig) (*Node, error) {
	replicas := make([]Replica, len(initialNetworkConfig.Nodes))
	for i, node := range initialNetworkConfig.Nodes {
		replicas[i] = Replica{
			ID: node,
		}
	}

	return &Node{
		Config:   config,
		Replicas: replicas,
		s: newSerializer(
			newStateMachine(
				initialNetworkConfig,
				config,
			),
			doneC,
		),
	}, nil
}

// ClientProposer TODO, decide if we want this as external API.  It should be slightly less
// expensive to retain the client proposer for clients who are submitting many txes at a time.
// On the other hand, it's more API surface that we could always expose later.
func (n *Node) ClientProposer(ctx context.Context, clientID []byte) (*ClientProposer, error) {
	replyC := make(chan *clientWaiter, 1)
	select {
	case n.s.clientC <- &clientReq{
		clientID: clientID,
		replyC:   replyC,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.s.errC:
		return nil, n.s.getExitErr()
	}

	var cw *clientWaiter

	select {
	case cw = <-replyC:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.s.errC:
		return nil, n.s.getExitErr()
	}

	return &ClientProposer{
		clientID:     clientID,
		clientWaiter: cw,
		s:            n.s,
	}, nil
}

// Propose injects a new message into the system.  The message may be
// forwarded to another node after pre-processing for ordering.  This method
// only returns an error if the context ends, or the node is stopped.
// In the case that the node is stopped gracefully it returns ErrStopped.
// If blocking is set to true, then even if this request is outside of the watermarks,
// call will wait for the watermarks to move (or the context to expire).
func (n *Node) Propose(ctx context.Context, blocking bool, requestData *pb.RequestData) error {
	cp, err := n.ClientProposer(ctx, requestData.ClientId)
	if err != nil {
		return err
	}

	return cp.Propose(ctx, blocking, requestData)
}

// Step takes authenticated messages from the other nodes in the network.  It
// is the responsibility of the caller to ensure that the message originated from
// the designed source.  This method returns an error if the context ends, the node
// stopped, or the message is not well formed (unknown proto fields, etc.).  In the
// case that the node is stopped gracefully, it returns ErrStopped.
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
	case <-n.s.errC:
		return n.s.getExitErr()
	}
}

// Status returns a static snapshot in time of the internal state of the state machine.
// This method necessarily exposes some of the internal architecture of the system, and
// especially while the library is in development, the data structures may change substantially.
// This method returns a nil status and an error if the context ends.  If the serializer go routine
// exits for any other reason, then a best effort is made to return the last (and final) status.
// This final status may be relied upon if it is non-nil.  If the serializer exited at the user's
// request (because the done channel was closed), then ErrStopped is returned.
func (n *Node) Status(ctx context.Context) (*Status, error) {
	statusC := make(chan *Status, 1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.s.statusC <- statusC:
		select {
		case status := <-statusC:
			return status, nil
		case <-n.s.errC:
		}
	case <-n.s.errC:
		// Note, we do not check the doneC, as if doneC closes, errC eventually closes
	}

	// The serializer has exited
	n.s.exitMutex.Lock()
	defer n.s.exitMutex.Unlock()

	return n.s.exitStatus, n.s.exitErr
}

// Ready returns a channel which will deliver Actions for the user to perform.
// See the documentation for Actions regarding the detailed responsibilities
// of the caller.
func (n *Node) Ready() <-chan Actions {
	return n.s.actionsC
}

// Err should never close unless the consumer has requested the library exit
// by closing the doneC supplied at construction time.  However, if unforeseen
// programatic errors violate the safety of the state machine, rather than panic
// the library will close this channel, and set an exit status.  The consumer may
// wish to call Status() to get the cause of the exit, and a best effort exit status.
// If the exit was caused gracefully (by closing the done channel), then ErrStopped
// is returned.
func (n *Node) Err() <-chan struct{} {
	return n.s.errC
}

// Tick injects a tick into the state machine.  Ticks inform the state machine that
// time has elapsed, and cause it to perform operations like emit no-op heartbeat
// batches, or transition into an epoch change.  Typically, a time.Ticker is used
// and selected on in the same select statement as Ready().  An error is returned
// only if the state machine has stopped (if it was stopped gracefully, ErrStopped is returned).
func (n *Node) Tick() error {
	select {
	case n.s.tickC <- struct{}{}:
		return nil
	case <-n.s.errC:
		return n.s.getExitErr()
	}
}

// AddResults is a callback from the consumer to the state machine, informing the
// state machine that Actions have been carried out, and the result of those
// Actions is applicable.  In the case that the node is stopped, it returns
// ErrStopped, otherwise nil is returned.
func (n *Node) AddResults(results ActionResults) error {
	select {
	case n.s.resultsC <- results:
		return nil
	case <-n.s.errC:
		return n.s.getExitErr()
	}
}
