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
	"github.com/IBM/mirbft/status"
	"github.com/pkg/errors"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

// WALStorage gives the state machine access to the most recently persisted state as
// requested by a previous instance of the state machine.
type WALStorage interface {
	// LoadAll will invoke the given function with the the persisted entry
	// iteratively, until the entire write-ahead-log has been loaded.
	// If an error is encountered reading the log, it is returned and iteration stops.
	LoadAll(forEach func(index uint64, p *pb.Persistent)) error
}

type RequestStorage interface {
	// Uncommitted must invoke forEach on each uncommitted entry in the
	// request store.  These requests must have been safely committed
	// to the request store before the actions which contained them
	// send the corresponding RequestAck.
	Uncommitted(forEach func(*pb.RequestAck)) error
}

// Node is the local instance of the MirBFT state machine through which the calling application
// proposes new messages, receives delegated actions, and returns action results.
// The methods exposed on Node are all thread safe, though typically, a single loop handles
// reading Actions, writing results, and writing ticks, while other go routines Propose and Step.
type Node struct {
	Config *Config
	s      *serializer
}

type ClientProposer struct {
	blocking     bool
	clientID     uint64
	clientWaiter *clientWaiter
	s            *serializer
}

// TODO, change requestData to bytes
func (cp *ClientProposer) Propose(ctx context.Context, requestData *pb.Request) error {
	for {
		if requestData.ReqNo < cp.clientWaiter.lowWatermark {
			return errors.Errorf("request %d below watermarks, lowWatermark=%d", requestData.ReqNo, cp.clientWaiter.lowWatermark)
		}

		if requestData.ReqNo <= cp.clientWaiter.highWatermark {
			break
		}

		if cp.blocking {
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
	case cp.s.propC <- &pb.StateEvent_Proposal{
		Request: requestData,
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-cp.s.errC:
		return cp.s.getExitErr()
	}
}

// TODO, maybe get rid of the varargs and go with a simple count?
func StandardInitialNetworkState(nodeCount int, clientIDs ...uint64) *pb.NetworkState {
	nodes := []uint64{}
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, uint64(i))
	}

	numberOfBuckets := int32(nodeCount)
	checkpointInterval := numberOfBuckets * 5
	maxEpochLength := checkpointInterval * 10

	clients := make([]*pb.NetworkState_Client, len(clientIDs))
	for i, clientID := range clientIDs {
		clients[i] = &pb.NetworkState_Client{
			Id:           clientID,
			Width:        100,
			LowWatermark: 0,
		}
	}

	return &pb.NetworkState{
		Config: &pb.NetworkState_Config{
			Nodes:              nodes,
			F:                  int32((nodeCount - 1) / 3),
			NumberOfBuckets:    numberOfBuckets,
			CheckpointInterval: checkpointInterval,
			MaxEpochLength:     uint64(maxEpochLength),
		},
		Clients: clients,
	}
}

type dummyReqStore struct{}

func (dummyReqStore) Uncommitted(forEach func(*pb.RequestAck)) error {
	return nil
}

type dummyWAL struct {
	initialNetworkState    *pb.NetworkState
	initialCheckpointValue []byte
}

func (dw *dummyWAL) LoadAll(forEach func(uint64, *pb.Persistent)) error {
	forEach(1, &pb.Persistent{
		Type: &pb.Persistent_CEntry{
			CEntry: &pb.CEntry{
				SeqNo:           0,
				CheckpointValue: dw.initialCheckpointValue,
				NetworkState:    dw.initialNetworkState,
				CurrentEpoch:    0,
			},
		},
	})

	forEach(2, &pb.Persistent{
		Type: &pb.Persistent_EpochChange{
			EpochChange: &pb.EpochChange{
				NewEpoch: 1,
				Checkpoints: []*pb.Checkpoint{
					{
						SeqNo: 0,
						Value: dw.initialCheckpointValue,
					},
				},
			},
		},
	})

	return nil
}

// StartNewNode creates a node to join a fresh network.  The first actions returned
// by the node will be to persist the initial parameters to the WAL so that on subsequent
// starts RestartNode should be invoked instead.  The initialCheckpointValue should reflect
// any initial state of the application as well as the initialNetworkState passed to the start.
func StartNewNode(
	config *Config,
	initialNetworkState *pb.NetworkState,
	initialCheckpointValue []byte,
) (*Node, error) {
	return RestartNode(
		config,
		&dummyWAL{
			initialNetworkState:    initialNetworkState,
			initialCheckpointValue: initialCheckpointValue,
		},
		dummyReqStore{},
	)
}

// RestartNode should be invoked for any subsequent starts of the Node.  It reads
// the supplied WAL and Request store to initialize the state machine and therefore
// does not require network parameters as they are embedded into the WAL.
func RestartNode(
	config *Config,
	walStorage WALStorage,
	reqStorage RequestStorage,
) (*Node, error) {
	serializer, err := newSerializer(config, walStorage, reqStorage)
	if err != nil {
		return nil, errors.Errorf("failed to start new node: %s", err)
	}

	return &Node{
		Config: config,
		s:      serializer,
	}, nil
}

type ClientProposerOption interface{}

type clientProposerBlocking struct {
	shouldBlock bool
}

// WaitForRoom indicates whether the client proposer should block, waiting for
// space to become available in the client window.  If set to false, the client
// will immediately return with an error if the window exhausts.
func WaitForRoom(shouldBlock bool) ClientProposerOption {
	return clientProposerBlocking{
		shouldBlock: shouldBlock,
	}
}

// Stop terminates the resources associated with the node
func (n *Node) Stop() {
	n.s.stop()
}

// ClientProposer returns a new ClientProposer for a given clientID.  It is the caller's
// responsibility to ensure that this method is never invoked twice with the same clientID.
func (n *Node) ClientProposer(ctx context.Context, clientID uint64, options ...ClientProposerOption) (*ClientProposer, error) {
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

	if cw == nil {
		return nil, errors.Errorf("client ID not found to be registered")
	}

	blocking := true
	for _, option := range options {
		switch o := option.(type) {
		case clientProposerBlocking:
			blocking = o.shouldBlock
		default:
			panic("unknown option")
		}
	}

	return &ClientProposer{
		blocking:     blocking,
		clientID:     clientID,
		clientWaiter: cw,
		s:            n.s,
	}, nil
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

	stepEvent := &pb.StateEvent_Step{
		Step: &pb.StateEvent_InboundMsg{
			Source: source,
			Msg:    msg,
		},
	}

	select {
	case n.s.stepC <- stepEvent:
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
func (n *Node) Status(ctx context.Context) (*status.StateMachine, error) {
	statusC := make(chan *status.StateMachine, 1)

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
	stateEventResults := &pb.StateEvent_ActionResults{
		Digests:     make([]*pb.HashResult, len(results.Digests)),
		Checkpoints: make([]*pb.CheckpointResult, len(results.Checkpoints)),
	}

	for i, hashResult := range results.Digests {
		// TODO, I really don't like this... but it should be safe
		hashResult.Request.Origin.Digest = hashResult.Digest
		stateEventResults.Digests[i] = hashResult.Request.Origin
	}

	for i, cr := range results.Checkpoints {
		stateEventResults.Checkpoints[i] = &pb.CheckpointResult{
			SeqNo: cr.Checkpoint.SeqNo,
			Value: cr.Value,
			NetworkState: &pb.NetworkState{
				Config:                  cr.Checkpoint.NetworkConfig,
				Clients:                 cr.Checkpoint.ClientsState,
				PendingReconfigurations: cr.Reconfigurations,
			},
		}
	}

	select {
	case n.s.resultsC <- stateEventResults:
		return nil
	case <-n.s.errC:
		return n.s.getExitErr()
	}
}
