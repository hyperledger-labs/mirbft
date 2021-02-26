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

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"
	"github.com/pkg/errors"
)

var ErrStopped = fmt.Errorf("stopped at caller request")

// WALStorage gives the state machine access to the most recently persisted state as
// requested by a previous instance of the state machine.
type WALStorage interface {
	// LoadAll will invoke the given function with the the persisted entry
	// iteratively, until the entire write-ahead-log has been loaded.
	// If an error is encountered reading the log, it is returned and iteration stops.
	LoadAll(forEach func(index uint64, p *msgs.Persistent)) error
}

// Node is the local instance of the MirBFT state machine through which the calling application
// proposes new messages, receives delegated actions, and returns action results.
// The methods exposed on Node are all thread safe, though typically, a single loop handles
// reading Actions, writing results, and writing ticks, while other go routines Propose and Step.
type Node struct {
	Config *Config
	s      *serializer
}

func StandardInitialNetworkState(nodeCount int, clientCount int) *msgs.NetworkState {
	nodes := []uint64{}
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, uint64(i))
	}

	numberOfBuckets := int32(nodeCount)
	checkpointInterval := numberOfBuckets * 5
	maxEpochLength := checkpointInterval * 10

	clients := make([]*msgs.NetworkState_Client, clientCount)
	for i := range clients {
		clients[i] = &msgs.NetworkState_Client{
			Id:           uint64(i),
			Width:        100,
			LowWatermark: 0,
		}
	}

	return &msgs.NetworkState{
		Config: &msgs.NetworkState_Config{
			Nodes:              nodes,
			F:                  int32((nodeCount - 1) / 3),
			NumberOfBuckets:    numberOfBuckets,
			CheckpointInterval: checkpointInterval,
			MaxEpochLength:     uint64(maxEpochLength),
		},
		Clients: clients,
	}
}

type dummyWAL struct {
	initialNetworkState    *msgs.NetworkState
	initialCheckpointValue []byte
}

func (dw *dummyWAL) LoadAll(forEach func(uint64, *msgs.Persistent)) error {
	forEach(1, &msgs.Persistent{
		Type: &msgs.Persistent_CEntry{
			CEntry: &msgs.CEntry{
				SeqNo:           0,
				CheckpointValue: dw.initialCheckpointValue,
				NetworkState:    dw.initialNetworkState,
			},
		},
	})

	forEach(2, &msgs.Persistent{
		Type: &msgs.Persistent_FEntry{
			FEntry: &msgs.FEntry{
				EndsEpochConfig: &msgs.EpochConfig{
					Number:  0,
					Leaders: dw.initialNetworkState.Config.Nodes,
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
	initialNetworkState *msgs.NetworkState,
	initialCheckpointValue []byte,
) (*Node, error) {
	return RestartNode(
		config,
		&dummyWAL{
			initialNetworkState:    initialNetworkState,
			initialCheckpointValue: initialCheckpointValue,
		},
	)
}

// RestartNode should be invoked for any subsequent starts of the Node.  It reads
// the supplied WAL and Request store to initialize the state machine and therefore
// does not require network parameters as they are embedded into the WAL.
func RestartNode(
	config *Config,
	walStorage WALStorage,
) (*Node, error) {
	serializer, err := newSerializer(config, walStorage)
	if err != nil {
		return nil, errors.Errorf("failed to start new node: %s", err)
	}

	return &Node{
		Config: config,
		s:      serializer,
	}, nil
}

// Stop terminates the resources associated with the node
func (n *Node) Stop() {
	n.s.stop()
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
func (n *Node) Actions() <-chan *statemachine.ActionList {
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

// InjectEvents is called by the consumer after processing actions, or because
// events such as network sends or client requests have occurred.
// If the node is stopped, it returns the exit error otherwise nil is returned.
func (n *Node) InjectEvents(events *statemachine.EventList) error {
	select {
	case n.s.eventsC <- events:
		return nil
	case <-n.s.errC:
		return n.s.getExitErr()
	}
}
