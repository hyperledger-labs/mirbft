/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dummyclient

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/requestreceiver"
	"google.golang.org/grpc"
	"sync"
)

const (
	// Maximum size of a gRPC message
	maxMessageSize = 1073741824
)

type DummyClient struct {
	ownId       uint64
	nextReqNo   uint64
	connections map[uint64]requestreceiver.RequestReceiver_ListenClient
}

func NewDummyClient(clientId uint64) *DummyClient {
	return &DummyClient{
		ownId:       clientId,
		nextReqNo:   0,
		connections: make(map[uint64]requestreceiver.RequestReceiver_ListenClient),
	}
}

// Connect establishes (in parallel) network connections to all nodes in the system.
// The nodes' RequestReceivers must be running.
// Only after Connect() returns, sending requests through this DummyClient is possible.
// TODO: Deal with errors, e.g. when the connection times out (make sure the RPC call in connectToNode() has a timeout).
func (dc *DummyClient) Connect(membership map[uint64]string) {

	// Initialize wait group used by the connecting goroutines
	wg := sync.WaitGroup{}
	wg.Add(len(membership))

	// Synchronizes concurrent access to connections.
	lock := sync.Mutex{}

	// For each node in the membership
	for nodeId, nodeAddr := range membership {

		// Launch a goroutine that connects to the node.
		go func(id uint64, addr string) {
			defer wg.Done()

			// Create and store connection
			connection, err := connectToNode(addr) // May take long time, execute before acquiring the lock.
			lock.Lock()
			dc.connections[id] = connection
			lock.Unlock()

			// Print debug info.
			if err != nil {
				fmt.Printf("Failed to connect to node %d (%s) connected.\n", id, addr)
			} else {
				fmt.Printf("Node %d (%s) connected.\n", id, addr)
			}

		}(nodeId, nodeAddr)
	}

	// Wait for connecting goroutines to finish.
	wg.Wait()
}

// SubmitRequest submits a request by sending it to all nodes (as configured when creating the DummyClient).
// It automatically appends meta-info like client ID and request number.
// SubmitRequest must not be called concurrently.
// If an error occurs, SubmitRequest returns immediately,
// even if sending of the request was not attempted for all nodes.
func (dc *DummyClient) SubmitRequest(data []byte) error {
	reqMsg := &messagepb.Request{
		ClientId: dc.ownId,
		ReqNo:    dc.nextReqNo,
		Data:     data,
	}

	for _, connection := range dc.connections {
		if err := connection.Send(reqMsg); err != nil {
			return err
		}
	}

	return nil
}

// Disconnect closes all open connections to MirBFT nodes.
func (dc *DummyClient) Disconnect() {

	// Close connections to all nodes.
	for id, connection := range dc.connections {
		if _, err := connection.CloseAndRecv(); err != nil {
			fmt.Printf("Could not close connection to node %d\n", id)
		}
	}
}

// Establishes a connection to a single node at address addrString.
func connectToNode(addrString string) (requestreceiver.RequestReceiver_ListenClient, error) {

	fmt.Printf("Connecting to node: %s\n", addrString)

	// Set general gRPC dial options.
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		grpc.WithInsecure(),
	}

	// Set up a gRPC connection.
	conn, err := grpc.Dial(addrString, dialOpts...)
	if err != nil {
		return nil, err
	}

	// Register client stub.
	client := requestreceiver.NewRequestReceiverClient(conn)

	// Remotely invoke the Listen function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.Listen(context.Background())
	if err != nil {
		if cerr := conn.Close(); cerr != nil {
			fmt.Printf("Failed to close connection: %v", cerr)
		}
		return nil, err
	}

	// Return the message sink connected to the node.
	return msgSink, nil
}
