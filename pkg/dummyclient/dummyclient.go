/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dummyclient

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"github.com/hyperledger-labs/mirbft/pkg/requestreceiver"
	"github.com/hyperledger-labs/mirbft/pkg/serializing"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"google.golang.org/grpc"
	"sync"
)

const (
	// Maximum size of a gRPC message
	maxMessageSize = 1073741824
)

// TODO: Update the comments around crypto, hasher, and request signing.

type DummyClient struct {
	ownId       t.ClientID
	hasher      modules.Hasher
	crypto      modules.Crypto
	nextReqNo   t.ReqNo
	connections map[t.NodeID]requestreceiver.RequestReceiver_ListenClient
	logger      logging.Logger
}

func NewDummyClient(clientId t.ClientID, hasher modules.Hasher, crypto modules.Crypto, l logging.Logger) *DummyClient {

	// If no logger was given, only write errors to the console.
	if l == nil {
		l = logging.ConsoleErrorLogger
	}

	return &DummyClient{
		ownId:       clientId,
		hasher:      hasher,
		crypto:      crypto,
		nextReqNo:   0,
		connections: make(map[t.NodeID]requestreceiver.RequestReceiver_ListenClient),
		logger:      l,
	}
}

// Connect establishes (in parallel) network connections to all nodes in the system.
// The nodes' RequestReceivers must be running.
// Only after Connect() returns, sending requests through this DummyClient is possible.
// TODO: Deal with errors, e.g. when the connection times out (make sure the RPC call in connectToNode() has a timeout).
func (dc *DummyClient) Connect(ctx context.Context, membership map[t.NodeID]string) {

	// Initialize wait group used by the connecting goroutines
	wg := sync.WaitGroup{}
	wg.Add(len(membership))

	// Synchronizes concurrent access to connections.
	lock := sync.Mutex{}

	// For each node in the membership
	for nodeId, nodeAddr := range membership {

		// Launch a goroutine that connects to the node.
		go func(id t.NodeID, addr string) {
			defer wg.Done()

			// Create and store connection
			connection, err := dc.connectToNode(ctx, addr) // May take long time, execute before acquiring the lock.
			lock.Lock()
			dc.connections[id] = connection
			lock.Unlock()

			// Print debug info.
			if err != nil {
				dc.logger.Log(logging.LevelError,
					fmt.Sprintf("Failed to connect to node %d (%s).", id, addr))
			} else {
				dc.logger.Log(logging.LevelDebug,
					fmt.Sprintf("Node %d (%s) connected.", id, addr))
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

	// Create new request message.
	reqMsg := &requestpb.Request{
		ClientId: dc.ownId.Pb(),
		ReqNo:    dc.nextReqNo.Pb(),
		Data:     data,
	}
	dc.nextReqNo++

	// Compute request hash (for signing).
	h := dc.hasher.New()
	for _, data := range serializing.RequestForHash(reqMsg) {
		h.Write(data)
	}

	// Sign (the hash of) the request, adding the signature to the request object itself.
	if signature, err := dc.crypto.Sign([][]byte{h.Sum(nil)}); err == nil {
		reqMsg.Authenticator = signature
	} else {
		return err
	}

	// Declare variables keeping track of failed send attempts.
	sendFailures := make([]t.NodeID, 0) // List of nodes to which sending the request failed.
	var firstSndErr error = nil         // The error produced by the first sending failure.

	// Send the request to all nodes.
	for nID, connection := range dc.connections {
		if err := connection.Send(reqMsg); err != nil {

			// If sending the request to a node fails, record that node's ID.
			sendFailures = append(sendFailures, nID)

			// If this is the first failure, save it for later reporting.
			if firstSndErr == nil {
				firstSndErr = err
			}
		}
	}

	// Return an error summarizing the failed send attempts or nil if the request was successfully sent to all nodes.
	if firstSndErr != nil {
		return fmt.Errorf("failed sending request to nodes: %v first error: %w", sendFailures, firstSndErr)
	} else {
		return nil
	}
}

// Disconnect closes all open connections to MirBFT nodes.
func (dc *DummyClient) Disconnect() {

	// Close connections to all nodes.
	for id, connection := range dc.connections {
		if connection == nil {
			dc.logger.Log(logging.LevelWarn, fmt.Sprintf("No connection to close to node %d", id))
		} else if _, err := connection.CloseAndRecv(); err != nil {
			dc.logger.Log(logging.LevelWarn, fmt.Sprintf("Could not close connection to node %d", id))
		}
	}
}

// Establishes a connection to a single node at address addrString.
func (dc *DummyClient) connectToNode(ctx context.Context, addrString string) (requestreceiver.RequestReceiver_ListenClient, error) {

	dc.logger.Log(logging.LevelDebug, fmt.Sprintf("Connecting to node: %s", addrString))

	// Set general gRPC dial options.
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		grpc.WithInsecure(),
	}

	// Set up a gRPC connection.
	conn, err := grpc.DialContext(ctx, addrString, dialOpts...)
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
			dc.logger.Log(logging.LevelWarn, fmt.Sprintf("Failed to close connection: %v", cerr))
		}
		return nil, err
	}

	// Return the message sink connected to the node.
	return msgSink, nil
}
