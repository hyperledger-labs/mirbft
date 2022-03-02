/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestreceiver

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"strconv"
	"sync"
)

type RequestReceiver struct {
	UnimplementedRequestReceiverServer

	// The Node to which to submit the received requests.
	node *mirbft.Node

	// The gRPC server used by this networking module.
	grpcServer *grpc.Server

	// Wait group that is notified when the grpcServer stops.
	// Waiting on this WaitGroup makes sure that the server exit status has been recorded correctly.
	grpcServerWg sync.WaitGroup

	// Error returned from the grpcServer.Serve() call (see Start() method).
	grpcServerError error

	// Logger use for all logging events of this RequestReceiver
	logger logging.Logger
}

// NewRequestReceiver returns a new initialized request receiver.
// The returned RequestReceiver is not yet running (able to receive requests).
// This needs to be done explicitly by calling the Start() method.
// For the requests to be processed by passed Node, the Node must also be running.
func NewRequestReceiver(node *mirbft.Node, logger logging.Logger) *RequestReceiver {
	// If no logger was given, only write errors to the console.
	if logger == nil {
		logger = logging.ConsoleErrorLogger
	}

	return &RequestReceiver{
		node:   node,
		logger: logger,
	}
}

// Listen implements the gRPC Listen service (multi-request-single-response).
// It receives messages from the gRPC client running on the MirBFT client
// and submits them to the Node associated with this RequestReceiver.
// This function is called by the gRPC system on every new connection
// from a MirBFT client's gRPC client.
func (rr *RequestReceiver) Listen(srv RequestReceiver_ListenServer) error {

	// Print address of incoming connection.
	p, ok := peer.FromContext(srv.Context())
	if ok {
		rr.logger.Log(logging.LevelDebug, fmt.Sprintf("Incoming connection from %s", p.Addr.String()))
	} else {
		return fmt.Errorf("failed to get grpc peer info from context")
	}

	// Declare loop variables outside, since err is checked also after the loop finishes.
	var err error
	var req *requestpb.Request

	// For each received request
	for req, err = srv.Recv(); err == nil; req, err = srv.Recv() {

		rr.logger.Log(logging.LevelInfo, "Received request",
			"clId", req.ClientId, "reqNo", req.ReqNo, "authLen", len(req.Authenticator))

		// Submit the request to the Node.
		if srErr := rr.node.SubmitRequest(
			context.Background(),
			t.ClientID(req.ClientId),
			t.ReqNo(req.ReqNo),
			req.Data,
			req.Authenticator,
		); srErr != nil {

			// If submitting fails, stop receiving further request (and close connection).
			rr.logger.Log(logging.LevelError, fmt.Sprintf("Could not submit request (%d-%d): %v. Closing connection.",
				req.ClientId, req.ReqNo, srErr))
			break
		}
	}

	// If the connection was terminated by the gRPC client, print the reason.
	// (This line could also be reached by breaking out of the above loop on request submission error.)
	if err != nil {
		rr.logger.Log(logging.LevelWarn, fmt.Sprintf("Connection terminated: %s (%v)", p.Addr.String(), err))
	}

	// Send gRPC response message and close connection.
	return srv.SendAndClose(&ByeBye{})
}

// Start starts the RequestReceiver by initializing and starting the internal gRPC server,
// listening on the passed port.
// Before ths method is called, no client connections are accepted.
func (rr *RequestReceiver) Start(port int) error {

	rr.logger.Log(logging.LevelInfo, fmt.Sprintf("Listening for request connections on port %d", port))

	// Create a gRPC server and assign it the logic of this RequestReceiver.
	rr.grpcServer = grpc.NewServer()
	RegisterRequestReceiverServer(rr.grpcServer, rr)

	// Start listening on the network
	conn, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("failed to listen for connections on port %d: %w", port, err)
	}

	// Start the gRPC server in a separate goroutine.
	// When the server stops, it will write its exit error into gt.grpcServerError.
	rr.grpcServerWg.Add(1)
	go func() {
		rr.grpcServerError = rr.grpcServer.Serve(conn)
		rr.grpcServerWg.Done()
	}()

	// If we got all the way here, no error occurred.
	return nil
}

// Stop stops the own gRPC server (preventing further incoming connections).
// After Stop() returns, the error returned by the gRPC server's Serve() call
// can be obtained through the ServerError() method.
func (rr *RequestReceiver) Stop() {

	rr.logger.Log(logging.LevelDebug, "Stopping request receiver.")

	// Stop own gRPC server and wait for its exit status to be recorded.
	rr.grpcServer.Stop()
	rr.grpcServerWg.Wait()

	rr.logger.Log(logging.LevelDebug, "Request receiver stopped.")
}

// ServerError returns the error returned by the gRPC server's Serve() call.
// ServerError() must not be called before the RequestReceiver is stopped and its Stop() method has returned.
func (rr *RequestReceiver) ServerError() error {
	return rr.grpcServerError
}
