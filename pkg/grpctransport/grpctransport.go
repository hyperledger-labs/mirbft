/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package grpctransport

import (
	"context"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	// Maximum size of a gRPC message
	maxMessageSize = 1073741824
)

// GrpcTransport represents a networking module that is based on gRPC.
// Each node's networking module contains one gRPC server, to which other nodes' modules connect.
// The type of gRPC connection is multi-request-single-response, where each module contains
// one instance of a gRPC client per node.
// A message to a node is sent as request to that node's gRPC server.
type GrpcTransport struct {

	// The numeric ID of the node that uses this networking module.
	ownId uint64

	// Complete static membership of the system.
	// Maps the numeric node ID of each node in the system to a string representation of its network address.
	// The address format "IPAddress:port"
	membership map[uint64]string // nodeId -> "IPAddress:port"

	// Channel to which all incoming messages are written.
	// This channel is also returned by the ReceiveChan() method.
	incomingMessages chan modules.ReceivedMessage

	// For each node ID, stores a gRPC message sink, calling the Send() method of which sends a message to that node.
	connections map[uint64]GrpcTransport_ListenClient

	// The gRPC server used by this networking module.
	grpcServer *grpc.Server

	// Error returned from the grpcServer.Serve() call (see Start() method).
	grpcServerError error
}

// NewGrpcTransport returns a pointer to a new initialized GrpcTransport networking module.
// The membership parameter must represent the complete static membership of the system.
// It maps the numeric node ID of each node in the system to
// a string representation of its network address with the format "IPAddress:port".
// The ownId parameter is the numeric ID of the node that will use the returned networking module.
// The returned GrpcTransport is not yet running (able to receive messages),
// nor is it connected to any nodes (able to send messages).
// This needs to be done explicitly by calling the respective Start() and Connect() methods.
func NewGrpcTransport(membership map[uint64]string, ownId uint64) *GrpcTransport {
	return &GrpcTransport{
		ownId:            ownId,
		incomingMessages: make(chan modules.ReceivedMessage),
		membership:       membership,
		connections:      make(map[uint64]GrpcTransport_ListenClient),
	}
}

// Send sends msg to the node with ID dest.
// Concurrent calls to Send are not (yet? TODO) supported.
func (gt *GrpcTransport) Send(dest uint64, msg *messagepb.Message) error {
	return gt.connections[dest].Send(&GrpcMessage{Sender: gt.ownId, Msg: msg})
}

// ReceiveChan returns a channel to which the Net module writes all received messages and sender IDs
// (Both the message itself and the sender ID are part of the ReceivedMessage struct.)
func (gt *GrpcTransport) ReceiveChan() <-chan modules.ReceivedMessage {
	return gt.incomingMessages
}

// Listen implements the gRPC Listen service (multi-request-single-response).
// It receives messages from the gRPC client running on the other node
// and writes them to a channel that the user can access through ReceiveChan().
// This function is called by the gRPC system on every new connection
// from another node's Net module's gRPC client.
func (gt *GrpcTransport) Listen(srv GrpcTransport_ListenServer) error {

	// Print address of incoming connection.
	p, ok := peer.FromContext(srv.Context())
	if ok {
		fmt.Printf("Incoming connection from %s\n", p.Addr.String())
	} else {
		return fmt.Errorf("failed to get grpc peer info from context")
	}

	// Declare loop variables outside, since err is checked also after the loop finishes.
	var err error
	var grpcMsg *GrpcMessage

	// For each message received
	for grpcMsg, err = srv.Recv(); err == nil; grpcMsg, err = srv.Recv() {
		// Write the message to the channel. This channel will be read by the user of the module.
		gt.incomingMessages <- modules.ReceivedMessage{Sender: grpcMsg.Sender, Msg: grpcMsg.Msg}
	}

	// Log error message produced on termination of the above loop.
	fmt.Printf("Connection terminated: %s (%v)\n", p.Addr.String(), err)

	// Send gRPC response message and close connection.
	return srv.SendAndClose(&ByeBye{})
}

// Start starts the networking module by initializing and starting the internal gRPC server,
// listening on the port determined by the membership and own ID.
// Before ths method is called, no other GrpcTransports can connect to this one.
func (gt *GrpcTransport) Start() error {

	// Obtain own port number from membership.
	_, ownPort, err := splitAddrPort(gt.membership[gt.ownId])

	fmt.Printf("Listening for connections on port %d\n", ownPort)

	// Create a gRPC server and assign it the logic of this module.
	gt.grpcServer = grpc.NewServer()
	RegisterGrpcTransportServer(gt.grpcServer, gt)

	// Start listening on the network
	conn, err := net.Listen("tcp", ":"+strconv.Itoa(ownPort))
	if err != nil {
		return fmt.Errorf("failed to listen for connections on port %d", ownPort)
	}

	// Start the gRPC server in a separate goroutine.
	// When the server stops, it will write its exit error into gt.grpcServerError.
	go func() {
		gt.grpcServerError = gt.grpcServer.Serve(conn)
	}()

	// If we got all the way here, no error occurred.
	return nil
}

// Stop closes all open connections to other nodes and stops the own gRPC server
// (preventing further incoming connections).
// After Stop() returns, the error returned by the gRPC server's Serve() call
// can be obtained through the Error() method.
func (gt *GrpcTransport) Stop() {

	// Close connections to other nodes.
	for id, connection := range gt.connections {
		if _, err := connection.CloseAndRecv(); err != nil {
			fmt.Printf("Could not close connection to node %d\n", id)
		}
	}

	// Stop own gRPC server.
	gt.grpcServer.GracefulStop()
}

// ServerError returns the error returned by the gRPC server's Serve() call.
// ServerError() must not be called before the GrpcTransport is stopped and its Stop() method has returned.
func (gt *GrpcTransport) ServerError() error {
	return gt.grpcServerError
}

// Connect establishes (in parallel) network connections to all nodes in the system.
// The other nodes' GrpcTransport modules must be running.
// Only after Connect() returns, sending messages over this GrpcTransport is possible.
// TODO: Deal with errors, e.g. when the connection times out (make sure the RPC call in connectToPeer() has a timeout).
func (gt *GrpcTransport) Connect() {

	// Initialize wait group used by the connecting goroutines
	wg := sync.WaitGroup{}
	wg.Add(len(gt.membership))

	// Synchronizes concurrent access to connections.
	lock := sync.Mutex{}

	// For each node in the membership
	for peerId, peerAddr := range gt.membership {

		// Launch a goroutine that connects to the node.
		go func(id uint64, addr string) {
			defer wg.Done()

			// Create and store connection
			connection, err := connectToPeer(addr) // May take long time, execute before acquiring the lock.
			lock.Lock()
			gt.connections[id] = connection
			lock.Unlock()

			// Print debug info.
			if err != nil {
				fmt.Printf("Failed to connect to peer %d (%s) connected.\n", id, addr)
			} else {
				fmt.Printf("Peer %d (%s) connected.\n", id, addr)
			}

		}(peerId, peerAddr)
	}

	// Wait for connecting goroutines to finish.
	wg.Wait()
}

// Establishes a connection to a single peer at address addrString.
func connectToPeer(addrString string) (GrpcTransport_ListenClient, error) {

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
	client := NewGrpcTransportClient(conn)

	// Remotely invoke the Listen function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.Listen(context.Background())
	if err != nil {
		if cerr := conn.Close(); cerr != nil {
			fmt.Printf("Failed to close connection: %v", cerr)
		}
		return nil, err
	}

	// Return the message sink connected to the peer.
	return msgSink, nil
}

// Parses an address string with the format "IPAddress:port" into a string address and an integer port number.
func splitAddrPort(addrString string) (string, int, error) {

	// Split string at the colon character into two.
	s := strings.Split(strings.TrimSpace(addrString), ":")
	if len(s) != 2 {
		return "", 0, fmt.Errorf("address string must contain exactly one colon character (:)")
	}

	// The address is the part before the colon
	addr := s[0]

	// Convert the part after the colon to an integer.
	if port, err := strconv.Atoi(s[1]); err != nil {
		return "", 0, fmt.Errorf("port must be a valid number")
	} else {

		// If conversion succeeds, return parsed values.
		return addr, port, nil
	}

}
