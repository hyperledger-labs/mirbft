// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package messenger

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var (
	clientConnections sync.Map

	// Synchronizes access to bucketSubscriptions and bucketAssignmentMsg.
	// A concurrent sync.Map does not do the job here, a concurrent subscription and announcement
	// could lead to a client missing an update.
	bucketAssignmentLock sync.Mutex
	bucketSubscriptions  = make(map[int32]pb.Messenger_BucketsServer)
	bucketAssignmentMsg  *pb.BucketAssignment

	ClientRequestHandler func(msg *pb.ClientRequest)
)

// Implementation of the gRPC Request service (multi-request-multi-response) used by ordering clients.
// On first client request (that it considers dummy and doesn't process normally), performs a handshake with the
// client. Submits all subsequent requests to the registered handler function.
func (ms *messengerServer) Request(srv pb.Messenger_RequestServer) error {

	// WARNING: If a simulate crash, the peer ignores all messages
	if Crashed {
		return nil
	}


	// Log address of incoming connection.
	p, ok := peer.FromContext(srv.Context())
	if ok {
		logger.Info().Str("addr", p.Addr.String()).Msg("Incoming connection for requests.")
	} else {
		logger.Error().Msg("Failed to get gRPC peer info from context.")
	}

	var err error
	var req *pb.ClientRequest

	// Exchange a dummy request and a dummy response with the client.
	err = ms.performClientHandshake(srv)

	// Call the handler for each request received from the client
	for req, err = srv.Recv(); err == nil; req, err = srv.Recv() {
		logger.Trace().
			Int32("clId", req.RequestId.ClientId).
			Int32("clSn", req.RequestId.ClientSn).
			Msg("Received request.")
		ClientRequestHandler(req)
	}

	// Log error message produced on termination of the above loop.
	// TODO: Implement graceful shutdown.
	logger.Info().Err(err).Str("address", p.Addr.String()).Msg("Connection terminated.")

	// Send gRPC response message and close connection.
	return err
}

func (ms *messengerServer) Buckets(msgSink pb.Messenger_BucketsServer) error {

	// The client always starts by sending one subscription.
	subscription, err := msgSink.Recv()
	if err != nil {
		logger.Error().Err(err).Msg("Could not receive bucket assignment subscription.")
	}

	// Store subscription for bucket assignment updates.
	bucketAssignmentLock.Lock()
	bucketSubscriptions[subscription.ClientId] = msgSink
	bucketAssignmentLock.Unlock()

	// Directly respond with the current bucket assignment if it exists.
	if bucketAssignmentMsg != nil {
		if err := msgSink.Send(bucketAssignmentMsg); err != nil {
			logger.Error().Err(err).Int32("clId", subscription.ClientId).Msg("Could not directly respond to bucket assignment subscription.")
		}
	}

	// Wait until the client closes the connection (no other actual messages are expected).
	_, err = msgSink.Recv()
	if err != io.EOF {
		logger.Err(err).Msg("Expected EOF on Buckets RPC.")
	} else {
		logger.Info().Int32("clId", subscription.ClientId).Msg("Finished sending bucket assignments to client.")
	}

	return nil
}

func AnnounceBucketAssignment(assignment *pb.BucketAssignment) {
	bucketAssignmentLock.Lock()
	defer bucketAssignmentLock.Unlock()

	// Update current bucket assignment.
	bucketAssignmentMsg = assignment

	// Announce new assignment to all subscribers.
	for clID, msgSink := range bucketSubscriptions {
		if err := msgSink.Send(bucketAssignmentMsg); err != nil {
			logger.Warn().Err(err).Int32("clId", clID).Msg("Could not announce bucket assignment.")
		}
	}
}

// Performs an initial handshake with a connecting client.
// One dummy request message and one dummy response message are used for this.
// Those messages are not treated by the ordering protocol and only serve for synchronizing the client and the peer.
// The client is expected to send one dummy request, on the reception of which the peer initializes the required
// client-related data structures. The peer then responds with a dummy response, indicating to the client that the
// peer is ready to send actual responses to actual requests.
func (ms *messengerServer) performClientHandshake(srv pb.Messenger_RequestServer) error {

	// Receive first dummy Request from client.
	// This is not an actual request and only serves for establishing the connection.
	req, err := srv.Recv()
	if err != nil {
		logger.Error().Msg("Error receiving first (dummy) client request.")
		return err
	}

	// Save the connection to the client.
	registerClientConnection(srv, req.RequestId.ClientId)

	// Send a dummy response to the client.
	// This is not an actual response, and only serves as an acknowledgment to the client that the connection has
	// been registered at the server.
	return srv.Send(&pb.ClientResponse{
		ClientSn: -1,
	})
}

// Saves the client connection in clientConnections.
// Required for sending responses to the client.
func registerClientConnection(srv pb.Messenger_RequestServer, clientID int32) {

	// Assert that client is not yet registered. This should never happen
	// (For now we assume that a client never disconnects and reconnects.)
	if _, ok := clientConnections.Load(clientID); ok {
		logger.Error().
			Int32("clientID", clientID).
			Msg("Client ID already registered. Ignoring. Two clients with the same ID?")
		return
	}

	// Save the client connection.
	clientConnections.Store(clientID, srv)
}

// Sends a response to a client request.
func RespondToClient(clientID int32, response *pb.ClientResponse) {

	// WARNING: If a simulate crash, the peer ignores all messages
	if Crashed {
		return
	}

	// Check whether the client connection is present.
	if srv, ok := clientConnections.Load(clientID); ok {

		// Try sending the response.
		if err := srv.(pb.Messenger_RequestServer).Send(response); err != nil {

			// Log sending error.
			logger.Error().
				Err(err).
				Int32("clientID", clientID).
				Msg("Error responding to client. No connection present.")
		}
	} else {
		// Log error if connection is not present.
		logger.Error().Int32("clientID", clientID).Msg("Not respondig to client. No connection present.")
	}
}

// Creates connections to all the orderers and returns them as a slice of gRPC client stubs.
// This function is used by the client.
func ConnectToOrderers(ownClientID int32, clientLog zerolog.Logger, ordererIDs []int32) (map[int32]pb.Messenger_RequestClient, map[int32]pb.Messenger_BucketsClient, map[int32]*grpc.ClientConn) {

	var mapLock sync.Mutex
	var wg sync.WaitGroup
	reqClients := make(map[int32]pb.Messenger_RequestClient, len(ordererIDs))
	bucketClients := make(map[int32]pb.Messenger_BucketsClient, len(ordererIDs))
	reqConns := make(map[int32]*grpc.ClientConn, len(ordererIDs))

	wg.Add(len(ordererIDs))
	// For each known orderer identity
	for _, id := range ordererIDs {

		// In parallel
		go func(peerID int32) {
			defer wg.Done()

			// Create a connection to orderer (represented by a gRPC client stub).
			reqClient, bucketClient, reqConn := connectToOrderer(peerID, ownClientID, clientLog)

			// Save client stub in clientStubs (or log an error on failure).
			if reqClient != nil && bucketClient != nil {
				mapLock.Lock()
				reqClients[peerID] = reqClient
				bucketClients[peerID] = bucketClient
				reqConns[peerID] = reqConn
				mapLock.Unlock()
			} else {
				clientLog.Error().Int32("ordererId", peerID).Msg("Could not connect to orderer.")
			}
		}(id)
	}
	wg.Wait()

	return reqClients, bucketClients, reqConns
}

// Connects to a single orderer node and returns a message sink (gRPC stub),
// through which messages destined to the orderer node can be sent.
// This function is used by connectToOrderers when the client is connecting to the system.
func connectToOrderer(ordererID int32, ownClientID int32, clientLog zerolog.Logger) (pb.Messenger_RequestClient, pb.Messenger_BucketsClient, *grpc.ClientConn) {

	// Get network address of orderer.
	// The client uses the public address of the orderer
	// (while the peers communicate through their private addresses).
	identity := membership.NodeIdentity(ordererID)
	addrString := fmt.Sprintf("%s:%d", identity.PublicAddr, identity.Port)

	clientLog.Info().Int32("peerId", ordererID).Str("addrStr", addrString).Msg("Connecting to orderer.")

	// Create connection for requests
	reqConn, err := newGRPCClientConnection(addrString)
	if err != nil {
		clientLog.Error().Str("addrStr", addrString).Msg("Couldn't connect to orderer")
		return nil, nil, nil
	}

	// Create connection for bucket assignment updates
	bucketConn, err := newGRPCClientConnection(addrString)
	if err != nil {
		clientLog.Error().Str("addrStr", addrString).Msg("Couldn't connect to orderer")
		return nil, nil, nil
	}

	// Create client stubs.
	reqStub := pb.NewMessengerClient(reqConn)
	bucketStub := pb.NewMessengerClient(bucketConn)

	// Remotely invoke the Request function on the orderer's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	reqClient, err := reqStub.Request(context.Background())
	if err != nil {
		clientLog.Error().Str("addrStr", addrString).Err(err).Msg("Could not invoke Request RPC.")
		if err := reqConn.Close(); err != nil {
			clientLog.Error().Str("addrStr", addrString).Err(err).Msg("Error closing connection to orderer.")
		}
		return nil, nil, nil
	}

	// Remotely invoke the Request function on the orderer's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	bucketClient, err := bucketStub.Buckets(context.Background())
	if err != nil {
		clientLog.Error().Str("addrStr", addrString).Err(err).Msg("Could not invoke Request RPC.")
		if err := bucketConn.Close(); err != nil {
			clientLog.Error().Str("addrStr", addrString).Err(err).Msg("Error closing connection to orderer.")
		}
		return nil, nil, nil
	}
	if err = bucketClient.Send(&pb.BucketSubscription{ClientId: ownClientID}); err != nil {
		clientLog.Error().Str("addrStr", addrString).Err(err).Msg("Could not send bucket assignment subscription.")
	}

	// Perform an initial handshake with the server, exchanging one dummy request and one dummy response.
	// TODO: Is this still necessary when using secure connections? (Probably yes.)
	performServerHandshake(reqClient, ownClientID)
	clientLog.Info().Int32("id", identity.NodeId).Str("addrStr", addrString).Msg("Connected to orderer.")

	// Return gRPC message sinks and connections.
	return reqClient, bucketClient, reqConn
}

func newGRPCClientConnection(addrString string) (*grpc.ClientConn, error) {

	// Set general gRPC dial options.
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	}

	// Add TLS-specific gRPC dial options, depending on configuration
	if config.Config.UseTLS {
		tlsConfig := ConfigureTLS(config.Config.CertFile, config.Config.KeyFile)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	// Set up a gRPC connection.
	return grpc.Dial(addrString, dialOpts...)
}

// Performs an initial application-level handshake with the server.
// Sends a dummy request and waits for a dummy response.
// This ensures that the server knows about this client and is ready for sending actual responses to actual requests.
// TODO: Is this still necessary when using secure connections? (Probably yes.)
func performServerHandshake(cl pb.Messenger_RequestClient, ownClientID int32) {

	// Send dummy request.
	err := cl.Send(&pb.ClientRequest{
		RequestId: &pb.RequestID{
			ClientId: ownClientID,
			ClientSn: -1,
		},
		Payload:   nil,
		Signature: nil,
	})
	if err != nil {
		logger.Error().Msg("Failed to send dummy request to server during handshake.")
	}

	// Wait for dummy response.
	_, err = cl.Recv()
	if err != nil {
		logger.Error().Msg("Failed to receive dummy response from server during handshake.")
	}
}
