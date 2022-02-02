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
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	logger "github.com/rs/zerolog/log"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// TODO: Move relevant constants to some config file
const (
	// Maximum size of a gRPC message
	maxMessageSize = 134217728 // 128 MB
)

// Simulated Crash Flag. It is set true if the peer is supposed to have crashed.
var Crashed = false

// Channels holding protocol messages to be sent to nodes, indexed by destination node ID.
var peerConnections = make(map[int32]PeerConnection)

// Message handlers. These variables hold functions the messenger calls on reception of messages of the corresponding
// type. Modules using the messenger must assign functions to these variables before the messenger is started (Start())
var CheckpointMsgHandler func(msg *pb.CheckpointMsg, senderID int32)
var StateTransferMsgHandler func(msg *pb.ProtocolMessage)
var OrdererMsgHandler func(msg *pb.ProtocolMessage)

type connectionTest struct {
	MsgSink         pb.Messenger_ListenClient
	DataTransmitted int // Number of bytes transmitted
	Duration        int // Number of milliseconds it took to transmit the data
	Bandwidth       int // Convenience variable: DataTransmitted / Duration (resulting int kB/s)
}

// Implementation of the gRPC server that listens for connections from other peers.
type messengerServer struct {
	// pb.UnimplementedMessengerServer
}

// Implementation of the gRPC Listen service (multi-request-single-response).
// Receives messages from the gRPC client running on the other node and dispatches them to the appropriate module by
// calling the corresponding handler function. The handler functions must have been set by the modules before the
// server started.
func (ms *messengerServer) Listen(srv pb.Messenger_ListenServer) error {

	// Log address of incoming connection.
	p, ok := peer.FromContext(srv.Context())
	if ok {
		logger.Info().Str("addr", p.Addr.String()).Msg("Incoming connection for protocol messages.")
	} else {
		logger.Error().Msg("Failed to get gRPC peer info from context.")
	}

	// Declare loop variables outside, since the err is checked also after the loop finishes.
	var err error
	var msg *pb.ProtocolMessage

	// For each message received from the peer
	finished := false
	for msg, err = srv.Recv(); !finished && err == nil; msg, err = srv.Recv() {
		checkForHotStuffProposal(msg, "Received HotStuffProposal.")
		finished = handleMessage(msg, srv)
	}

	// Log error message produced on termination of the above loop.
	// TODO: Implement graceful shutdown.
	if err != nil {
		logger.Info().Err(err).Str("address", p.Addr.String()).Msg("Connection terminated.")
	}

	return nil
}

// Dispatch message to the appropriate module by calling the corresponding handler.
// Unpack message batches and dispatch each message separately.
func handleMessage(msg *pb.ProtocolMessage, srv pb.Messenger_ListenServer) (finished bool) {

	// WARNING: If a simulate crash, the peer ignores all messages
	if Crashed {
		return
	}

	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_Multi:
		for _, item := range m.Multi.Msgs {
			handleMessage(item, srv)
		}
	case *pb.ProtocolMessage_Checkpoint:
		//logger.Trace().Int32("from", msg.SenderId).Msg("Received protocol message: Checkpoint.")
		CheckpointMsgHandler(m.Checkpoint, msg.SenderId)
	case *pb.ProtocolMessage_MissingEntryReq:
		StateTransferMsgHandler(msg)
	case *pb.ProtocolMessage_MissingEntry:
		StateTransferMsgHandler(msg)
	case *pb.ProtocolMessage_BandwidthTest:
		logger.Debug().Int32("peerId", msg.SenderId).Int32("sn", msg.Sn).Int("payloadSize", len(m.BandwidthTest.Payload)).Msg("Received bandwidth test message.")
		// Only acknowledge messages with sequence number 0.
		if msg.Sn == 0 {
			if err := srv.Send(&pb.BandwidthTestAck{}); err != nil {
				logger.Error().Err(err).Int32("peerId", msg.SenderId).Msg("Failed to acknowledge bandwidth test message.")
			}
		}
	case *pb.ProtocolMessage_Close:
		logger.Info().Int32("peerId", msg.SenderId).Msg("Connection closed by gRPC client.")
		return true
	// If the message is not a checkpoint message, it is an Orderer message.
	default:
		//logger.Trace().Int32("from", msg.SenderId).Msg("Received protocol message: Orderer.")
		OrdererMsgHandler(msg)
	}
	return false
}

// Starts the messenger by instantiating a gRPC server that listens to connections from other nodes.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func Start(wg *sync.WaitGroup) {
	defer wg.Done()

	// Get port number to listen on.
	port := membership.NodeIdentity(membership.OwnID).Port

	logger.Info().
		Bool("useTLS", config.Config.UseTLS).
		Int32("port", port).
		Msg("Listening for connections.")

	// Depending on configuration, create a plain or TLS-enabled gRPC server
	srvOptions := make([]grpc.ServerOption, 0)
	srvOptions = append(srvOptions, grpc.MaxRecvMsgSize(maxMessageSize))
	srvOptions = append(srvOptions, grpc.MaxSendMsgSize(maxMessageSize))
	if config.Config.UseTLS {
		tlsConfig := ConfigureTLS(config.Config.CertFile, config.Config.KeyFile)
		srvOptions = append(srvOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	grpcServer := grpc.NewServer(srvOptions...)

	// Assign messenger logic to the gRPC server.
	pb.RegisterMessengerServer(grpcServer, &messengerServer{})

	// Start listening on the network
	conn, err := net.Listen("tcp", ":"+strconv.Itoa(int(port)))
	if err != nil {
		logger.Fatal().Int32("port", port).Err(err).Msg("Failed to listen for connections.")
	}

	// Start the gRPC server (this call should block as long as the server is running).
	err = grpcServer.Serve(conn)
	if err != nil {
		logger.Fatal().Msg("Failed to start messenger server.")
	}

	// TODO: Implement graceful shutdown.
}

// Establishes (in parallel) network connections to all peers in the system.
// TODO: Deal with errors, e.g. when the connection times out (make sure the RPC call in connectToPeer() has a timeout).
func Connect() {

	// Get list of all peer IDs (this is a copy of the list maintained by the membership package, so we can modify it.)
	allNodeIds := membership.AllNodeIDs()

	// Find index of own ID.
	var idx int
	var id int32
	for idx, id = range allNodeIds {
		if id == membership.OwnID {
			break
		}
	}

	// Rotates slice so it starts with the own ID.
	allNodeIds = append(allNodeIds[idx:], allNodeIds[:idx]...)

	// Start goroutine collecting the established connections.
	connChan := make(chan struct {
		nodeID int32
		conn   PeerConnection
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < len(allNodeIds); i++ {
			c := <-connChan
			peerConnections[c.nodeID] = c.conn
		}
		wg.Done()
	}()

	// Connect to all nodes.
	for _, id := range allNodeIds {

		// If connections are being tested, open them one by one.
		if config.Config.TestConnections {
			connChan <- struct {
				nodeID int32
				conn   PeerConnection
			}{nodeID: id, conn: connectToPeer(id)}
			logger.Debug().Int32("peerID", id).Msg("Connected.")
			// If connection tests are not performed, connect in parallel.
		} else {
			go func(nodeID int32) {
				connChan <- struct {
					nodeID int32
					conn   PeerConnection
				}{nodeID: nodeID, conn: connectToPeer(nodeID)}
				logger.Debug().Int32("peerID", nodeID).Msg("Connected.")
			}(id)
		}
	}

	wg.Wait()
}

// Enqueues a message for sending to a node.
// Messages are passed by reference, so no modification of a msg must occur after enqueuing.
// Must not be called concurrently with Connect() to avoid concurrent access to peerConnections map.
// Unless outgoing messages are buffered (see OutMessageBufSize in the config file), must not be called
// concurrently with the same destNodeID.
// If outgoing messages are not buffered and no priority is used (see PriorityConnection in config file),
// also must not be called concurrently with EnqueuePriorityMessage with the same destNodeID.
func EnqueueMsg(msg *pb.ProtocolMessage, destNodeID int32) {

	// WARNING: If a simulate crash, the peer ignores all messages
	if Crashed {
		return
	}

	if peerConnections[destNodeID] == nil {
		logger.Error().Int32("nodeID", destNodeID).Msg("Cannot enqueue message. Node not connected.")
	} else {
		peerConnections[destNodeID].Send(msg)
	}
}

// Enqueues a priority message for sending to a node.
// Messages are passed by reference, so no modification of a msg must occur after enqueuing.
// Must not be called concurrently with Connect() to avoid concurrent access to peerConnections map.
// Unless outgoing messages are buffered (see OutMessageBufSize in the config file), must not be called
// concurrently with the same destNodeID.
// If outgoing messages are not buffered and no priority is used (see PriorityConnection in config file),
// also must not be called concurrently with EnqueueMessage with the same destNodeID.
func EnqueuePriorityMsg(msg *pb.ProtocolMessage, destNodeID int32) {

	// WARNING: If a simulate crash, the peer ignores all messages
	if Crashed {
		return
	}

	if peerConnections[destNodeID] == nil {
		logger.Error().Int32("nodeID", destNodeID).Msg("Cannot enqueue message. Node not connected.")
	} else {
		peerConnections[destNodeID].SendPriority(msg)
	}
}

// Connects to a single peer and returns a PeerConnection, through which messages destined to this node can be sent.
// Depending on the ConnectionMultiplier configuration parameter, creates multiple parallel underlying gRPC
// connections, to which messages (written to the returned channel) will be sent in a round-robin way.
// If PriorityConnection in the configuration is set to true, the PeerConnection will use a separate underlying
// network connection for messages sent through PeerConnection.SendPriority(). Otherwise, normal messages and
// priority messages will be sent through the same network connection(s).
// If OutMessageBufSize is configured > 0, all messages submitted to the returned PeerConnection will be first placed
// in a buffered channel and a separate background thread will send them on the network.
func connectToPeer(nodeID int32) PeerConnection {
	// Get network address of peer to which we connect.
	// We use the private address of the other peer.
	// The peers communicate through their private addresses,
	// while the clients connect to them using the peers' public addresses.
	identity := membership.NodeIdentity(nodeID)
	addrString := fmt.Sprintf("%s:%d", identity.PrivateAddr, identity.Port)

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

	logger.Info().
		Str("addr", addrString).
		Int("basicConns", config.Config.BasicConnections).
		Int("priorityConns", config.Config.PriorityConnections).
		Msg("Connecting to peer.")

	// Create new network connections to peer.
	var basicMsgSinks, priorityMsgSinks []pb.Messenger_ListenClient
	if config.Config.TestConnections {
		basicMsgSinks, priorityMsgSinks = createTestedConnections(addrString, dialOpts, nodeID)
	} else {
		basicMsgSinks, priorityMsgSinks = createConnections(addrString, dialOpts, nodeID)
	}

	// Create a new peer connection (wrapped around the network connection).
	var connection PeerConnection
	connection = NewBufferedMultiConnection(basicMsgSinks, priorityMsgSinks, config.Config.OutMessageBufSize)

	// If configured, add batching to the connection
	// Note that the batched connection includes "infinite" buffering for the message batches.
	// Adding extra message buffering to a batched connection is not meaningful, as it only adds
	// an additional thread shoveling messages from the extra buffer to the batch buffer.
	if config.Config.OutMessageBatchPeriod > 0 {
		return NewBatchedConnection(connection, time.Duration(config.Config.OutMessageBatchPeriod)*time.Millisecond)
		// Otherwise, return base connection directly.
	} else {
		logger.Info().Int32("peerId", nodeID).Msg("Returning unbuffered connection to peer.")
		return connection
	}
}

func createTestedConnections(addrString string, dialOpts []grpc.DialOption, nodeID int32) ([]pb.Messenger_ListenClient, []pb.Messenger_ListenClient) {
	// Initialize list of gRPC message sinks
	numConnections := config.Config.BasicConnections + config.Config.PriorityConnections + config.Config.ExcessConnections
	msgSinks := make([]pb.Messenger_ListenClient, numConnections)

	// Create multiple connections (between the same two peers) in parallel.
	connChan := make(chan pb.Messenger_ListenClient)
	for i := 0; i < numConnections; i++ {
		go func() {
			msgSink := createConnection(addrString, dialOpts)
			if msgSink == nil {
				logger.Error().Int("connIdx", i).Msg("Could not connect to peer.")
			}
			connChan <- msgSink
		}()
	}

	// Collect created connections
	for i := 0; i < numConnections; i++ {
		msgSinks[i] = <-connChan
	}

	// Test connections in parallel
	tests := testConnectionsParallel(msgSinks)
	// Print measured values.
	for _, t := range tests {
		logger.Info().Int32("peerId", nodeID).Int("bandwidth", t.Bandwidth).Int("duration", t.Duration).Msg("Parallel connection bandwidth.")
	}

	// Test connections sequentially
	tests = testConnections(msgSinks)
	// Print measured values.
	for _, t := range tests {
		logger.Info().Int32("peerId", nodeID).Int("bandwidth", t.Bandwidth).Int("duration", t.Duration).Msg("Connection bandwidth.")
	}

	// Sort connection test by DECREASING bandwidth
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Bandwidth > tests[j].Bandwidth
	})

	// Set priority connections to be the ones with the highest bandwidth, if configured.
	priority := tests[:config.Config.PriorityConnections]
	basic := tests[len(priority) : len(priority)+config.Config.BasicConnections]
	excess := tests[len(priority)+len(basic):]

	// Close excess connections
	for _, t := range excess {
		err := t.MsgSink.Send(&pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       0,
			Msg:      &pb.ProtocolMessage_Close{Close: &pb.CloseConnection{}},
		})
		if err != nil {
			logger.Error().Err(err).Int32("peerId", nodeID).Int("bandwidth", t.Bandwidth).Msg("Failed to close slow excess connection.")
		}
	}

	// Get priority message sinks
	priorityMsgSinks := make([]pb.Messenger_ListenClient, len(priority), len(priority))
	for i, t := range priority {
		priorityMsgSinks[i] = t.MsgSink
	}

	// Get basic message sinks
	basicMsgSinks := make([]pb.Messenger_ListenClient, len(basic), len(basic))
	for i, t := range basic {
		basicMsgSinks[i] = t.MsgSink
	}

	usedSinks := make([]pb.Messenger_ListenClient, 0)
	usedSinks = append(usedSinks, priorityMsgSinks...)
	usedSinks = append(usedSinks, basicMsgSinks...)

	totalBandwidth := approximateTotalBandwidth(tests)
	tracing.MainTrace.Event(tracing.BANDWIDTH, int64(nodeID), int64(totalBandwidth))
	logger.Info().
		Int("totalBw", totalBandwidth).
		Int("numConns", len(usedSinks)).
		Int32("peerId", nodeID).
		Msg("Measured approximate total bandwidth.")

	return basicMsgSinks, priorityMsgSinks
}

func createConnections(addrString string, dialOpts []grpc.DialOption, nodeID int32) ([]pb.Messenger_ListenClient, []pb.Messenger_ListenClient) {

	numConnections := config.Config.BasicConnections + config.Config.PriorityConnections
	connChan := make(chan pb.Messenger_ListenClient)
	// Create multiple connections (between the same two peers) in parallel.
	for i := 0; i < numConnections; i++ {
		go func() {
			msgSink := createConnection(addrString, dialOpts)
			if msgSink == nil {
				logger.Error().Int("connIdx", i).Msg("Could not connect to peer.")
			}
			connChan <- msgSink
		}()
	}

	// Get priority message sinks
	priorityMsgSinks := make([]pb.Messenger_ListenClient, config.Config.PriorityConnections)
	for i := 0; i < config.Config.PriorityConnections; i++ {
		priorityMsgSinks[i] = <-connChan
	}

	// Get basic message sinks
	basicMsgSinks := make([]pb.Messenger_ListenClient, config.Config.BasicConnections)
	for i := 0; i < config.Config.BasicConnections; i++ {
		basicMsgSinks[i] = <-connChan
	}

	return basicMsgSinks, priorityMsgSinks
}

// Creates a single connection to a peer at address addr, using options provided as dialOpts.
// Returns a new Protobuf client stub for sending messages on this connection.
func createConnection(addr string, dialOpts []grpc.DialOption) pb.Messenger_ListenClient {

	// Set up a gRPC connection.
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		logger.Error().Err(err).Str("addrStr", addr).Msg("Could not connect to node.")
		return nil
	}

	// Register client stub.
	client := pb.NewMessengerClient(conn)

	// Remotely invoke the Listen function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.Listen(context.Background())
	if err != nil {
		logger.Error().Err(err).Str("addrStr", addr).Msg("Could not invoke Listen RPC.")
		conn.Close()
		return nil
	}

	// Return the message sing connected to the peer.
	return msgSink
}

func testConnections(clients []pb.Messenger_ListenClient) []*connectionTest {

	// Initialize bandwidths slice
	tests := make([]*connectionTest, len(clients), len(clients))

	// Obtain bandwidth values by testing each connection
	for i, client := range clients {
		tests[i] = testConnection(client)
	}

	return tests
}

func testConnectionsParallel(clients []pb.Messenger_ListenClient) []*connectionTest {

	resultChan := make(chan *connectionTest)

	// Obtain bandwidth values by testing each connection in parallel
	for i, cl := range clients {
		go func(idx int, client pb.Messenger_ListenClient) {
			resultChan <- testConnection(client)
		}(i, cl)
	}

	// Collect test results
	tests := make([]*connectionTest, len(clients), len(clients))
	for i := 0; i < len(clients); i++ {
		tests[i] = <-resultChan
	}

	return tests

}

func approximateTotalBandwidth(tests []*connectionTest) int {

	// Get minimal test duration
	minTime := -1
	for _, test := range tests {
		if minTime == -1 || test.Duration < minTime {
			minTime = test.Duration
		}
	}

	// Avoid division by zero.
	if minTime == 0 {
		return 0
	}

	// Only consider the time during which all connections were transmitting data
	// This is not optimal, but hopefully a fair enough approximation.
	// The not so realistic assumption here is that each connection sends data at a uniform rate,
	// Regardless of other parallel connections sending data or not.
	totalData := 0
	for _, t := range tests {
		totalData += t.DataTransmitted * minTime / t.Duration
		logger.Info().
			Int("data", t.DataTransmitted).
			Int("duration", t.Duration).
			Int("bw", t.Bandwidth).
			Int("minTime", minTime).
			Int("adjustedData", t.DataTransmitted*minTime/t.Duration).
			Msg("Approximating total bandwidth.")
	}
	return totalData / minTime
}

func testConnection(client pb.Messenger_ListenClient) *connectionTest {
	start := time.Now()

	// Send predefined number of messages on the link
	// All messages have sequence numbers greater than 0, except for the last one.
	// The server on the other side only acknowledges bandwidth test messages with sequence number 0.
	// This implementation assumes that the transport guarantees in-order delivery.
	for i := config.Config.ConnectionTestMsgs - 1; i >= 0; i-- {
		err := client.Send(&pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       int32(i),
			Msg: &pb.ProtocolMessage_BandwidthTest{BandwidthTest: &pb.BandwidthTest{
				Payload: make([]byte, config.Config.ConnectionTestPayload, config.Config.ConnectionTestPayload),
			}},
		})
		if err != nil {
			logger.Error().Err(err).Msg("Failed sending bandwidth test message to peer.")
		}
	}

	// Wait for acknowledgment of the reception of all messages and return the computed bandwidth.
	if _, err := client.Recv(); err == nil {
		dataTransmitted := config.Config.ConnectionTestMsgs * config.Config.ConnectionTestPayload
		duration := int(time.Since(start).Nanoseconds())
		bandwidth := 0 // in kB/s
		if duration != 0 {
			bandwidth = dataTransmitted * 1000000 / duration
		}
		return &connectionTest{
			MsgSink:         client,
			DataTransmitted: dataTransmitted,
			Duration:        duration / 1000000, // in ms
			Bandwidth:       bandwidth,
		}
	} else {
		logger.Error().Err(err).Msg("Failed receiving bandwidth test ack.")
		return nil
	}
}
