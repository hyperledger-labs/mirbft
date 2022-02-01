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

package discovery

import (
	"net"
	"sync"

	logger "github.com/rs/zerolog/log"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"google.golang.org/grpc"
)

// Represents a slave as seen by the master (server).
type slave struct {
	SlaveID      int32
	Status       int32                  // Status reported by the slave in a NextCommand request. -1 before the first request.
	CommandQueue chan *pb.MasterCommand // Commands to be sent to the slave.
	Tag          string                 // Tag inherent to the slave (specified by the user when starting the slave, not assigned by master).
	// Tags are used to differentiate slaves. Commands can be sent to slaves with a specific tag.
}

// Represents a discovery server and saves all its state.
type DiscoveryServer struct {
	// pb.UnimplementedDiscoveryServer

	// Fields related to peer discovery.
	peers                sync.Map           // Peer identities are added here as peers register.
	peerIdentities       []*pb.NodeIdentity // Used to include in responses to requests. Populated when all peers call RegisterPeer.
	TBLSPublicKey        []byte             // Public key of the BLS threshold cryptosystem
	TBLSPrivateKeyShares [][]byte           // Privte key shares of the BLS threshold cryptosystem
	peerWg               sync.WaitGroup     // Used to wait for all peers to be ready to receive the membership list.
	syncWg               sync.WaitGroup     // Used to wait for all peers to connect to each other.
	doOnce               sync.Once          // Used to generate response that is sent to all peers.
	keyGenOnce           sync.Once          // Used to generate the keys for the BLS threshold cryptosystem once, to be included in the response sent to all peers.

	// Fields related to master and slaves.
	slaves sync.Map // Maps slave IDs to slaves. Used as map[int32]*slave

	// Channel with peer IDs that will be distributed to peers as they are being discovered.
	// We use a channel instead of a simple counter variable, as IDs may be issued concurrently.
	// The channel is not buffered and a background thread generates peerIDs on demand (see DistributeIDs()).
	peerIDs   chan int32
	clientIDs chan int32 // Same as peerIDs, but for clients.
	slaveIDs  chan int32 // Same as peerIDs, but for slaves.
	cmdIDs    chan int32 // Same as peerIDs, but for master commands.

	// Next ID to be pushed into its respective channel.
	nextPeerID   int32
	nextClientID int32
	nextSlaveID  int32
	nextCmdID    int32

	// When a value is pushed to this channel, the peerIDs and clientIDs are reset to 0.
	idResetChan chan struct{}

	// Channel indicating whether the ID distributor thread should stop.
	// Pushing a value to this channel stops the ID distributor thread.
	stopIDDistributor chan bool

	// When issuing a master command, this variable can be set to pointer to an actual WaitGroup.
	// What WG will be initialized to the number of slaves to which the command is sent.
	// When a slave executes that command and asks for the next one (referencing the executed commands ID in the
	// status message), this WG is decremented.
	// Used to make the master wait until slaves execute a command.
	responseWG *sync.WaitGroup

	// If responseWG is not nil, this must hold the ID of the command the execution of which the master is waiting for.
	waitingForCmd int32

	// If the master is waiting for a command to finish,
	// this variable stores the maximum exit status of all the responses to that command.
	// This is used with "exec-wait" to detect if a timeout occurred.
	maxCommandExitStatus int32
}

// Creates and initializes a new instance of a discovery server.
func NewDiscoveryServer() *DiscoveryServer {
	ds := DiscoveryServer{}

	// Initialize ID distributor channels (need to be unbuffered).
	ds.peerIDs = make(chan int32)
	ds.clientIDs = make(chan int32)
	ds.slaveIDs = make(chan int32)
	ds.cmdIDs = make(chan int32)
	ds.idResetChan = make(chan struct{})

	// Initially not waiting for any command.
	ds.waitingForCmd = -1
	ds.maxCommandExitStatus = 0

	// NOTE: The peerIdentities list is allocated in collectPeerIdentities.

	return &ds
}

// Runs the discovery server.
// Meant to be run as a separate goroutine.
// Calls Done() on the wg argument when finished. (Used for synchronization)
func RunDiscoveryServer(port string, grpcServer *grpc.Server, discoveryImpl *DiscoveryServer, wg *sync.WaitGroup) {

	// Start ID distributor thread.
	go discoveryImpl.DistributeIDs()
	defer discoveryImpl.StopIDDistribution()

	// Start listening on the network.
	conn, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to listen on port " + port)
	}

	// Register the discovery server implementation with the GRPC server
	pb.RegisterDiscoveryServer(grpcServer, discoveryImpl)

	// Run GRPC server.
	logger.Info().Str("port", port).Msg("Starting server.")
	err = grpcServer.Serve(conn)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to run discovery server.")
	}

	logger.Info().Msg("Discovery server stopped.")
	wg.Done()
}

// Background goroutine generating IDs on demand.
func (ds *DiscoveryServer) DistributeIDs() {

loop:
	for {
		select {
		case ds.peerIDs <- ds.nextPeerID:
			ds.nextPeerID++
		case ds.clientIDs <- ds.nextClientID:
			ds.nextClientID++
		case ds.slaveIDs <- ds.nextSlaveID:
			ds.nextSlaveID++
		case ds.cmdIDs <- ds.nextCmdID:
			ds.nextCmdID++
		case <-ds.idResetChan:
			ds.nextPeerID = 0
			ds.nextClientID = 0
		case <-ds.stopIDDistributor:
			break loop
		}
	}
}

func (ds *DiscoveryServer) StopIDDistribution() {
	ds.stopIDDistributor <- true
}

// Resets part of the server's state that deals with Peers and Clients.
// In particular:
// - ID distributor for peers
// - ID distributor for clients
// - WaitGroups used for synchronizing peers at startup.
// The numPeers parameter signifies the number of peers to wait for after the reset.
// The WaitGroup in initialized to numPeers.
// No ID assignment must be concurrent with resetPC().
func (ds *DiscoveryServer) resetPC(numPeers int) {

	// Reset peer and client ID counters.
	ds.idResetChan <- struct{}{}

	// Reset data structures holding peer information, as well as synchronization objects used for replying
	ds.peers = sync.Map{} // TODO: Technically this may cause a race condition with accessing the map. Fix it!
	ds.peerIdentities = nil
	ds.peerWg = sync.WaitGroup{}
	ds.peerWg.Add(numPeers)
	ds.syncWg = sync.WaitGroup{}
	ds.syncWg.Add(numPeers)
	ds.doOnce = sync.Once{}
	ds.keyGenOnce = sync.Once{}
}
