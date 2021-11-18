/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deploytest

import (
	"context"
	"crypto"
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	mirCrypto "github.com/hyperledger-labs/mirbft/pkg/crypto"
	"github.com/hyperledger-labs/mirbft/pkg/dummyclient"
	"github.com/hyperledger-labs/mirbft/pkg/grpctransport"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
)

const (
	// BaseListenPort defines the starting port number on which test replicas will be listening
	// in case the test is being run with the "grpc" setting for networking.
	// A node with numeric ID id will listen on port (BaseListenPort + id)
	BaseListenPort = 10000

	// RequestListenPort is the port number on which nodes' RequestReceivers listen for incoming requests.
	RequestListenPort = 20000
)

// TestConfig contains the parameters of the deployment to be tested.
type TestConfig struct {

	// Number of replicas in the tested deployment.
	NumReplicas int

	// Number of clients in the tested deployment.
	NumClients int

	// Type of networking to use.
	// Current possible values: "fake", "grpc"
	Transport string

	// The number of requests each client submits during the execution of the deployment.
	NumFakeRequests int

	// The number of requests sent over the network (by a single DummyClient)
	NumNetRequests int

	// Directory where all the test-related files will be stored.
	// If empty, an OS-default temporary directory will be used.
	Directory string

	// Duration after which the test deployment will be asked to shut down.
	Duration time.Duration
}

// The Deployment represents a list of replicas interconnected by a simulated network transport.
type Deployment struct {
	testConfig *TestConfig

	// The fake transport layer is only used if the deployment is configured to use it
	// by setting testConfig.Net to "fake".
	// Otherwise, the fake transport might be created, but will not be used.
	FakeTransport *FakeTransport

	// The replicas of the deployment.
	TestReplicas []*TestReplica

	// Dummy clients to submit requests to replicas over the (local loopback) network.
	Clients []*dummyclient.DummyClient
}

// NewDeployment returns a Deployment initialized according to the passed configuration.
func NewDeployment(testConfig *TestConfig) (*Deployment, error) {

	// Use a common logger for all clients and replicas.
	logger := logging.Synchronize(logging.ConsoleDebugLogger)

	// Create a simulated network transport to route messages between replicas.
	fakeTransport := NewFakeTransport(testConfig.NumReplicas)

	// Create a dummy static membership with replica IDs from 0 to len(replicas) - 1
	membership := make([]t.NodeID, testConfig.NumReplicas)
	for i := 0; i < len(membership); i++ {
		membership[i] = t.NodeID(i)
	}

	// Compute a list of all client IDs.
	// It consists of all dummy client IDs, plus the "fake" client associated with replicas submitting requests directly
	clientIDs := []t.ClientID{0} // "Fake" client has always ID 0, others start from 1.
	for i := 1; i <= testConfig.NumClients; i++ {
		clientIDs = append(clientIDs, t.ClientID(i))
	}

	// Create all TestReplicas for this deployment.
	replicas := make([]*TestReplica, testConfig.NumReplicas)
	for i := range replicas {

		// Configure the test replica's node.
		config := &mirbft.NodeConfig{
			Logger: logging.Decorate(logger, fmt.Sprintf("Node %d: ", i)),
		}

		// Create network transport module
		var transport modules.Net
		switch testConfig.Transport {
		case "fake":
			transport = fakeTransport.Link(t.NodeID(i))
		case "grpc":
			transport = localGrpcTransport(membership, t.NodeID(i))
		}

		// Create instance of test replica.
		replicas[i] = &TestReplica{
			Id:              t.NodeID(i),
			Config:          config,
			Membership:      membership,
			ClientIDs:       clientIDs,
			Dir:             filepath.Join(testConfig.Directory, fmt.Sprintf("node%d", i)),
			App:             &FakeApp{},
			Net:             transport,
			NumFakeRequests: testConfig.NumFakeRequests,
		}
	}

	// Create dummy clients.
	netClients := make([]*dummyclient.DummyClient, 0)
	for i := 1; i <= testConfig.NumClients; i++ {
		// The loop counter i is used as client ID.
		// We start counting at 1 (and not 0), since client ID 0 is reserved
		// for the "fake" requests submitted directly by the TestReplicas.

		// Create client-specific Crypto module
		cryptoModule, err := mirCrypto.ClientPseudo(membership, clientIDs, t.ClientID(i), mirCrypto.DefaultPseudoSeed)
		if err != nil {
			return nil, err
		}

		// Create new DummyClient
		netClients = append(netClients, dummyclient.NewDummyClient(
			t.ClientID(i),
			crypto.SHA256,
			//			&mirCrypto.DummyCrypto{DummySig: []byte{0}},
			cryptoModule,
			logger,
		))
	}

	return &Deployment{
		testConfig:    testConfig,
		FakeTransport: fakeTransport,
		TestReplicas:  replicas,
		Clients:       netClients,
	}, nil
}

// Run launches the test deployment.
// It starts all test replicas, the dummy client, and the fake message transport subsystem,
// waits until the replicas stop, and returns the final statuses of all the replicas.
// Closing the passed stopC channel makes the deployment terminate by signaling all those subsystems to stop gracefully.
func (d *Deployment) Run(tickInterval time.Duration, stopC <-chan struct{}) []NodeStatus {

	// Initialize helper variables.
	finalStatuses := make([]NodeStatus, len(d.TestReplicas))
	var wg sync.WaitGroup

	clientConnectionCtx, cancelClientConnections := context.WithCancel(context.Background())

	// Start the Mir nodes.
	for i, testReplica := range d.TestReplicas {

		// Start the replica in a separate goroutine.
		wg.Add(1)
		go func(i int, testReplica *TestReplica) {
			defer GinkgoRecover()
			defer wg.Done()

			fmt.Printf("Node %d: running\n", i)
			finalStatuses[i] = testReplica.Run(tickInterval, stopC)
			fmt.Printf("Node %d: exit with exitErr=%v\n", i, finalStatuses[i].ExitErr)
			cancelClientConnections()
		}(i, testReplica)
	}

	// Start the message transport subsystem
	d.FakeTransport.Start()
	defer d.FakeTransport.Stop()

	// Connect the deployment's DummyClients to all replicas and have them submit their requests in separate goroutines.
	for _, client := range d.Clients {
		client.Connect(clientConnectionCtx, d.localRequestReceiverAddrs())
		go func(c *dummyclient.DummyClient) {
			GinkgoRecover()
			submitDummyRequests(c, d.testConfig.NumNetRequests)
		}(client)
	}

	// Wait until the deployment receives the stop signal (stopC closed)
	<-stopC

	// Disconnect DummyClients
	cancelClientConnections()
	for _, client := range d.Clients {
		client.Disconnect()
	}

	// Wait for all replicas to terminate
	wg.Wait()

	fmt.Printf("All go routines shut down\n")
	return finalStatuses
}

// Creates an instance of GrpcTransport based on the numeric IDs of test replicas.
// The network address of each test replica is the loopback 127.0.0.1
func localGrpcTransport(nodeIds []t.NodeID, ownId t.NodeID) *grpctransport.GrpcTransport {

	// Compute network addresses and ports for all test replicas.
	// Each test replica is on the local machine - 127.0.0.1
	membership := make(map[t.NodeID]string, len(nodeIds))
	for _, id := range nodeIds {
		membership[id] = fmt.Sprintf("127.0.0.1:%d", BaseListenPort+id)
	}

	return grpctransport.NewGrpcTransport(membership, ownId, nil)
}

func (d *Deployment) localRequestReceiverAddrs() map[t.NodeID]string {

	// Compute network addresses and ports for the RequestReceivers at all replicas.
	// Each test replica is on the local machine - 127.0.0.1
	addrs := make(map[t.NodeID]string, len(d.TestReplicas))
	for _, tr := range d.TestReplicas {
		addrs[tr.Id] = fmt.Sprintf("127.0.0.1:%d", RequestListenPort+tr.Id)
	}

	return addrs
}

func submitDummyRequests(client *dummyclient.DummyClient, n int) {
	for i := 0; i < n; i++ {
		if err := client.SubmitRequest([]byte(fmt.Sprintf("Request %d", i))); err != nil {
			panic(err)
		}
	}
}
