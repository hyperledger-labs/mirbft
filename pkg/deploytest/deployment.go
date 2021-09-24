/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deploytest

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/grpctransport"
	"github.com/hyperledger-labs/mirbft/pkg/logger"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/ordering"
	"github.com/hyperledger-labs/mirbft/pkg/simplewal"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"os"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (

	// TestMsgBufSize is the number of bytes each test replica uses for the buffer for backlogging messages.
	TestMsgBufSize = 5 * 1024 * 1024 // 5 MB

	// BaseListenPort defines the starting port number on which test replicas will be listening
	// in case the test is being run with the "grpc" setting for networking.
	// A node with numeric ID id will listen on port (BaseListenPort + id)
	BaseListenPort = 10000
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
	NumRequests int

	// Directory where all the test-related files will be stored.
	// If empty, an OS-default temporary directory will be used.
	Directory string

	// Duration after which the test deployment will be asked to shut down.
	Duration time.Duration
}

// The Deployment represents a list of replicas interconnected by a simulated network transport.
type Deployment struct {

	// The fake transport layer is only used if the deployment is configured to use it
	// by setting testConfig.Net to "fake".
	// Otherwise, the fake transport might be created, but will not be used.
	FakeTransport *FakeTransport

	// The replicas of the deployment.
	TestReplicas []*TestReplica
}

// NewDeployment returns a Deployment initialized according to the passed configuration.
// Closing the passed doneC channel makes the deployment terminate.
func NewDeployment(testConfig *TestConfig, doneC <-chan struct{}) (*Deployment, error) {

	// Create a simulated network transport to route messages between replicas.
	fakeTransport := NewFakeTransport(testConfig.NumReplicas)

	// Create a dummy static membership with replica IDs from 0 to len(replicas) - 1
	membership := make([]uint64, testConfig.NumReplicas)
	for i := 0; i < len(membership); i++ {
		membership[i] = uint64(i)
	}

	// Create all TestReplicas for this deployment.
	replicas := make([]*TestReplica, testConfig.NumReplicas)
	for i := range replicas {

		// Configure the test replica's node.
		config := &mirbft.NodeConfig{
			BufferSize: TestMsgBufSize,
			Logger:     logger.ConsoleDebugLogger,
		}

		// Create network transport module
		var transport modules.Net
		switch testConfig.Transport {
		case "fake":
			transport = fakeTransport.Link(uint64(i))
		case "grpc":
			transport = createLocalGrpcTransport(membership, uint64(i))
		}

		// Create instance of test replica.
		replicas[i] = &TestReplica{
			ID:              uint64(i),
			Config:          config,
			Membership:      membership,
			Dir:             filepath.Join(testConfig.Directory, fmt.Sprintf("node%d", i)),
			App:             &FakeApp{},
			Net:             transport,
			DoneC:           doneC,
			NumFakeRequests: testConfig.NumRequests,
		}
	}

	return &Deployment{
		FakeTransport: fakeTransport,
		TestReplicas:  replicas,
	}, nil
}

// TestReplica represents one replica (that uses one instance of the mirbft.Node) in the test system.
type TestReplica struct {

	// Closing this channel indicates to the replica to terminate gracefully.
	DoneC <-chan struct{}

	// ID of the replica as seen by the protocol.
	ID uint64

	// Dummy test application the replica is running.
	App *FakeApp

	// Name of the directory where the persisted state of this TestReplica will be stored,
	// along with the logs produced by running the replica.
	Dir string

	// Configuration of the node corresponding to this replica.
	Config *mirbft.NodeConfig

	// List of replica IDs constituting the (static) membership.
	Membership []uint64

	// Network transport subsystem.
	Net modules.Net

	// Number of simulated requests inserted in the test replica by a hypothetical client.
	NumFakeRequests int
}

// EventLogFile returns the name of the file where the replica's event log is stored.
func (tr *TestReplica) EventLogFile() string {
	return filepath.Join(tr.Dir, "eventlog.gz")
}

// Run initializes all the required modules and starts the test replica.
// The function blocks until the replica stops.
// Run returns, in this order
//   - The final status of the replica
//   - The error that made the node terminate
//   - The error that occurred while obtaining the final node status
func (tr *TestReplica) Run(tickInterval time.Duration) (*status.StateMachine, error, error) {

	// Create logical time for the test replica.
	// (Note that this is not just for testing - production deployment also only uses this form of time.)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	//// Initialize the request store.
	//reqStorePath := filepath.Join(tr.TmpDir, "reqstore")
	//err := os.MkdirAll(reqStorePath, 0700)
	//Expect(err).NotTo(HaveOccurred())
	//reqStore, err := reqstore.Open(reqStorePath)
	//Expect(err).NotTo(HaveOccurred())
	//defer reqStore.Close()

	// Initialize the write-ahead log.
	walPath := filepath.Join(tr.Dir, "wal")
	err := os.MkdirAll(walPath, 0700)
	Expect(err).NotTo(HaveOccurred())
	wal, err := simplewal.Open(walPath)
	Expect(err).NotTo(HaveOccurred())
	defer wal.Close()

	// Initialize recording of events.
	file, err := os.Create(tr.EventLogFile())
	Expect(err).NotTo(HaveOccurred())
	defer file.Close()
	interceptor := eventlog.NewRecorder(tr.ID, file)
	defer func() {
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
	}()

	// Create the mirbft node for this replica.
	node, err := mirbft.NewNode(
		tr.ID,
		tr.Config,
		&modules.Modules{
			Net:         tr.Net,
			App:         tr.App,
			WAL:         wal,
			Protocol:    ordering.NewDummyProtocol(tr.Config.Logger, tr.Membership, tr.ID),
			Interceptor: interceptor,
		},
	)
	Expect(err).NotTo(HaveOccurred())

	// Initialize WaitGroup for the replica's threads.
	var wg sync.WaitGroup

	// Start thread submitting requests from a (single) hypothetical client.
	// The client submits a predefined number of requests and then stops.
	// TODO: Make the number of clients configurable.
	wg.Add(1)
	go func() {
		defer GinkgoRecover()
		defer wg.Done()
		for i := 0; i < tr.NumFakeRequests; i++ {
			select {
			case <-tr.DoneC:
				// Stop submitting if shutting down.
				break
			default:
				// Otherwise, submit next request.
				if err := node.SubmitRequest(
					context.Background(),
					0,
					uint64(i),
					clientReq(0, uint64(i))); err != nil {

					// TODO (Jason), failing on err causes flakes in the teardown,
					// so just returning for now, we should address later
					break
				}
				// TODO: Add some configurable delay here
			}
		}
	}()

	// ATTENTION! This is hacky!
	// If the test replica used the GRPC transport, initialize the Net module.
	switch transport := tr.Net.(type) {
	case *grpctransport.GrpcTransport:
		err := transport.Start()
		Expect(err).NotTo(HaveOccurred())
		transport.Connect()
	}

	// Run the node until it stops and obtain the node's final status.
	err = node.Run(tr.DoneC, ticker.C)
	fmt.Println("Run returned!")

	finalStatus, statusErr := node.Status(context.Background())

	wg.Wait()

	// ATTENTION! This is hacky!
	// If the test replica used the GRPC transport, stop the Net module.
	switch transport := tr.Net.(type) {
	case *grpctransport.GrpcTransport:
		transport.Stop()
	}

	return finalStatus, err, statusErr
}

// NodeStatus represents the final status of a test replica.
type NodeStatus struct {

	// Status as returned by mirbft.Node.Status()
	Status *status.StateMachine

	// Potential error returned by mirbft.Node.Status() in case of obtaining of the status failed.
	StatusErr error

	// Reason the node terminated, as returned by mirbft.Node.Run()
	ExitErr error
}

// Run launches the test deployment.
// It starts all test replicas and the fake message transport subsystem, waits until the replicas
func (d *Deployment) Run(tickInterval time.Duration) []*NodeStatus {
	finalStatuses := make([]*NodeStatus, len(d.TestReplicas))
	var wg sync.WaitGroup

	// Start the Mir nodes
	for i, testReplica := range d.TestReplicas {

		// Create an object for the final node status of this replica.
		// Its values will be filled in later, when the replica terminates.
		nodeStatus := &NodeStatus{}
		finalStatuses[i] = nodeStatus

		// Start the replica in a separate goroutine.
		wg.Add(1)
		go func(i int, testReplica *TestReplica) {
			defer GinkgoRecover()
			defer wg.Done()

			fmt.Printf("Node %d: running\n", i)
			nodeStatus.Status, nodeStatus.ExitErr, nodeStatus.StatusErr = testReplica.Run(tickInterval)
			fmt.Printf("Node %d: exit with exitErr=%v\n", i, nodeStatus.ExitErr)
		}(i, testReplica)
	}

	// Start the message transport subsystem
	d.FakeTransport.Start()
	defer d.FakeTransport.Stop()

	// Wait for all replicas to terminate
	wg.Wait()

	fmt.Printf("All go routines shut down\n")
	return finalStatuses
}

// Assembles a byte slice representation of a dummy client request.
func clientReq(clientID, reqNo uint64) []byte {
	res := make([]byte, 16)
	binary.BigEndian.PutUint64(res, clientID)
	binary.BigEndian.PutUint64(res[8:], reqNo)
	return res
}

// Creates an instance of GrpcTransport based on the numeric IDs of test replicas.
// The network address of each test replica is the loopback 127.0.0.1
func createLocalGrpcTransport(nodeIds []uint64, ownId uint64) *grpctransport.GrpcTransport {

	// Compute network addresses and ports for all test replicas.
	// Each test replica is on the local machine - 127.0.0.1
	membership := make(map[uint64]string, len(nodeIds))
	for _, id := range nodeIds {
		membership[id] = fmt.Sprintf("127.0.0.1:%d", BaseListenPort+id)
	}

	return grpctransport.NewGrpcTransport(membership, ownId)
}
