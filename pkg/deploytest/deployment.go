/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package deploytest

import (
	"context"
	"crypto"
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/logger"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/reqstore"
	"github.com/hyperledger-labs/mirbft/pkg/simplewal"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (

	// TestMsgBufSize is the number of bytes each test replica uses for the buffer for backlogging messages.
	TestMsgBufSize = 5 * 1024 * 1024 // 5 MB
)

// TestConfig contains the parameters of the deployment to be tested.
type TestConfig struct {

	// Number of replicas in the tested deployment.
	NumReplicas int

	// Number of clients in the tested deployment.
	NumClients int

	// The width of the client watermark window.
	ClientWMWidth int

	// The number of requests each client submits during the execution of the deployment.
	NumRequests int
}

// The Deployment represents a list of replicas interconnected by a simulated network transport.
type Deployment struct {
	Transport    *FakeTransport
	TestReplicas []*TestReplica
}

// NewDeployment returns a Deployment initialized according to the passed configuration.
// Closing the passed doneC channel makes the deployment terminate.
func NewDeployment(testConfig TestConfig, doneC <-chan struct{}) (*Deployment, error) {

	// Create a temporary directory in the OS-default location.
	// This directory will contain the files generated by the deployment.
	tmpDir, err := ioutil.TempDir("", "mirbft-deployment-test.*")
	if err != nil {
		return nil, err
	}

	// Create a simulated network transport to route messages between replicas.
	transport := NewFakeTransport(testConfig.NumReplicas)

	// Create all TestReplicas for this deployment.
	replicas := make([]*TestReplica, testConfig.NumReplicas)
	for i := range replicas {

		// Configure the test replica's node.
		config := &mirbft.NodeConfig{
			BufferSize: TestMsgBufSize,
			Logger:     logger.ConsoleWarnLogger,
		}

		// Create instance of test replica.
		replicas[i] = &TestReplica{
			ID:              uint64(i),
			Config:          config,
			TmpDir:          filepath.Join(tmpDir, fmt.Sprintf("node%d", i)),
			App:             &FakeApp{},
			FakeTransport:   transport,
			DoneC:           doneC,
			NumFakeRequests: testConfig.NumRequests,
		}
	}

	return &Deployment{
		Transport:    transport,
		TestReplicas: replicas,
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
	TmpDir string

	// Configuration of the node corresponding to this replica.
	Config *mirbft.NodeConfig

	// Simulated network transport subsystem.
	FakeTransport *FakeTransport

	// Number of simulated requests inserted in the test replica by a hypothetical client.
	NumFakeRequests int
}

// EventLogFile returns the name of the file where the replica's event log is stored.
func (tr *TestReplica) EventLogFile() string {
	return filepath.Join(tr.TmpDir, "eventlog.gz")
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

	// Initialize the request store.
	reqStorePath := filepath.Join(tr.TmpDir, "reqstore")
	err := os.MkdirAll(reqStorePath, 0700)
	Expect(err).NotTo(HaveOccurred())
	reqStore, err := reqstore.Open(reqStorePath)
	Expect(err).NotTo(HaveOccurred())
	defer reqStore.Close()

	// Initialize the write-ahead log.
	walPath := filepath.Join(tr.TmpDir, "wal")
	err = os.MkdirAll(walPath, 0700)
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
			Net:          tr.FakeTransport.Link(tr.ID),
			Hasher:       crypto.SHA256,
			RequestStore: reqStore,
			App:          tr.App,
			WAL:          wal,
			Interceptor:  interceptor,
			StateMachine: NewDummySM(tr.Config.Logger),
			//StateMachine: &statemachine.StateMachine{
			//	Logger: mirbft.LogAdapter{Logger: tr.Config.Logger},
			//},
		},
	)
	Expect(err).NotTo(HaveOccurred())

	// Initialize WaitGroup for the replica's threads.
	var wg sync.WaitGroup
	defer wg.Wait()

	// Start thread inserting incoming messages in the replica's mirbft node.
	wg.Add(1)
	go func() {
		defer GinkgoRecover()
		defer wg.Done()
		recvC := tr.FakeTransport.RecvC(node.ID)
		for {
			select {
			case sourceMsg := <-recvC:
				node.Step(context.Background(), sourceMsg.Source, sourceMsg.Msg)
			case <-tr.DoneC:
				return
			}
		}
	}()

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

	// Run the node until it stops and obtain the node's final status.
	err = node.Run(tr.DoneC, ticker.C)

	finalStatus, statusErr := node.Status(context.Background())

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
			defer func() {
				fmt.Printf("Node %d: shutting down\n", i)
				if r := recover(); r != nil {
					fmt.Printf("  Node %d: received panic %s\n%s\n", i, r, debug.Stack())
					panic(r)
				}
			}()

			fmt.Printf("Node %d: running\n", i)
			nodeStatus.Status, nodeStatus.ExitErr, nodeStatus.StatusErr = testReplica.Run(tickInterval)
			fmt.Printf("Node %d: exit with exitErr=%v\n", i, nodeStatus.ExitErr)
		}(i, testReplica)
	}

	// Start the message transport subsystem
	d.Transport.Start()
	defer d.Transport.Stop()

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
