/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/deploytest"
	"github.com/onsi/ginkgo/extensions/table"
	"io/ioutil"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	tickInterval = 50 * time.Millisecond
	testTimeout  = 10 * time.Second
)

// TODO: Update Jason's comment.
// StressyTest attempts to spin up as 'real' a network as possible, using
// fake links, but real concurrent go routines.  This means the test is non-deterministic
// so we can't make assertions about the state of the network that are as specific
// as the more general single threaded testengine type tests.  Still, there
// seems to be value in confirming that at a basic level, a concurrent network executes
// correctly.
var _ = Describe("Basic test", func() {

	var (
		currentTestConfig *deploytest.TestConfig

		// The deployment used by the test.
		deployment *deploytest.Deployment

		// Channel used to stop the deployment.
		stopC = make(chan struct{})

		// When the deployment stops, the final node statuses will be written here.
		finalStatuses []deploytest.NodeStatus

		// Map of all the directories accessed by the tests.
		// All of those will be deleted after the tests complete.
		// We are not deleting them on the fly, to make it possible for a test
		// to access a directory created by a previous test.
		tempDirs = make(map[string]struct{})
	)

	// Before each run, clear the test state variables.
	BeforeEach(func() {
		finalStatuses = nil
		stopC = make(chan struct{})
	})

	// Define what happens when the test runs.
	// Each run is parametrized with a TestConfig
	testFunc := func(testConfig *deploytest.TestConfig) {

		// Set current test config so it can be accessed after the test is complete.
		currentTestConfig = testConfig

		// Create a directory for the deployment-generated files
		// and s the test directory name, for later deletion.
		err := createDeploymentDir(testConfig)
		Expect(err).NotTo(HaveOccurred())
		tempDirs[testConfig.Directory] = struct{}{}

		// Create new test deployment.
		deployment, err = deploytest.NewDeployment(testConfig)
		Expect(err).NotTo(HaveOccurred())

		// Schedule shutdown of test deployment
		if testConfig.Duration != 0 {
			go func() {
				time.Sleep(testConfig.Duration)
				close(stopC)
			}()
		}

		// Run deployment until it stops and returns final node statuses.
		finalStatuses = deployment.Run(tickInterval, stopC)

		// Check whether all the test replicas exited correctly.
		Expect(finalStatuses).NotTo(BeNil())
		for _, status := range finalStatuses {
			if status.ExitErr != mirbft.ErrStopped {
				Expect(status.ExitErr).NotTo(HaveOccurred())
			}
			Expect(status.StatusErr).NotTo(HaveOccurred())
		}

		// Check if all requests were delivered.
		for _, replica := range deployment.TestReplicas {
			Expect(int(replica.App.RequestsProcessed)).To(Equal(testConfig.NumFakeRequests + testConfig.NumNetRequests))
		}

		fmt.Printf("Test finished.\n\n")
	}

	// After each test, check for errors and print them if any occurred.
	AfterEach(func() {
		// If the test failed
		if CurrentGinkgoTestDescription().Failed {

			// Keep the generated data
			retainedDir := fmt.Sprintf("failed-test-data/%s", currentTestConfig.Directory)
			os.MkdirAll(retainedDir, 0777)
			os.Rename(currentTestConfig.Directory, retainedDir)
			fmt.Printf("Test failed. Moved deployment data to: %s\n", retainedDir)

			// Print final status of the system.
			fmt.Printf("\n\nPrinting status because of failed test in %s\n",
				CurrentGinkgoTestDescription().TestText)

			for nodeIndex, nodeStatus := range finalStatuses {
				fmt.Printf("\nStatus for node %d\n", nodeIndex)

				// ErrStopped indicates normal termination.
				// If another error is detected, print it.
				if nodeStatus.ExitErr == mirbft.ErrStopped {
					fmt.Printf("\nStopped normally\n")
				} else {
					fmt.Printf("\nStopped with error: %+v\n", nodeStatus.ExitErr)
				}

				// Print node status if available.
				if nodeStatus.StatusErr != nil {
					// If node status could not be obtained, print the associated error.
					fmt.Printf("Could not obtain final status of node %d: %v", nodeIndex, nodeStatus.StatusErr)
				} else {
					// Otherwise, print the status.
					// TODO: print the status in a readable form.
					fmt.Printf("%v\n", nodeStatus.Status)
				}
			}
		}
	})

	table.DescribeTable("Simple tests", testFunc,
		table.Entry("Does nothing with 1 node", &deploytest.TestConfig{
			NumReplicas: 1,
			Transport:   "fake",
			Directory:   "",
			Duration:    2 * time.Second,
		}),
		table.Entry("Does nothing with 4 nodes", &deploytest.TestConfig{
			NumReplicas: 4,
			Transport:   "fake",
			Directory:   "",
			Duration:    2 * time.Second,
		}),
		table.Entry("Submits 10 fake requests with 1 node", &deploytest.TestConfig{
			NumReplicas:     1,
			Transport:       "fake",
			NumFakeRequests: 10,
			Directory:       "mirbft-deployment-test",
			Duration:        4 * time.Second,
		}),
		table.Entry("Submits 10 fake requests with 1 node, loading WAL", &deploytest.TestConfig{
			NumReplicas:     1,
			NumClients:      1,
			Transport:       "fake",
			NumFakeRequests: 10,
			Directory:       "mirbft-deployment-test",
			Duration:        4 * time.Second,
		}),
		table.Entry("Submits 10 fake requests with 4 nodes", &deploytest.TestConfig{
			NumReplicas:     4,
			NumClients:      0,
			Transport:       "fake",
			NumFakeRequests: 10,
			Directory:       "",
			Duration:        2 * time.Second,
		}),
		table.Entry("Submits 10 fake requests with 4 nodes and actual networking", &deploytest.TestConfig{
			NumReplicas:     4,
			NumClients:      1,
			Transport:       "grpc",
			NumFakeRequests: 10,
			Directory:       "",
			Duration:        2 * time.Second,
		}),
		table.Entry("Submits 10 requests with 1 node and actual networking", &deploytest.TestConfig{
			NumReplicas:    1,
			NumClients:     1,
			Transport:      "grpc",
			NumNetRequests: 10,
			Directory:      "",
			Duration:       2 * time.Second,
		}),
		table.Entry("Submits 10 requests with 4 nodes and actual networking", &deploytest.TestConfig{
			NumReplicas:    4,
			NumClients:     1,
			Transport:      "grpc",
			NumNetRequests: 10,
			Directory:      "",
			Duration:       4 * time.Second,
		}),
	)

	// Remove all temporary data produced by the tests at the end.
	AfterSuite(func() {
		for dir := range tempDirs {
			fmt.Printf("Removing temporary test directory: %v\n", dir)
			if err := os.RemoveAll(dir); err != nil {
				fmt.Printf("Could not remove directory: %v\n", err)
			}
		}
	})
})

// If config.Directory is not empty, creates a directory with that path if it does not yet exist.
// If config.Directory is empty, creates a directory in the OS-default temporary path
// and sets config.Directory to that path.
// If an error occurs, it is returned without modifying config.Directory.
func createDeploymentDir(config *deploytest.TestConfig) error {
	if config.Directory != "" {
		fmt.Printf("Using deployment dir: %s\n", config.Directory)
		// If a directory is configured, use the configured one.
		if err := os.MkdirAll(config.Directory, 0777); err != nil {
			return err
		} else {
			return nil
		}
	} else {
		// If no directory is configured, create a temporary directory in the OS-default location.
		tmpDir, err := ioutil.TempDir("", "mirbft-deployment-test.")
		fmt.Printf("Creating temp dir: %s\n", tmpDir)
		if err != nil {
			return err
		} else {
			config.Directory = tmpDir
			return nil
		}
	}
}
