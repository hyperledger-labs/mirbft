/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/deploytest"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
)

var (
	tickInterval = 100 * time.Millisecond
	testTimeout  = 10 * time.Second
)

// StressyTest attempts to spin up as 'real' a network as possible, using
// fake links, but real concurrent go routines.  This means the test is non-deterministic
// so we can't make assertions about the state of the network that are as specific
// as the more general single threaded testengine type tests.  Still, there
// seems to be value in confirming that at a basic level, a concurrent network executes
// correctly.
var _ = Describe("Basic test", func() {

	network := deploytest.NewDeployment(testConfig, doneC)
	go func() {
		nodeStatusesC <- network.Run(tickInterval)
	}()

	var (
		doneC                 chan struct{}
		expectedProposalCount int
		proposals             map[uint64]*msgs.Request
		wg                    sync.WaitGroup
		nodeStatusesC         chan []*deploytest.NodeStatus

		network *deploytest.Deployment
	)

	BeforeEach(func() {
		proposals = map[uint64]*msgs.Request{}

		doneC = make(chan struct{})

	})

	AfterEach(func() {
		close(doneC)
		wg.Wait()

		if nodeStatusesC == nil {
			fmt.Printf("Unexpected network status is nil, skipping status!\n")
			return
		}

		nodeStatuses := <-nodeStatusesC

		if !CurrentGinkgoTestDescription().Failed {
			for _, replica := range network.TestReplicas {
				os.RemoveAll(replica.TmpDir)
			}
			return
		}

		fmt.Printf("\n\nPrinting state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)

		for nodeIndex, replica := range network.TestReplicas {
			fmt.Printf("\nStatus for node %d\n", nodeIndex)
			nodeStatus := nodeStatuses[nodeIndex]

			if nodeStatus.ExitErr == mirbft.ErrStopped {
				fmt.Printf("\nStopped normally\n")
			} else {
				fmt.Printf("\nStopped with error: %+v\n", nodeStatus.ExitErr)
			}

			if nodeStatus.Status == nil {
				fmt.Printf("Could not get status for node %d", nodeIndex)
			} else {
				fmt.Printf("%s\n", nodeStatus.Status.Pretty())
			}

			fmt.Printf("\nFakeApp has %d messages\n", len(replica.App.Entries))

			if expectedProposalCount > len(replica.App.Entries) {
				fmt.Printf("Expected %d entries, but only got %d\n", len(proposals), len(replica.App.Entries))
			}

			fmt.Printf("\nLog available at %s\n", replica.EventLogPath())
		}

	})

	DescribeTable("commits all messages", func(testConfig *deploytest.TestConfig) {

		nodeStatusesC = make(chan []*deploytest.NodeStatus, 1)

		observations := map[uint64]struct{}{}
		for j, replica := range network.TestReplicas {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < testConfig.MsgCount {
				entry := &msgs.QEntry{}
				Eventually(replica.App.CommitC, 10*time.Second).Should(Receive(&entry))

				for _, req := range entry.Requests {
					Expect(req.ReqNo).To(BeNumerically("<", testConfig.MsgCount))
					_, ok := observations[req.ReqNo]
					Expect(ok).To(BeFalse())
					observations[req.ReqNo] = struct{}{}
				}
			}
		}
	},
		Entry("SingleNode greenpath", &deploytest.TestConfig{
			NodeCount: 1,
			MsgCount:  1000,
		}),

		Entry("FourNodeBFT greenpath", &deploytest.TestConfig{
			NodeCount:          4,
			CheckpointInterval: 20,
			MsgCount:           1000,
		}),

		Entry("FourNodeBFT single bucket greenpath", &deploytest.TestConfig{
			NodeCount:          4,
			BucketCount:        1,
			CheckpointInterval: 10,
			MsgCount:           1000,
		}),

		Entry("FourNodeBFT single bucket big batch greenpath", &deploytest.TestConfig{
			NodeCount:          4,
			BucketCount:        1,
			CheckpointInterval: 10,
			BatchSize:          10,
			ClientWidth:        1000,
			MsgCount:           10000,
			// ParallelProcess:    true, // TODO, re-enable once parallel processing exists again
		}),
	)
})
