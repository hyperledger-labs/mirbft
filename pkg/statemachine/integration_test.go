/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemachine_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	. "github.com/IBM/mirbft/pkg/testengine"
)

type Occurred int

const (
	No Occurred = iota
	Yes
	Maybe
)

type TestConf struct {
	Spec       Spec
	Assertions Assertions
}

type Assertions struct {
	CompletesInSteps      int
	StateTransferOccurred map[uint64]Occurred
}

var _ = Describe("Mirbft", func() {
	var (
		recording     *Recording
		recordingFile *os.File
		gzWriter      *gzip.Writer
	)

	BeforeEach(func() {
		tDesc := CurrentGinkgoTestDescription()
		var err error
		recordingFile, err = ioutil.TempFile("", fmt.Sprintf("%s.%d-*.eventlog", filepath.Base(tDesc.FileName), tDesc.LineNumber))
		Expect(err).NotTo(HaveOccurred())

		gzWriter = gzip.NewWriter(recordingFile)
	})

	AfterEach(func() {
		if gzWriter != nil {
			gzWriter.Close()
		}

		if recordingFile != nil {
			recordingFile.Close()
		}

		tDesc := CurrentGinkgoTestDescription()
		if tDesc.Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			Expect(recording).NotTo(BeNil())

			for nodeIndex, node := range recording.Nodes {
				if node.StateMachine == nil {
					fmt.Printf("\nStatus for node %d unavailable as it is nto started\n\n", nodeIndex)
					continue
				}
				status := node.StateMachine.Status()
				fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
			}

			fmt.Printf("EventLog available at '%s'\n", recordingFile.Name())
		} else {
			err := os.Remove(recordingFile.Name())
			Expect(err).NotTo(HaveOccurred())
		}
	})

	DescribeTable("delivers all requests", func(testConf TestConf) {
		recorder := testConf.Spec.Recorder()

		var err error
		recording, err = recorder.Recording(gzWriter)
		Expect(err).NotTo(HaveOccurred())

		steps, err := recording.DrainClients(testConf.Assertions.CompletesInSteps)
		Expect(err).NotTo(HaveOccurred())
		// This assertion is to ensure that we have our step expectations reasonably tight,
		// as drastically increasing or decreasing the number of steps is a red flag.
		Expect(steps).To(BeNumerically(">=", testConf.Assertions.CompletesInSteps/2))

		for _, node := range recording.Nodes {
			nodeID := node.Config.InitParms.Id
			stExpected := testConf.Assertions.StateTransferOccurred[nodeID]
			switch {
			case stExpected == Yes && len(node.State.StateTransfers) == 0:
				Fail(fmt.Sprintf("expected state transfers, but at least node %d did not", nodeID))
			case stExpected == No && len(node.State.StateTransfers) > 0:
				Fail(fmt.Sprintf("expected no state transfers, but at least node %d did", nodeID))
			default:
			}
		}
	},
		Entry("one-node-one-client-green", TestConf{
			Spec: Spec{
				NodeCount:     1,
				ClientCount:   1,
				ReqsPerClient: 100,
			},
			Assertions: Assertions{
				CompletesInSteps: 500,
			},
		}),
		Entry("one-node-one-client-large-batch-green", TestConf{
			Spec: Spec{
				NodeCount:     1,
				ClientCount:   1,
				ReqsPerClient: 100,
				BatchSize:     20,
			},
			Assertions: Assertions{
				CompletesInSteps: 300,
			},
		}),
		Entry("one-node-four-client-green", TestConf{
			Spec: Spec{
				NodeCount:     1,
				ClientCount:   4,
				ReqsPerClient: 100,
			},
			Assertions: Assertions{
				CompletesInSteps: 1000,
			},
		}),
		Entry("four-node-one-client-green", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   1,
				ReqsPerClient: 100,
			},
			Assertions: Assertions{
				CompletesInSteps: 5000,
			},
		}),
		Entry("four-node-four-client-green", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 100,
			},
			Assertions: Assertions{
				CompletesInSteps: 20000,
			},
		}),
		Entry("four-node-four-client-large-batch-green", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 100,
				BatchSize:     20,
			},
			Assertions: Assertions{
				CompletesInSteps: 10000,
			},
		}),
		Entry("a client ignores node 0", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   1,
				ReqsPerClient: 100,
				ClientsIgnore: []uint64{0},
			},
			Assertions: Assertions{
				CompletesInSteps: 20000,
				StateTransferOccurred: map[uint64]Occurred{
					0: Yes, // XXX this should be false once forwarding is working again
				},
			},
		}),
		Entry("node0 crashes in the middle", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 100,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().FromSelf().OfTypeCheckpoint().WithSequence(5)).CrashAndRestartAfter(10, r.NodeConfigs[0].InitParms)
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 20000,
			},
		}),
		Entry("node0 is silenced", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().FromNodes(0)).Drop()
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 5000,
			},
		}),
		Entry("node3 is silenced", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().FromNodes(3)).Drop()
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 5000,
			},
		}),
		Entry("node3 starts late", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = Until(MatchMsgs().FromNode(1).OfTypeCheckpoint().WithSequence(20)).Do(For(MatchNodeStartup().ForNode(3)).Delay(500))
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 10000,
				StateTransferOccurred: map[uint64]Occurred{
					3: Yes,
				},
			},
		}),
		Entry("network drops 2 percent of messages", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 100,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().AtPercent(2)).Drop()
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 30000,
				StateTransferOccurred: map[uint64]Occurred{
					0: Maybe,
					1: Maybe,
					2: Maybe,
					3: Maybe,
				},
			},
		}),
		Entry("network drops almost all acks from node0 and node1", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().FromNodes(0, 1).OfTypeRequestAck().AtPercent(70)).Drop()
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 20000,
			},
		}),
		Entry("network messages have a small 30ms jittery delay", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs()).Jitter(30)
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 5000,
			},
		}),
		Entry("network messages have a large 1000ms jittery delay", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs()).Jitter(1000)
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 10000,
				StateTransferOccurred: map[uint64]Occurred{
					0: Maybe,
					1: Maybe,
					2: Maybe,
					3: Maybe,
				},
			},
		}),
		Entry("network messages are duplicated most of the time", TestConf{
			Spec: Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 20,
				TweakRecorder: func(r *Recorder) {
					r.Mangler = For(MatchMsgs().AtPercent(75)).Duplicate(300)
				},
			},
			Assertions: Assertions{
				CompletesInSteps: 8000,
			},
		}),
	)
})
