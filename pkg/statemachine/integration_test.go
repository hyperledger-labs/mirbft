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

	DescribeTable("delivers all requests", func(spec Spec) {
		recorder := spec.Recorder()

		var err error
		recording, err = recorder.Recording(gzWriter)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(50000)
		Expect(err).NotTo(HaveOccurred())
	},
		Entry("one-node-one-client-green", Spec{
			NodeCount:     1,
			ClientCount:   1,
			ReqsPerClient: 100,
		}),
		Entry("one-node-one-client-large-batch-green", Spec{
			NodeCount:     1,
			ClientCount:   1,
			ReqsPerClient: 100,
			BatchSize:     20,
		}),
		Entry("one-node-four-client-green", Spec{
			NodeCount:     1,
			ClientCount:   4,
			ReqsPerClient: 100,
		}),
		Entry("four-node-one-client-green", Spec{
			NodeCount:     4,
			ClientCount:   1,
			ReqsPerClient: 100,
		}),
		Entry("four-node-four-client-green", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 100,
		}),
		Entry("four-node-four-client-large-batch-green", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 100,
			BatchSize:     20,
		}),
		Entry("a client ignores node 0", Spec{
			NodeCount:     4,
			ClientCount:   1,
			ReqsPerClient: 100,
			ClientsIgnore: []uint64{0},
		}),
		Entry("node0 crashes in the middle", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 100,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().FromSelf().OfTypeCheckpoint().WithSequence(5)).CrashAndRestartAfter(10, r.NodeConfigs[0].InitParms)
			},
		}),
		Entry("node0 is silenced", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().FromNodes(0)).Drop()
			},
		}),
		Entry("node3 is silenced", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().FromNodes(3)).Drop()
			},
		}),
		Entry("node3 starts late", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = Until(MatchMsgs().FromNode(1).OfTypeCheckpoint().WithSequence(20)).Do(For(MatchNodeStartup().ForNode(3)).Delay(500))
			},
		}),
		Entry("network drops 2 percent of messages", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 100,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().AtPercent(2)).Drop()
			},
		}),
		Entry("network drops almost all acks from node0 and node1", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().FromNodes(0, 1).OfTypeRequestAck().AtPercent(70)).Drop()
			},
		}),
		Entry("network messages have a small 30ms jittery delay", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs()).Jitter(30)
			},
		}),
		Entry("network messages have a large 1000ms jittery delay", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs()).Jitter(1000)
			},
		}),
		Entry("network messages are duplicated most of the time", Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
			TweakRecorder: func(r *Recorder) {
				r.Mangler = For(MatchMsgs().AtPercent(75)).Duplicate(300)
			},
		}),
	)
})
