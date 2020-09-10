package testengine_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Recorder", func() {
	var (
		recorder      *testengine.Recorder
		recording     *testengine.Recording
		totalReqs     uint64
		recordingFile *os.File
		gzWriter      *gzip.Writer
	)

	BeforeEach(func() {
		totalReqs = 4 * 200

		tDesc := CurrentGinkgoTestDescription()
		var err error
		recordingFile, err = ioutil.TempFile("", fmt.Sprintf("%s.%d-*.eventlog", filepath.Base(tDesc.FileName), tDesc.LineNumber))
		Expect(err).NotTo(HaveOccurred())

		gzWriter = gzip.NewWriter(recordingFile)
	})

	AfterEach(func() {
		if recorder.Logger != nil {
			recorder.Logger.Sync()
		}

		if gzWriter != nil {
			gzWriter.Close()
		}

		if recordingFile != nil {
			recordingFile.Close()
		}

		if CurrentGinkgoTestDescription().Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			Expect(recording).NotTo(BeNil())

			for nodeIndex, node := range recording.Nodes {
				status := node.PlaybackNode.StateMachine.Status()
				fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
			}

			fmt.Printf("EventLog available at '%s'\n", recordingFile.Name())
		} else {
			err := os.Remove(recordingFile.Name())
			Expect(err).NotTo(HaveOccurred())
		}
	})

	When("There is a four node network", func() {
		BeforeEach(func() {
			totalReqs = 4 * 200
			recorder = testengine.BasicRecorder(4, 4, 200)
			recorder.NetworkState.Config.MaxEpochLength = 100000 // XXX this works around a bug in the library for now

			var err error
			recording, err = recorder.Recording(gzWriter)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Executes and produces a log", func() {
			count, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(43806))

			fmt.Printf("Executing test required a log of %d events\n", count)

			for _, node := range recording.Nodes {
				status := node.PlaybackNode.StateMachine.Status()
				Expect(status.EpochTracker.LastActiveEpoch).To(Equal(uint64(1)))
				Expect(status.EpochTracker.EpochTargets).To(HaveLen(1))
				Expect(status.EpochTracker.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(node.State.Length).To(Equal(totalReqs))
				Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(800)))

				// Expect(fmt.Sprintf("%x", node.State.Value)).To(BeEmpty())
				Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("da16dfe9948fda2694e0825338d874cba74757fadd311f330ce15635fcafeee0"))
			}
		})
	})

	When("A single-node network is selected", func() {
		BeforeEach(func() {
			recorder = testengine.BasicRecorder(1, 1, 3)

			var err error
			recording, err = recorder.Recording(gzWriter)
			Expect(err).NotTo(HaveOccurred())
		})

		It("still executes and produces a log", func() {
			count, err := recording.DrainClients(100)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(49))
		})
	})
})
