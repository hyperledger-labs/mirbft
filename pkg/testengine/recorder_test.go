package testengine_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/pkg/testengine"
)

var _ = Describe("Recorder", func() {
	var (
		recorder      *testengine.Recorder
		recording     *testengine.Recording
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
			recorder = testengine.BasicRecorder(4, 4, 200)
			recorder.NetworkState.Config.MaxEpochLength = 100000 // XXX this works around a bug in the library for now

			var err error
			recording, err = recorder.Recording(gzWriter)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Executes and produces a log", func() {
			count, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(46785))

			fmt.Printf("Executing test required a log of %d events\n", count)

			for _, node := range recording.Nodes {
				status := node.PlaybackNode.StateMachine.Status()
				Expect(status.EpochTracker.LastActiveEpoch).To(Equal(uint64(1)))
				Expect(status.EpochTracker.EpochTargets).To(HaveLen(0))
				//Expect(status.EpochTracker.EpochTargets[0].Suspicions).To(BeEmpty())

				// Expect(fmt.Sprintf("%x", node.State.ActiveHash.Sum(nil))).To(BeEmpty())
				Expect(fmt.Sprintf("%x", node.State.ActiveHash.Sum(nil))).To(Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"))
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
			Expect(count).To(Equal(52))
		})
	})
})
