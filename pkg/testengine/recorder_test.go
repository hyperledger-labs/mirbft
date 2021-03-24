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
			fmt.Printf("Printing state machine status because of failed test in %q\n", CurrentGinkgoTestDescription().TestText)
			Expect(recording).NotTo(BeNil())

			for nodeIndex, node := range recording.Nodes {
				fmt.Printf("\nStatus for node %d\n", nodeIndex)
				if node.StateMachine == nil {
					fmt.Printf("  Uninitialized\n")
					continue
				}
				status, err := node.StateMachine.Status()
				if err != nil {
					fmt.Printf("error fetching status: %v\n", err)
				} else {
					fmt.Printf("%s\n", status.Pretty())
				}
			}

			fmt.Printf("EventLog available at '%s'\n", recordingFile.Name())

			fmt.Printf("\nTest event queue looks like:\n")
			fmt.Println(recording.EventQueue.Status())
		} else {
			err := os.Remove(recordingFile.Name())
			Expect(err).NotTo(HaveOccurred())
		}
	})

	When("There is a four node network", func() {
		BeforeEach(func() {
			recorder = (&testengine.Spec{
				NodeCount:     4,
				ClientCount:   4,
				ReqsPerClient: 200,
			}).Recorder()

			var err error
			recording, err = recorder.Recording(gzWriter)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Executes and produces a log", func() {
			count, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(43950))

			fmt.Printf("Executing test required a log of %d events\n", count)

			for _, node := range recording.Nodes {
				status, err := node.StateMachine.Status()
				Expect(err).NotTo(HaveOccurred())
				Expect(status.EpochTracker.ActiveEpoch.Number).To(Equal(uint64(4)))
				Expect(status.EpochTracker.ActiveEpoch.Suspicions).To(HaveLen(0))
				//Expect(status.EpochTracker.EpochTargets[0].Suspicions).To(BeEmpty())

				// Expect(fmt.Sprintf("%x", node.State.ActiveHash.Sum(nil))).To(BeEmpty())
				Expect(fmt.Sprintf("%x", node.State.ActiveHash.Sum(nil))).To(Equal("cb81c7299ad4019baca241f267d570f1b451b751717ce18bb8efc16ae8a555c4"))
			}
		})
	})

	When("A single-node network is selected", func() {
		BeforeEach(func() {
			recorder = (&testengine.Spec{
				NodeCount:     1,
				ClientCount:   1,
				ReqsPerClient: 3,
			}).Recorder()

			var err error
			recording, err = recorder.Recording(gzWriter)
			Expect(err).NotTo(HaveOccurred())
		})

		It("still executes and produces a log", func() {
			count, err := recording.DrainClients(100)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(67))
		})
	})
})
