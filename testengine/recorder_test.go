package testengine_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Recorder", func() {
	var (
		recorder  *testengine.Recorder
		recording *testengine.Recording
		totalReqs uint64
	)

	BeforeEach(func() {
		recorder = testengine.BasicRecorder(4, 4, 200)
		recorder.NetworkConfig.MaxEpochLength = 100000 // XXX this works around a bug in the library for now
		totalReqs = 4 * 200

		var err error
		recording, err = recorder.Recording()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if recording != nil && recording.Player != nil && recording.Player.DoneC != nil {
			close(recording.Player.DoneC)
		}

		if recorder.Logger != nil {
			recorder.Logger.Sync()
		}

		if CurrentGinkgoTestDescription().Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			Expect(recording).NotTo(BeNil())

			for nodeIndex, node := range recording.Nodes {
				status, err := node.PlaybackNode.Node.Status(context.Background())
				if err != nil && status == nil {
					fmt.Printf("Could not get status for node %d: %s", nodeIndex, err)
				} else {
					fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
				}
			}

			fmt.Printf("\nWriting EventLog to disk\n")
			tDesc := CurrentGinkgoTestDescription()
			tmpFile, err := ioutil.TempFile("", fmt.Sprintf("%s.%d-*.eventlog", filepath.Base(tDesc.FileName), tDesc.LineNumber))
			if err != nil {
				fmt.Printf("Encountered error creating tempfile: %s\n", err)
				return
			}
			defer tmpFile.Close()
			err = recording.EventLog.Write(tmpFile)
			Expect(err).NotTo(HaveOccurred())
			fmt.Printf("EventLog available at '%s'\n", tmpFile.Name())
		}

	})

	It("Executes and produces a log", func() {
		count, err := recording.DrainClients(50000)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(36305))

		fmt.Printf("Executing test required a log of %d events\n", count)

		for _, node := range recording.Nodes {
			status, err := node.PlaybackNode.Node.Status(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(status.EpochChanger.LastActiveEpoch).To(Equal(uint64(0)))
			Expect(status.EpochChanger.EpochTargets).To(HaveLen(1))
			Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
			Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
			Expect(node.State.Length).To(Equal(totalReqs))
			Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(800)))

			// Expect(fmt.Sprintf("%x", node.State.Value)).To(BeEmpty())
			Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("689567a3721c39955e37f71beee801fdf89862ac9d2b2354ee7e01e9a2839230"))
		}
	})

	When("A single-node network is selected", func() {
		BeforeEach(func() {
			recorder = testengine.BasicRecorder(1, 1, 3)

			var err error
			recording, err = recorder.Recording()
			Expect(err).NotTo(HaveOccurred())
		})

		It("still executes and produces a log", func() {
			count, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(27))
		})
	})
})
