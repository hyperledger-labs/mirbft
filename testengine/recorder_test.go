package testengine_test

import (
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
		totalReqs = 4 * 200
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
				status := node.PlaybackNode.StateMachine.Status()
				fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
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

	When("There is a four node network", func() {
		BeforeEach(func() {
			totalReqs = 4 * 200
			recorder = testengine.BasicRecorder(4, 4, 200)
			recorder.NetworkState.Config.MaxEpochLength = 100000 // XXX this works around a bug in the library for now

			var err error
			recording, err = recorder.Recording()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Executes and produces a log", func() {
			count, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(36393))

			fmt.Printf("Executing test required a log of %d events\n", count)

			for _, node := range recording.Nodes {
				status := node.PlaybackNode.StateMachine.Status()
				Expect(status.EpochChanger.LastActiveEpoch).To(Equal(uint64(1)))
				Expect(status.EpochChanger.EpochTargets).To(HaveLen(2))
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(node.State.Length).To(Equal(totalReqs))
				Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(800)))

				// Expect(fmt.Sprintf("%x", node.State.Value)).To(BeEmpty())
				Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("105dd39693d8df1564db08fb0d2e5e3e04abf267039d5ae2a02f66af19cdb34b"))
			}
		})
	})

	When("A single-node network is selected", func() {
		BeforeEach(func() {
			recorder = testengine.BasicRecorder(1, 1, 3)

			var err error
			recording, err = recorder.Recording()
			Expect(err).NotTo(HaveOccurred())
		})

		It("still executes and produces a log", func() {
			count, err := recording.DrainClients(50)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(30))
		})
	})
})
