package mirbft_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/IBM/mirbft"
	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Mirbft", func() {
	var (
		recorder      *testengine.Recorder
		recording     *testengine.Recording
		recordingFile *os.File
		gzWriter      *gzip.Writer
	)

	BeforeEach(func() {
		recorder = testengine.BasicRecorder(4, 4, 100)
		Expect(recorder.NetworkState.Config.MaxEpochLength).To(Equal(uint64(200)))

		tDesc := CurrentGinkgoTestDescription()
		var err error
		recordingFile, err = ioutil.TempFile("", fmt.Sprintf("%s.%d-*.eventlog", filepath.Base(tDesc.FileName), tDesc.LineNumber))
		Expect(err).NotTo(HaveOccurred())

		gzWriter = gzip.NewWriter(recordingFile)
	})

	JustBeforeEach(func() {
		var err error
		recording, err = recorder.Recording(gzWriter)
		Expect(err).NotTo(HaveOccurred())
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

		tDesc := CurrentGinkgoTestDescription()
		if tDesc.Failed {
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

	It("delivers all requests", func() {
		_, err := recording.DrainClients(50000)
		Expect(err).NotTo(HaveOccurred())
	})

	When("a larger batch size is used", func() {
		BeforeEach(func() {
			for _, nodeConfig := range recorder.RecorderNodeConfigs {
				nodeConfig.InitParms.BatchSize = 20
			}
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network is has just one client", func() {
		BeforeEach(func() {
			recorder = testengine.BasicRecorder(4, 1, 200)
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network is comprised of just one node", func() {
		BeforeEach(func() {
			recorder = testengine.BasicRecorder(1, 1, 20)
			for _, clientConfig := range recorder.ClientConfigs {
				clientConfig.Total = 20
			}
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(500)
			Expect(err).NotTo(HaveOccurred())
		})

		When("the node crashes in the middle", func() {
			BeforeEach(func() {
				recorder.Mangler = testengine.MangleMsgs().FromSelf().OfTypeCheckpoint().WithSequence(5).CrashAndRestartAfter(10, recorder.RecorderNodeConfigs[0].InitParms)
			})

			It("still delivers all requests", func() {
				_, err := recording.DrainClients(5000)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	When("the first node is silenced", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().FromNodes(0).Drop()
			for _, clientConfig := range recorder.ClientConfigs {
				clientConfig.Total = 20
			}
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the third node is silenced", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().FromNodes(3).Drop()
			for _, clientConfig := range recorder.ClientConfigs {
				clientConfig.Total = 20
			}
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network loses 2 percent of messages", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().AtPercent(2).Drop()
		})

		PIt("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network loses many acks", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().OfTypeRequestAck().AtPercent(20).Drop()
			for _, clientConfig := range recorder.ClientConfigs {
				clientConfig.Total = 20
			}
			// TODO, we need to configure a very short ack re-transmit interval
		})

		PIt("still delivers all requests", func() {
			_, err := recording.DrainClients(7000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network messages have up to a 30ms jittery delay", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().Jitter(30)
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	When("the network duplicates messages 75 percent of the time", func() {
		BeforeEach(func() {
			recorder.Mangler = testengine.MangleMsgs().AtPercent(75).Duplicate(300)
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(50000)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
