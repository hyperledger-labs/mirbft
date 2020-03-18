package mirbft_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/IBM/mirbft"
	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Mirbft", func() {
	var (
		recorder  *testengine.Recorder
		recording *testengine.Recording
	)

	BeforeEach(func() {
		recorder = testengine.BasicRecorder(4, 4, 100)
	})

	JustBeforeEach(func() {
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
					fmt.Printf("Could not get status for node %d: %s\n", nodeIndex, err)
				} else {
					fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
					if err != nil {
						fmt.Printf("Node exited with err: %+v\n", err)
					}
				}
			}
		}
	})

	It("delivers all requests", func() {
		_, err := recording.DrainClients(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
	})

	When("the third node is silenced", func() {
		BeforeEach(func() {
			recorder.Manglers = []testengine.Mangler{
				&testengine.SilencingMangler{
					NodeToSilence: 3,
				},
			}
			for _, clientConfig := range recorder.ClientConfigs {
				clientConfig.Total = 20
			}
		})

		It("still delivers all requests", func() {
			_, err := recording.DrainClients(5 * time.Second)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
