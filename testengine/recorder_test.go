package testengine_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/testengine"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"

	"go.uber.org/zap"
)

var _ = Describe("Recorder", func() {
	var (
		networkConfig *pb.NetworkConfig
		clientConfigs []*testengine.ClientConfig
		nodeConfigs   []*tpb.NodeConfig
		logger        *zap.Logger
		player        *testengine.Player
		recorder      *testengine.Recorder
		recording     *testengine.Recording
		totalReqs     uint64
	)

	BeforeEach(func() {
		nodeCount := 4

		networkConfig = mirbft.StandardInitialNetworkConfig(nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodeConfigs = append(nodeConfigs, &tpb.NodeConfig{
				Id:                   uint64(i),
				HeartbeatTicks:       2,
				SuspectTicks:         4,
				NewEpochTimeoutTicks: 8,
				TickInterval:         500,
				LinkLatency:          100,
				ReadyLatency:         50,
				ProcessLatency:       10,
			})
		}

		clientCount := 4

		for i := 0; i < clientCount; i++ {
			total := uint64(200)
			clientConfigs = append(clientConfigs, &testengine.ClientConfig{
				ID:          []byte(fmt.Sprintf("%d", i)),
				MaxInFlight: int(networkConfig.CheckpointInterval / 2),
				Total:       total,
			})
			totalReqs += total
		}

		var err error
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		recorder = &testengine.Recorder{
			NetworkConfig: networkConfig,
			NodeConfigs:   nodeConfigs,
			Logger:        logger,
			Hasher:        sha256.New,
			ClientConfigs: clientConfigs,
		}

		recording, err = recorder.Recording()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if player != nil && player.DoneC != nil {
			close(player.DoneC)
		}

		if logger != nil {
			logger.Sync()
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
		}

	})

	It("Executes and produces a log", func() {
		start := time.Now()
		for {
			err := recording.Step()
			Expect(err).NotTo(HaveOccurred())

			allDone := true
			for _, node := range recording.Nodes {
				if node.State.Length < totalReqs {
					allDone = false
					break
				}
			}

			if time.Since(start) > 9*time.Second {
				panic("test took too long")
			}

			if allDone {
				break
			}
		}

		for _, node := range recording.Nodes {
			status, err := node.PlaybackNode.Node.Status(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(status.EpochChanger.LastActiveEpoch).To(Equal(uint64(0)))
			Expect(status.EpochChanger.EpochTargets).To(HaveLen(1))
			Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
			Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
			Expect(node.State.Length).To(Equal(totalReqs))
			Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(864)))
			// Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("dfae94a36c763806b7894e7c49e8557cce1d4662a01c14749787e9e0ad930fe1"))
			// Expect(fmt.Sprintf("%x", node.State.Value)).To(BeEmpty())
		}
	})
})
