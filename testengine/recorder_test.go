package testengine_test

import (
	"context"
	"crypto/sha256"
	"fmt"

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
		nodeConfigs   []*tpb.NodeConfig
		eventLog      *testengine.EventLog
		player        *testengine.Player
		recorder      *testengine.Recorder
	)

	Describe("Single Node", func() {
		BeforeEach(func() {
			networkConfig = mirbft.StandardInitialNetworkConfig(1)

			nodeConfigs = []*tpb.NodeConfig{
				{
					Id:                   0,
					HeartbeatTicks:       2,
					SuspectTicks:         4,
					NewEpochTimeoutTicks: 8,
					TickInterval:         500,
					LinkLatency:          100,
					ReadyLatency:         50,
					ProcessLatency:       10,
				},
			}

			eventLog = &testengine.EventLog{
				InitialConfig: networkConfig,
				NodeConfigs:   nodeConfigs,
			}

			logger, err := zap.NewProduction()
			Expect(err).NotTo(HaveOccurred())

			player, err = testengine.NewPlayer(eventLog, logger)
			Expect(err).NotTo(HaveOccurred())

			recorder = testengine.NewRecorder(player, sha256.New)
		})

		AfterEach(func() {
			if player != nil && player.DoneC != nil {
				close(player.DoneC)
			}
		})

		It("Executes and produces a log", func() {
			fmt.Println("Starting test")
			for i := 0; i < 1000; i++ {
				err := recorder.Step()
				Expect(err).NotTo(HaveOccurred())
			}

			for _, node := range recorder.Nodes {
				status, err := node.PlaybackNode.Node.Status(context.Background())
				Expect(err).NotTo(HaveOccurred())
				Expect(status.EpochChanger.LastActiveEpoch).To(Equal(uint64(0)))
				Expect(status.EpochChanger.EpochTargets).To(HaveLen(1))
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(node.State.Length).To(Equal(uint64(0)))
				Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(105)))
				Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"))
			}
		})
	})

	Describe("Four Node", func() {
		BeforeEach(func() {
			networkConfig = mirbft.StandardInitialNetworkConfig(4)

			nodeConfigs = []*tpb.NodeConfig{
				{
					Id:                   0,
					HeartbeatTicks:       2,
					SuspectTicks:         4,
					NewEpochTimeoutTicks: 8,
					TickInterval:         500,
					LinkLatency:          100,
					ReadyLatency:         50,
					ProcessLatency:       10,
				},
				{
					Id:                   1,
					HeartbeatTicks:       2,
					SuspectTicks:         4,
					NewEpochTimeoutTicks: 8,
					TickInterval:         500,
					LinkLatency:          100,
					ReadyLatency:         50,
					ProcessLatency:       10,
				},
				{
					Id:                   2,
					HeartbeatTicks:       2,
					SuspectTicks:         4,
					NewEpochTimeoutTicks: 8,
					TickInterval:         500,
					LinkLatency:          100,
					ReadyLatency:         50,
					ProcessLatency:       10,
				},
				{
					Id:                   3,
					HeartbeatTicks:       2,
					SuspectTicks:         4,
					NewEpochTimeoutTicks: 8,
					TickInterval:         500,
					LinkLatency:          100,
					ReadyLatency:         50,
					ProcessLatency:       10,
				},
			}

			eventLog = &testengine.EventLog{
				InitialConfig: networkConfig,
				NodeConfigs:   nodeConfigs,
			}

			logger, err := zap.NewProduction()
			Expect(err).NotTo(HaveOccurred())

			player, err = testengine.NewPlayer(eventLog, logger)
			Expect(err).NotTo(HaveOccurred())

			recorder = testengine.NewRecorder(player, sha256.New)
		})

		AfterEach(func() {
			if player != nil && player.DoneC != nil {
				close(player.DoneC)
			}
		})

		It("Executes and produces a log", func() {
			fmt.Println("Starting test")
			for i := 0; i < 10000; i++ {
				err := recorder.Step()
				Expect(err).NotTo(HaveOccurred())
			}

			for _, node := range recorder.Nodes {
				status, err := node.PlaybackNode.Node.Status(context.Background())
				Expect(err).NotTo(HaveOccurred())
				Expect(status.EpochChanger.LastActiveEpoch).To(Equal(uint64(0)))
				Expect(status.EpochChanger.EpochTargets).To(HaveLen(1))
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(status.EpochChanger.EpochTargets[0].Suspicions).To(BeEmpty())
				Expect(node.State.Length).To(Equal(uint64(0)))
				Expect(node.State.LastCommittedSeqNo).To(Equal(uint64(260)))
				Expect(fmt.Sprintf("%x", node.State.Value)).To(Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"))
			}
		})
	})
})
