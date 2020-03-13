package testengine_test

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/testengine"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"

	"github.com/golang/protobuf/proto"
)

var _ = Describe("Eventlog", func() {

	var (
		networkConfig *pb.NetworkConfig
		nodeConfigs   []*tpb.NodeConfig
		eventLog      *testengine.EventLog
	)

	BeforeEach(func() {
		networkConfig = mirbft.StandardInitialNetworkConfig(7)

		nodeConfigs = []*tpb.NodeConfig{
			{
				Id:                   7,
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
			Name:          "fake-name",
			Description:   "fake-description",
			InitialConfig: networkConfig,
			NodeConfigs:   nodeConfigs,
		}

		eventLog.InsertTick(1, 10)
		eventLog.InsertTick(2, 20)

	})

	It("can roundtrip a log", func() {
		var buffer bytes.Buffer
		err := eventLog.Write(&buffer)
		Expect(err).NotTo(HaveOccurred())

		newEventLog, err := testengine.ReadEventLog(&buffer)
		Expect(err).NotTo(HaveOccurred())
		Expect(newEventLog.Name).To(Equal("fake-name"))
		Expect(newEventLog.Description).To(Equal("fake-description"))
		Expect(proto.Equal(eventLog.InitialConfig, newEventLog.InitialConfig)).To(BeTrue())
		Expect(proto.Equal(eventLog.NodeConfigs[0], newEventLog.NodeConfigs[0])).To(BeTrue())
		Expect(proto.Equal(
			eventLog.FirstEventLogEntry.Event,
			newEventLog.FirstEventLogEntry.Event,
		)).To(BeTrue())
		Expect(proto.Equal(
			eventLog.FirstEventLogEntry.Next.Event,
			newEventLog.FirstEventLogEntry.Next.Event,
		)).To(BeTrue())
		Expect(newEventLog.FirstEventLogEntry.Next.Next).To(BeNil())
	})

})
