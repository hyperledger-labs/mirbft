package testengine_test

import (
	"bytes"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/testengine"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

// For this test, place one event log at 'eventlog-1.bin', and another at 'eventlog-2.bin'.
// This test will identify, and dump as JSON the first difference between the two logs
var _ = XDescribe("Non-determinism finding test", func() {
	var jsonMarshaler = &jsonpb.Marshaler{
		EmitDefaults: true,
		OrigName:     true,
		Indent:       "  ",
	}

	It("compares the two files", func() {
		f1, err := os.Open("eventlog-1.bin")
		Expect(err).NotTo(HaveOccurred())
		eventLog1, err := testengine.ReadEventLog(f1)
		f1.Close()
		Expect(err).NotTo(HaveOccurred())

		f2, err := os.Open("eventlog-2.bin")
		Expect(err).NotTo(HaveOccurred())
		eventLog2, err := testengine.ReadEventLog(f2)
		f2.Close()
		Expect(err).NotTo(HaveOccurred())

		logEntry1 := eventLog1.FirstEventLogEntry
		logEntry2 := eventLog2.FirstEventLogEntry
		for {
			if !proto.Equal(
				logEntry1.Event,
				logEntry2.Event,
			) {
				jLogEntry1, err := jsonMarshaler.MarshalToString(logEntry1.Event)
				Expect(err).NotTo(HaveOccurred())

				jLogEntry2, err := jsonMarshaler.MarshalToString(logEntry2.Event)
				Expect(err).NotTo(HaveOccurred())

				Expect(jLogEntry1).To(MatchJSON(jLogEntry2))
				fmt.Printf("logEntry1=%+v\n", logEntry1)
				fmt.Printf("logEntry2=%+v\n", logEntry2)
				Fail("protos are not equal")
			}

			logEntry1 = logEntry1.Next
			logEntry2 = logEntry2.Next

			if logEntry1 == nil {
				Expect(logEntry2).To(BeNil())
				break
			}
		}
	})
})

var _ = Describe("Eventlog", func() {

	var (
		eventLog *testengine.EventLog
	)

	BeforeEach(func() {
		eventLog = &testengine.EventLog{
			Name:        "fake-name",
			Description: "fake-description",
		}

		eventLog.InsertStateEvent(1, &pb.StateEvent{Type: &pb.StateEvent_Tick{Tick: &pb.StateEvent_TickElapsed{}}}, 10)
		eventLog.InsertStateEvent(2, &pb.StateEvent{Type: &pb.StateEvent_Tick{Tick: &pb.StateEvent_TickElapsed{}}}, 20)

		Expect(eventLog.ConsumeAndAdvance()).NotTo(BeNil())
		Expect(eventLog.ConsumeAndAdvance()).NotTo(BeNil())
	})

	It("can roundtrip a log", func() {
		var buffer bytes.Buffer
		err := eventLog.Write(&buffer)
		Expect(err).NotTo(HaveOccurred())

		newEventLog, err := testengine.ReadEventLog(&buffer)
		Expect(err).NotTo(HaveOccurred())
		Expect(newEventLog.Name).To(Equal("fake-name"))
		Expect(newEventLog.Description).To(Equal("fake-description"))
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
