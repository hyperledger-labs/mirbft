package testengine_test

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/testengine"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// For this test, place one event log at 'eventlog-1.bin', and another at 'eventlog-2.bin'.
// This test will identify, and dump as JSON the first difference between the two logs
var _ = XDescribe("Non-determinism finding test", func() {
	var jsonMarshaler = &protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
		Indent:          "  ",
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

		logEntry1 := eventLog1.List.Front()
		logEntry2 := eventLog2.List.Front()
		for {
			e1 := logEntry1.Value.(*rpb.RecordedEvent)
			e2 := logEntry2.Value.(*rpb.RecordedEvent)

			if !proto.Equal(e1, e2) {
				jLogEntry1, err := jsonMarshaler.Marshal(e1)
				Expect(err).NotTo(HaveOccurred())

				jLogEntry2, err := jsonMarshaler.Marshal(e2)
				Expect(err).NotTo(HaveOccurred())

				Expect(jLogEntry1).To(MatchJSON(jLogEntry2))
				fmt.Printf("logEntry1=%+v\n", e1)
				fmt.Printf("logEntry2=%+v\n", e2)
				Fail("protos are not equal")
			}

			logEntry1 = logEntry1.Next()
			logEntry2 = logEntry2.Next()

			if logEntry1 == nil {
				Expect(logEntry2).To(BeNil())
				break
			}
		}
	})
})

var tickEvent = &pb.StateEvent{Type: &pb.StateEvent_Tick{Tick: &pb.StateEvent_TickElapsed{}}}

var _ = Describe("Eventlog", func() {

	var (
		serializedLog *bytes.Buffer
	)

	BeforeEach(func() {
		serializedLog = &bytes.Buffer{}
		gzw := gzip.NewWriter(serializedLog)
		defer gzw.Close()

		initialLog := testengine.EventLog{
			Output: gzw,
			List:   list.New(),
		}
		initialLog.Insert(
			&rpb.RecordedEvent{
				NodeId:     1,
				StateEvent: tickEvent,
				Time:       10,
			},
		)
		initialLog.Insert(
			&rpb.RecordedEvent{
				NodeId:     2,
				StateEvent: tickEvent,
				Time:       20,
			},
		)
		_, err := initialLog.ReadEvent()
		Expect(err).NotTo(HaveOccurred())
		_, err = initialLog.ReadEvent()
		Expect(err).NotTo(HaveOccurred())
	})

	It("can roundtrip a log", func() {
		eventLog, err := testengine.ReadEventLog(serializedLog)
		Expect(err).NotTo(HaveOccurred())

		Expect(eventLog.List.Len()).To(Equal(2))
		Expect(proto.Equal(
			eventLog.List.Front().Value.(*rpb.RecordedEvent),
			&rpb.RecordedEvent{
				NodeId:     1,
				StateEvent: &pb.StateEvent{Type: &pb.StateEvent_Tick{Tick: &pb.StateEvent_TickElapsed{}}},
				Time:       10,
			},
		)).To(BeTrue())
		Expect(proto.Equal(
			eventLog.List.Back().Value.(*rpb.RecordedEvent),
			&rpb.RecordedEvent{
				NodeId:     2,
				StateEvent: &pb.StateEvent{Type: &pb.StateEvent_Tick{Tick: &pb.StateEvent_TickElapsed{}}},
				Time:       20,
			},
		)).To(BeTrue())
	})
})
