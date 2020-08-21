package recorder_test

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/golang/protobuf/proto"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/recorder"
	rpb "github.com/IBM/mirbft/recorder/recorderpb"
)

var tickEvent = &pb.StateEvent{
	Type: &pb.StateEvent_Tick{
		Tick: &pb.StateEvent_TickElapsed{},
	},
}

var _ = Describe("Interceptor", func() {
	var (
		interceptor *recorder.Interceptor
		output      *bytes.Buffer
		doneC       chan struct{}
	)

	BeforeEach(func() {
		interceptor = recorder.NewInterceptor(1, func() int64 { return 2 }, 3)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)

		output = &bytes.Buffer{}
		doneC = make(chan struct{})
	})

	It("intercepts and writes state events", func() {
		goDone := make(chan struct{})
		go func() {
			err := interceptor.Drain(output, doneC)
			Expect(err).NotTo(HaveOccurred())
			close(goDone)
		}()

		close(doneC)
		Eventually(goDone).Should(BeClosed())

		Expect(output.Len()).To(Equal(18))
	})

	It("blocks when the buffer space is exceeded", func() {
		goDone := make(chan struct{})
		go func() {
			interceptor.Intercept(tickEvent)
			interceptor.Intercept(tickEvent)
			close(goDone)
		}()

		Consistently(goDone).ShouldNot(BeClosed())
		close(doneC)
	})

	It("writes everything in the buffer even when doneC is closed", func() {
		close(doneC)
		interceptor.Intercept(tickEvent)
		err := interceptor.Drain(output, doneC)
		Expect(err).NotTo(HaveOccurred())
		Expect(output.Len()).To(Equal(27))
	})

	It("can be read back with a Reader", func() {
		close(doneC)
		err := interceptor.Drain(output, doneC)
		Expect(err).NotTo(HaveOccurred())

		reader := recorder.NewReader(output)

		recordedTickEvent := &rpb.RecordedEvent{
			NodeId:     1,
			Time:       2,
			StateEvent: tickEvent,
		}

		se, err := reader.ReadEvent()
		Expect(err).NotTo(HaveOccurred())
		Expect(proto.Equal(se, recordedTickEvent)).To(BeTrue())

		se, err = reader.ReadEvent()
		Expect(err).NotTo(HaveOccurred())
		Expect(proto.Equal(se, recordedTickEvent)).To(BeTrue())

		_, err = reader.ReadEvent()
		Expect(err).To(Equal(io.EOF))
	})

	When("the input proto is malformed", func() {
		BeforeEach(func() {
			interceptor.Intercept(&pb.StateEvent{
				Type: &pb.StateEvent_Tick{},
			})
		})

		It("returns an error", func() {
			close(doneC)
			err := interceptor.Drain(output, doneC)
			Expect(err).To(MatchError("error serializing to stream: could not marshal: proto: oneof field has nil value"))

		})
	})

	When("the output is truncated", func() {
		BeforeEach(func() {
			close(doneC)
			err := interceptor.Drain(output, doneC)
			Expect(err).NotTo(HaveOccurred())
			output.Truncate(2)
		})

		It("reading returns an error", func() {
			reader := recorder.NewReader(output)

			_, err := reader.ReadEvent()
			Expect(err).To(MatchError("error reading event: could not read message: EOF"))
		})
	})
})
