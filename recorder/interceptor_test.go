package recorder_test

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"

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
		output *bytes.Buffer
	)

	BeforeEach(func() {
		output = &bytes.Buffer{}
	})

	It("intercepts and writes state events", func() {
		interceptor := recorder.NewInterceptor(
			1,
			output,
			recorder.TimeSourceOpt(func() int64 { return 2 }),
			recorder.BufferSizeOpt(3),
		)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
		Expect(output.Len()).To(Equal(35))
	})

	// TODO, add tests with write failures, write blocking, etc. generate mock
})

var _ = Describe("Reader", func() {

	var (
		output *bytes.Buffer
	)

	BeforeEach(func() {
		output = &bytes.Buffer{}
		interceptor := recorder.NewInterceptor(
			1,
			output,
			recorder.TimeSourceOpt(func() int64 { return 2 }),
		)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("can be read back with a Reader", func() {
		reader, err := recorder.NewReader(output)
		Expect(err).NotTo(HaveOccurred())

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

	When("the output is truncated", func() {
		BeforeEach(func() {
			output.Truncate(2)
		})

		It("reading returns an error", func() {
			_, err := recorder.NewReader(output)
			Expect(err).To(MatchError("could not read source as a gzip stream: unexpected EOF"))
		})
	})
})
