package recorder_test

import (
	"bytes"
	"io"
	"sync"

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
		interceptor *recorder.Interceptor
		output      *bytes.Buffer
		mutex       *sync.Mutex // Artificially help out the race detector
	)

	BeforeEach(func() {
		mutex = &sync.Mutex{}
		output = &bytes.Buffer{}

		interceptor = recorder.NewInterceptor(1, func() int64 { return 2 }, 3)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)
	})

	AfterEach(func() {
		mutex.Lock()
		defer mutex.Unlock()
	})

	It("intercepts and writes state events", func() {
		goDone := make(chan struct{})
		go func() {
			mutex.Lock()
			defer mutex.Unlock()
			err := interceptor.Drain(output)
			Expect(err).NotTo(HaveOccurred())
			close(goDone)
		}()

		interceptor.Stop()
		Eventually(goDone).Should(BeClosed())

		Expect(output.Len()).To(Equal(35))
	})

	It("blocks when the buffer space is exceeded", func() {
		goDone := make(chan struct{})
		go func() {
			mutex.Lock()
			defer mutex.Unlock()
			interceptor.Intercept(tickEvent)
			interceptor.Intercept(tickEvent)
			close(goDone)
		}()

		Consistently(goDone).ShouldNot(BeClosed())
		go interceptor.Drain(output)
		interceptor.Stop()
		Eventually(goDone).Should(BeClosed())
	})

	It("writes everything in the buffer even when Stop has been called", func() {
		interceptor.Intercept(tickEvent)
		go interceptor.Stop()
		err := interceptor.Drain(output)
		Expect(err).NotTo(HaveOccurred())
		Expect(output.Len()).To(Equal(35))
	})

	It("can be read back with a Reader", func() {
		go interceptor.Stop()
		err := interceptor.Drain(output)
		Expect(err).NotTo(HaveOccurred())

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
			go interceptor.Stop()
			err := interceptor.Drain(output)
			Expect(err).NotTo(HaveOccurred())
			output.Truncate(2)
		})

		It("reading returns an error", func() {
			_, err := recorder.NewReader(output)
			Expect(err).To(MatchError("could not read source as a gzip stream: unexpected EOF"))
		})
	})
})
