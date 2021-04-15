/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package eventlog_test

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"

	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/pb/recording"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
)

var tickEvent = &state.Event{
	Type: &state.Event_TickElapsed{
		TickElapsed: &state.EventTickElapsed{},
	},
}

var _ = Describe("Recorder", func() {
	var (
		output *bytes.Buffer
	)

	BeforeEach(func() {
		output = &bytes.Buffer{}
	})

	It("intercepts and writes state events", func() {
		interceptor := eventlog.NewRecorder(
			1,
			output,
			eventlog.TimeSourceOpt(func() int64 { return 2 }),
			eventlog.BufferSizeOpt(3),
		)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
		Expect(output.Len()).To(Equal(46))
	})

	// TODO, add tests with write failures, write blocking, etc. generate mock
})

var _ = Describe("Reader", func() {

	var (
		output *bytes.Buffer
	)

	BeforeEach(func() {
		output = &bytes.Buffer{}
		interceptor := eventlog.NewRecorder(
			1,
			output,
			eventlog.TimeSourceOpt(func() int64 { return 2 }),
		)
		interceptor.Intercept(tickEvent)
		interceptor.Intercept(tickEvent)
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("can be read back with a Reader", func() {
		reader, err := eventlog.NewReader(output)
		Expect(err).NotTo(HaveOccurred())

		recordedTickEvent := &recording.Event{
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
			_, err := eventlog.NewReader(output)
			Expect(err).To(MatchError("could not read source as a gzip stream: unexpected EOF"))
		})
	})
})
