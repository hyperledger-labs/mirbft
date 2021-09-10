/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: Properly comment all the code in this file.

package eventlog_test

import (
	"bytes"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/proto"

	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/pb/recording"
)

var tickEvent = events.Tick()

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
		interceptor.Intercept((&events.EventList{}).PushBack(tickEvent))
		interceptor.Intercept((&events.EventList{}).PushBack(tickEvent))
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
		interceptor.Intercept((&events.EventList{}).PushBack(tickEvent))
		interceptor.Intercept((&events.EventList{}).PushBack(tickEvent))
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("can be read back with a Reader", func() {
		reader, err := eventlog.NewReader(output)
		Expect(err).NotTo(HaveOccurred())

		recordedTickEvent := &recording.Entry{
			NodeId: 1,
			Time:   2,
			Events: []*eventpb.Event{tickEvent},
		}

		se, err := reader.ReadEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(proto.Equal(se, recordedTickEvent)).To(BeTrue())

		se, err = reader.ReadEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(proto.Equal(se, recordedTickEvent)).To(BeTrue())

		_, err = reader.ReadEntry()
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
