package main

import (
	"bytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/testengine"
)

var _ = Describe("Parsing", func() {
	var (
		logBytes *bytes.Buffer
	)

	BeforeEach(func() {
		logBytes = &bytes.Buffer{}

		recorder := testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		recording, err := recorder.Recording(logBytes)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5000)
		Expect(err).NotTo(HaveOccurred())
	})

	It("parses a fully populated command line", func() {
		args, err := parseArgs([]string{
			"--input", "main.go",
			"--interactive",
			"--nodeID", "1",
			"--nodeID", "2",
			"--eventType", "Step",
			"--eventType", "Tick",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(args.input).NotTo(BeNil())
		Expect(args.input.Close()).NotTo(HaveOccurred())
		Expect(args.interactive).To(BeTrue())
		Expect(args.nodeIDs).To(Equal([]uint64{1, 2}))
		Expect(args.eventTypes).To(Equal([]string{"Step", "Tick"}))
	})
})

var _ = Describe("Execution", func() {
	var (
		logBytes *bytes.Buffer
	)

	BeforeEach(func() {
		logBytes = &bytes.Buffer{}

		recorder := testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		recording, err := recorder.Recording(logBytes)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5000)
		Expect(err).NotTo(HaveOccurred())
	})

	It("reads from the source", func() {

	})
})
