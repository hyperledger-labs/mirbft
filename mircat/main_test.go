package main

import (
	"bytes"
	"io/ioutil"

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
			"--stepType", "Preprepare",
			"--stepType", "EpochChange",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(args.input).NotTo(BeNil())
		Expect(args.input.Close()).NotTo(HaveOccurred())
		Expect(args.interactive).To(BeTrue())
		Expect(args.nodeIDs).To(Equal([]uint64{1, 2}))
		Expect(args.eventTypes).To(Equal([]string{"Step", "Tick"}))
	})

	When("both event includes and event excludes are present", func() {
		It("returns an error", func() {
			_, err := parseArgs([]string{
				"--eventType", "Step",
				"--notEventType", "Tick",
			})
			Expect(err).To(MatchError("cannot set both --eventType and --notEventType"))
		})
	})

	When("both event includes and step excludes are present", func() {
		It("returns an error", func() {
			_, err := parseArgs([]string{
				"--stepType", "Preprepare",
				"--notStepType", "Commit",
			})
			Expect(err).To(MatchError("cannot set both --stepType and --notStepType"))
		})
	})
})

var _ = Describe("Execution", func() {
	var (
		logBytes *bytes.Buffer
		output   *bytes.Buffer
		args     *arguments
	)

	BeforeEach(func() {
		logBytes = &bytes.Buffer{}
		output = &bytes.Buffer{}

		recorder := testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		recording, err := recorder.Recording(logBytes)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5000)
		Expect(err).NotTo(HaveOccurred())

		args = &arguments{
			input:      ioutil.NopCloser(logBytes),
			nodeIDs:    []uint64{0, 2},
			eventTypes: []string{"Step", "Initialize"},
			stepTypes:  []string{"Checkpoint", "NewEpoch"},
		}
	})

	It("reads from the source", func() {
		err := args.execute(output)
		Expect(err).NotTo(HaveOccurred())
		// Expect(output.String()).To(Equal(""))
	})
})
