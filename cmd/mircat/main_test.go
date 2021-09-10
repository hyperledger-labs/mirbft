package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/hyperledger-labs/mirbft/pkg/testengine"
)

var _ = Describe("Parsing", func() {
	var (
		logBytes *bytes.Buffer
	)

	BeforeEach(func() {
		logBytes = &bytes.Buffer{}
		gzWriter := gzip.NewWriter(logBytes)
		defer gzWriter.Close()

		recorder := (&legacy_testengine.Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
		}).Recorder()

		recording, err := recorder.Recording(gzWriter)
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
			"--statusIndex", "301",
			"--statusIndex", "305",
			"--verboseText",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(args.input).NotTo(BeNil())
		Expect(args.input.Close()).NotTo(HaveOccurred())
		Expect(args.interactive).To(BeTrue())
		Expect(args.nodeIDs).To(Equal([]uint64{1, 2}))
		Expect(args.eventTypes).To(Equal([]string{"Step", "Tick"}))
		Expect(args.statusIndices).To(Equal([]uint64{301, 305}))
		Expect(args.verboseText).To(BeTrue())
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

	When("status indexes are specified, but interactive is not", func() {
		It("returns an error", func() {
			_, err := parseArgs([]string{
				"--statusIndex", "7",
			})
			Expect(err).To(MatchError("cannot set status indices for non-interactive playback"))
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
		gzWriter := gzip.NewWriter(logBytes)
		defer gzWriter.Close()

		recorder := (&legacy_testengine.Spec{
			NodeCount:     4,
			ClientCount:   4,
			ReqsPerClient: 20,
		}).Recorder()

		recording, err := recorder.Recording(gzWriter)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5000)
		Expect(err).NotTo(HaveOccurred())

		args = &arguments{
			input:       ioutil.NopCloser(logBytes),
			nodeIDs:     []uint64{0, 2},
			eventTypes:  []string{"Initialize", "CompleteInitialization"},
			stepTypes:   []string{"Checkpoint", "NewEpoch"},
			interactive: true,
		}
	})

	It("reads from the source", func() {
		err := args.execute(output)
		Expect(err).NotTo(HaveOccurred())
		Expect(output.String()).To(ContainSubstring("1 [node_id=0 time=10 state_event=[initialize=[id=0 batch_size=1 heartbeat_ticks=2 suspect_ticks=4 new_epoch_timeout_ticks=8 buffer_size=5242880]]]"))
		Expect(output.String()).To(ContainSubstring("4 [node_id=0 time=10 state_event=[complete_initialization=[]]]"))
	})
})
