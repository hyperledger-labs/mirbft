package main

import (
	"bytes"
	"compress/gzip"
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
		gzWriter := gzip.NewWriter(logBytes)
		defer gzWriter.Close()

		recorder := testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

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
			"--verboseText",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(args.input).NotTo(BeNil())
		Expect(args.input.Close()).NotTo(HaveOccurred())
		Expect(args.interactive).To(BeTrue())
		Expect(args.nodeIDs).To(Equal([]uint64{1, 2}))
		Expect(args.eventTypes).To(Equal([]string{"Step", "Tick"}))
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

		recorder := testengine.BasicRecorder(4, 4, 20)
		recorder.NetworkState.Config.MaxEpochLength = 200000 // XXX this works around a bug in the library for now

		recording, err := recorder.Recording(gzWriter)
		Expect(err).NotTo(HaveOccurred())

		_, err = recording.DrainClients(5000)
		Expect(err).NotTo(HaveOccurred())

		args = &arguments{
			input:       ioutil.NopCloser(logBytes),
			nodeIDs:     []uint64{0, 2},
			eventTypes:  []string{"Step", "Initialize"},
			stepTypes:   []string{"Checkpoint", "NewEpoch"},
			interactive: true,
		}
	})

	It("reads from the source", func() {
		err := args.execute(output)
		Expect(err).NotTo(HaveOccurred())
		Expect(output.String()).To(Equal("[node_id=0 time=0 state_event=[initialize=[id=0 batch_size=1 heartbeat_ticks=2 suspect_ticks=4 new_epoch_timeout_ticks=8 buffer_size=5000]]]\n" +
			"[node_id=2 time=0 state_event=[initialize=[id=2 batch_size=1 heartbeat_ticks=2 suspect_ticks=4 new_epoch_timeout_ticks=8 buffer_size=5000]]]\n" +
			"[node_id=0 time=2420 state_event=[step=[source=1 msg=[new_epoch=[new_config=[config=[number=1 leaders=0 leaders=1 leaders=2 leaders=3 planned_expiration=200000] starting_checkpoint=[seq_no=0 value=66616b65]] epoch_changes=[node_id=0 digest=9a53f69f] epoch_changes=[node_id=2 digest=9a53f69f] epoch_changes=[node_id=3 digest=9a53f69f]]]]]]\n" +
			"[node_id=2 time=2420 state_event=[step=[source=1 msg=[new_epoch=[new_config=[config=[number=1 leaders=0 leaders=1 leaders=2 leaders=3 planned_expiration=200000] starting_checkpoint=[seq_no=0 value=66616b65]] epoch_changes=[node_id=0 digest=9a53f69f] epoch_changes=[node_id=2 digest=9a53f69f] epoch_changes=[node_id=3 digest=9a53f69f]]]]]]\n" +
			"[node_id=0 time=3220 state_event=[step=[source=0 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=0 time=3220 state_event=[step=[source=0 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=2 time=3220 state_event=[step=[source=2 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=2 time=3220 state_event=[step=[source=2 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=0 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=0 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=1 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=1 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=1 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=1 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=2 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=2 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=3 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=3 msg=[checkpoint=[seq_no=20 value=7249e65e]]]]]\n" +
			"[node_id=0 time=3320 state_event=[step=[source=3 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n" +
			"[node_id=2 time=3320 state_event=[step=[source=3 msg=[checkpoint=[seq_no=40 value=042fcea9]]]]]\n",
		))
	})
})
