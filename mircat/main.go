/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// mircat is a package for reviewing Mir state machine recordings.
// It understands the format encoded via github.com/IBM/mirbft/recorder
// and is able to parse and filter these log files.  It is also able to
// play them against an identical version of the state machine for problem
// reproduction and debugging.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/recorder"
	rpb "github.com/IBM/mirbft/recorder/recorderpb"
)

// command line flags
var (
	allEventTypes = []string{
		"Initialize",
		"LoadEntry",
		"CompleteInitialization",
		"Tick",
		"Step",
		"Propose",
		"AddResults",
		"ActionsReceived",
	}

	allMsgTypes = []string{
		"Preprepare",
		"Prepare",
		"Commit",
		"Checkpoint",
		"EpochChange",
		"EpochChangeAck",
		"Suspect",
		"NewEpoch",
		"NewEpochEcho",
		"NewEpochReady",
		"FetchBatch",
		"ForwardBatch",
		"FetchRequest",
		"RequestAck",
		"ForwardRequest",
	}
)

// excludeByType is used both for --stepTypes/--notStepTypes and
// for --eventTypes/--notEventTypes.  The assumption is that at least
// one of include or exclude is nil.
func excludeByType(value string, include []string, exclude []string) bool {
	if include != nil {
		for _, includeName := range include {
			if includeName == value {
				return false
			}
		}

		return true
	}

	for _, excludeName := range exclude {
		if excludeName == value {
			return true
		}
	}

	return false
}

func excludedByNodeID(re *rpb.RecordedEvent, nodeIDs []uint64) bool {
	if nodeIDs == nil {
		return false
	}

	for _, nodeID := range nodeIDs {
		if nodeID == re.NodeId {
			return false
		}
	}

	return true
}

type arguments struct {
	input         io.ReadCloser
	interactive   bool
	nodeIDs       []uint64
	eventTypes    []string
	notEventTypes []string
	stepTypes     []string
	notStepTypes  []string
	verboseText   bool
}

func (a *arguments) execute(output io.Writer) error {
	defer a.input.Close()

	reader := recorder.NewReader(a.input)
	for {
		event, err := reader.ReadEvent()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return errors.WithMessage(err, "failed reading input")
		}

		if excludedByNodeID(event, a.nodeIDs) {
			continue
		}

		var eventTypeText string
		switch event.StateEvent.Type.(type) {
		case *pb.StateEvent_Initialize:
			eventTypeText = "Initialize"
		case *pb.StateEvent_LoadEntry:
			eventTypeText = "LoadEntry"
		case *pb.StateEvent_CompleteInitialization:
			eventTypeText = "CompleteInitialization"
		case *pb.StateEvent_Tick:
			eventTypeText = "Tick"
		case *pb.StateEvent_Propose:
			eventTypeText = "Propose"
		case *pb.StateEvent_AddResults:
			eventTypeText = "AddResults"
		case *pb.StateEvent_ActionsReceived:
			eventTypeText = "ActionsReceived"
		case *pb.StateEvent_Step:
			eventTypeText = "Step"
		default:
			panic("Unknown event type")
		}

		if excludeByType(eventTypeText, a.eventTypes, a.notEventTypes) {
			continue
		}

		switch et := event.StateEvent.Type.(type) {
		case *pb.StateEvent_Initialize:
		case *pb.StateEvent_LoadEntry:
		case *pb.StateEvent_CompleteInitialization:
		case *pb.StateEvent_Tick:
		case *pb.StateEvent_Propose:
		case *pb.StateEvent_AddResults:
		case *pb.StateEvent_ActionsReceived:
		case *pb.StateEvent_Step:
			var stepTypeText string
			switch et.Step.Msg.Type.(type) {
			case *pb.Msg_Preprepare:
				stepTypeText = "Preprepare"
			case *pb.Msg_Prepare:
				stepTypeText = "Prepare"
			case *pb.Msg_Commit:
				stepTypeText = "Commit"
			case *pb.Msg_Checkpoint:
				stepTypeText = "Checkpoint"
			case *pb.Msg_Suspect:
				stepTypeText = "Suspect"
			case *pb.Msg_EpochChange:
				stepTypeText = "EpochChange"
			case *pb.Msg_EpochChangeAck:
				stepTypeText = "EpochChangeAck"
			case *pb.Msg_NewEpoch:
				stepTypeText = "NewEpoch"
			case *pb.Msg_NewEpochEcho:
				stepTypeText = "NewEpochEcho"
			case *pb.Msg_NewEpochReady:
				stepTypeText = "NewEpochReady"
			case *pb.Msg_FetchBatch:
				stepTypeText = "FetchBatch"
			case *pb.Msg_ForwardBatch:
				stepTypeText = "ForwardBatch"
			case *pb.Msg_FetchRequest:
				stepTypeText = "FetchRequest"
			case *pb.Msg_ForwardRequest:
				stepTypeText = "ForwardRequest"
			case *pb.Msg_RequestAck:
				stepTypeText = "RequestAck"
			default:
				panic("unknown message type")
			}
			if excludeByType(stepTypeText, a.stepTypes, a.notStepTypes) {
				continue
			}
		default:
			panic("Unknown event type")
		}

		text, err := textFormat(event, !a.verboseText)
		if err != nil {
			return errors.WithMessage(err, "could not marshal event")
		}
		fmt.Fprintf(output, string(text))
		fmt.Fprintf(output, "\n")
	}
}

func parseArgs(args []string) (*arguments, error) {
	app := kingpin.New("mircat", "Utility for processing Mir recorder logs.")
	input := app.Flag("input", "The input file to read (defaults to stdin).").Default(os.Stdin.Name()).File()
	interactive := app.Flag("interactive", "Whether to apply this log to a Mir state machine.").Default("false").Bool()
	nodeIDs := app.Flag("nodeID", "Report events from this nodeID only (useful for interleaved logs), may be repeated").Uint64List()
	eventTypes := app.Flag("eventType", "Which event types to report.").Enums(allEventTypes...)
	notEventTypes := app.Flag("notEventType", "Which eventtypes to exclude. (Cannot combine with --eventTypes)").Enums(allEventTypes...)
	stepTypes := app.Flag("stepType", "Which step message types to report.").Enums(allMsgTypes...)
	notStepTypes := app.Flag("notStepType", "Which step message types to exclude. (Cannot combine with --stepTypes)").Enums(allMsgTypes...)
	verboseText := app.Flag("verboseText", "Whether to be verbose (output full bytes) in the text frmatting.").Default("false").Bool()

	_, err := app.Parse(args)
	if err != nil {
		return nil, err
	}

	switch {
	case *eventTypes != nil && *notEventTypes != nil:
		return nil, errors.Errorf("cannot set both --eventType and --notEventType")
	case *stepTypes != nil && *notStepTypes != nil:
		return nil, errors.Errorf("cannot set both --stepType and --notStepType")
	}

	return &arguments{
		input:         *input,
		interactive:   *interactive,
		nodeIDs:       *nodeIDs,
		eventTypes:    *eventTypes,
		notEventTypes: *notEventTypes,
		stepTypes:     *stepTypes,
		notStepTypes:  *notStepTypes,
		verboseText:   *verboseText,
	}, nil
}

func main() {
	kingpin.Version("0.0.1")
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("Error, %s, try --help", err)
	}
	args.execute(os.Stdout)
}
