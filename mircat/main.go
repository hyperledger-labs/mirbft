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

func excludedAsMessage(se *pb.StateEvent_Step, matchedMsgs, excludedMsgs []string) bool {
	switch se.Step.Msg.Type.(type) {
	case *pb.Msg_Preprepare:
		return excludeByType("Preprepare", matchedMsgs, excludedMsgs)
	case *pb.Msg_Prepare:
		return excludeByType("Prepare", matchedMsgs, excludedMsgs)
	case *pb.Msg_Commit:
		return excludeByType("Commit", matchedMsgs, excludedMsgs)
	case *pb.Msg_Checkpoint:
		return excludeByType("Checkpoint", matchedMsgs, excludedMsgs)
	case *pb.Msg_Suspect:
		return excludeByType("Suspect", matchedMsgs, excludedMsgs)
	case *pb.Msg_EpochChange:
		return excludeByType("EpochChange", matchedMsgs, excludedMsgs)
	case *pb.Msg_EpochChangeAck:
		return excludeByType("EpochChangeAck", matchedMsgs, excludedMsgs)
	case *pb.Msg_NewEpoch:
		return excludeByType("NewEpoch", matchedMsgs, excludedMsgs)
	case *pb.Msg_NewEpochEcho:
		return excludeByType("NewEpochEcho", matchedMsgs, excludedMsgs)
	case *pb.Msg_NewEpochReady:
		return excludeByType("NewEpochReady", matchedMsgs, excludedMsgs)
	case *pb.Msg_FetchBatch:
		return excludeByType("FetchBatch", matchedMsgs, excludedMsgs)
	case *pb.Msg_ForwardBatch:
		return excludeByType("ForwardBatch", matchedMsgs, excludedMsgs)
	case *pb.Msg_FetchRequest:
		return excludeByType("FetchRequest", matchedMsgs, excludedMsgs)
	case *pb.Msg_ForwardRequest:
		return excludeByType("ForwardRequest", matchedMsgs, excludedMsgs)
	case *pb.Msg_RequestAck:
		return excludeByType("RequestAck", matchedMsgs, excludedMsgs)
	default:
		panic("unknown message type")
	}
}

type arguments struct {
	input         *os.File
	interactive   bool
	nodeIDs       []uint64
	eventTypes    []string
	notEventTypes []string
	stepTypes     []string
	notStepTypes  []string
}

func (a *arguments) execute() error {
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

		var exclude bool
		switch et := event.StateEvent.Type.(type) {
		case *pb.StateEvent_Initialize:
			exclude = excludeByType("Initialize", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_LoadEntry:
			exclude = excludeByType("LoadEntry", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_CompleteInitialization:
			exclude = excludeByType("CompleteInitialization", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_Tick:
			exclude = excludeByType("Tick", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_Propose:
			exclude = excludeByType("Propose", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_AddResults:
			exclude = excludeByType("AddResults", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_ActionsReceived:
			exclude = excludeByType("ActionsReceived", a.eventTypes, a.notEventTypes)
		case *pb.StateEvent_Step:
			exclude = excludedAsMessage(et, a.stepTypes, a.notStepTypes)
		default:
			panic("Unknown event type")
		}

		if exclude {
			continue
		}
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

	_, err := app.Parse(args)
	if err != nil {
		return nil, err
	}

	switch {
	case *eventTypes != nil && *notEventTypes != nil:
		return nil, errors.Errorf("Cannot set both --eventTypes and --notEventTypes")
	case *stepTypes != nil && *notStepTypes != nil:
		return nil, errors.Errorf("Cannot set both --stepTypes and --notStepTypes")
	}

	return &arguments{
		input:         *input,
		interactive:   *interactive,
		nodeIDs:       *nodeIDs,
		eventTypes:    *eventTypes,
		notEventTypes: *notEventTypes,
		stepTypes:     *stepTypes,
		notStepTypes:  *notStepTypes,
	}, nil
}

func main() {
	kingpin.Version("0.0.1")
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("%s, try --help", err)
	}
	args.execute()
}
