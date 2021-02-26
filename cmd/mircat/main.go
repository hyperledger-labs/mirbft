/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// mircat is a package for reviewing Mir state machine recordings.
// It understands the format encoded via github.com/IBM/mirbft/eventlog
// and is able to parse and filter these log files.  It is also able to
// play them against an identical version of the state machine for problem
// reproduction and debugging.
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sort"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/IBM/mirbft/pkg/eventlog"
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/recording"
	"github.com/IBM/mirbft/pkg/pb/state"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"
)

// command line flags
var (
	allEventTypes = []string{
		"Initialize",
		"LoadEntry",
		"CompleteInitialization",
		"Tick",
		"Step",
		"AddResults",
		"ActionsReceived",
		"ClientActionsReceived",
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

func excludedByNodeID(re *recording.Event, nodeIDs []uint64) bool {
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
	logLevel      statemachine.LogLevel
	nodeIDs       []uint64
	eventTypes    []string
	notEventTypes []string
	stepTypes     []string
	notStepTypes  []string
	statusIndices []uint64
	verboseText   bool
}

type namedLogger struct {
	level  statemachine.LogLevel
	name   string
	output io.Writer
}

func (nl namedLogger) Log(level statemachine.LogLevel, msg string, args ...interface{}) {
	if level < nl.level {
		return
	}

	fmt.Fprint(nl.output, nl.name)
	fmt.Fprint(nl.output, ": ")
	fmt.Fprint(nl.output, msg)
	for i := 0; i < len(args); i++ {
		if i+1 < len(args) {
			switch args[i+1].(type) {
			case []byte:
				fmt.Fprintf(nl.output, " %s=%x", args[i], args[i+1])
			default:
				fmt.Fprintf(nl.output, " %s=%v", args[i], args[i+1])
			}
			i++
		} else {
			fmt.Fprintf(nl.output, " %s=%%MISSING%%", args[i])
		}
	}
	fmt.Fprintf(nl.output, "\n")
}

type stateMachines struct {
	logLevel statemachine.LogLevel
	nodes    map[uint64]*stateMachine
	output   io.Writer
}

type stateMachine struct {
	machine        *statemachine.StateMachine
	pendingActions *statemachine.ActionList
	executionTime  time.Duration
}

func newStateMachines(output io.Writer, logLevel statemachine.LogLevel) *stateMachines {
	return &stateMachines{
		output:   output,
		logLevel: logLevel,
		nodes:    map[uint64]*stateMachine{},
	}
}

func (s *stateMachines) apply(event *recording.Event) (result *statemachine.ActionList, err error) {
	var node *stateMachine

	if _, ok := event.StateEvent.Type.(*state.Event_Initialize); ok {
		delete(s.nodes, event.NodeId)
		node = &stateMachine{
			machine: &statemachine.StateMachine{
				Logger: namedLogger{
					name:   fmt.Sprintf("node%d", event.NodeId),
					output: s.output,
					level:  s.logLevel,
				},
			},
			pendingActions: &statemachine.ActionList{},
		}
		s.nodes[event.NodeId] = node
	} else {
		var ok bool
		node, ok = s.nodes[event.NodeId]
		if !ok {
			return nil, errors.Errorf("malformed log: node %d attempted to apply event of type %T without initializing first.", event.NodeId, event.StateEvent.Type)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("node %d panic-ed while applying state event to state machine:\n\n%s\n\n%s", event.NodeId, r, debug.Stack())
		}
	}()

	start := time.Now()
	actions := node.machine.ApplyEvent(event.StateEvent)
	node.executionTime += time.Since(start)
	node.pendingActions.PushBackList(actions)
	if err != nil {
		return nil, err
	}

	switch event.StateEvent.Type.(type) {
	case *state.Event_ActionsReceived:
		result := node.pendingActions
		node.pendingActions = &statemachine.ActionList{}
		return result, nil
	default:
		return nil, nil
	}
}

func (s *stateMachines) status(event *recording.Event) *status.StateMachine {
	node := s.nodes[event.NodeId]
	return node.machine.Status()
}

func (a *arguments) shouldPrint(event *recording.Event) bool {
	var eventTypeText string
	switch event.StateEvent.Type.(type) {
	case *state.Event_Initialize:
		eventTypeText = "Initialize"
	case *state.Event_LoadPersistedEntry:
		eventTypeText = "LoadPersistedEntry"
	case *state.Event_CompleteInitialization:
		eventTypeText = "CompleteInitialization"
	case *state.Event_TickElapsed:
		eventTypeText = "TickElapsed"
	case *state.Event_HashResult:
		eventTypeText = "HashResult"
	case *state.Event_CheckpointResult:
		eventTypeText = "CheckpointResult"
	case *state.Event_RequestPersisted:
		eventTypeText = "RequestPersisted"
	case *state.Event_ActionsReceived:
		eventTypeText = "ActionsReceived"
	case *state.Event_Step:
		eventTypeText = "Step"
	case *state.Event_StateTransferComplete:
		eventTypeText = "StateTransferComplete"
	case *state.Event_StateTransferFailed:
		eventTypeText = "StateTransferFailed"
	default:
		panic(fmt.Sprintf("Unknown event type '%T'", event.StateEvent.Type))
	}

	if excludeByType(eventTypeText, a.eventTypes, a.notEventTypes) {
		return false
	}

	switch et := event.StateEvent.Type.(type) {
	case *state.Event_Initialize:
	case *state.Event_LoadPersistedEntry:
	case *state.Event_CompleteInitialization:
	case *state.Event_TickElapsed:
	case *state.Event_HashResult:
	case *state.Event_CheckpointResult:
	case *state.Event_RequestPersisted:
	case *state.Event_ActionsReceived:
	case *state.Event_Step:
		var stepTypeText string
		switch et.Step.Msg.Type.(type) {
		case *msgs.Msg_Preprepare:
			stepTypeText = "Preprepare"
		case *msgs.Msg_Prepare:
			stepTypeText = "Prepare"
		case *msgs.Msg_Commit:
			stepTypeText = "Commit"
		case *msgs.Msg_Checkpoint:
			stepTypeText = "Checkpoint"
		case *msgs.Msg_Suspect:
			stepTypeText = "Suspect"
		case *msgs.Msg_EpochChange:
			stepTypeText = "EpochChange"
		case *msgs.Msg_EpochChangeAck:
			stepTypeText = "EpochChangeAck"
		case *msgs.Msg_NewEpoch:
			stepTypeText = "NewEpoch"
		case *msgs.Msg_NewEpochEcho:
			stepTypeText = "NewEpochEcho"
		case *msgs.Msg_NewEpochReady:
			stepTypeText = "NewEpochReady"
		case *msgs.Msg_FetchBatch:
			stepTypeText = "FetchBatch"
		case *msgs.Msg_ForwardBatch:
			stepTypeText = "ForwardBatch"
		case *msgs.Msg_FetchRequest:
			stepTypeText = "FetchRequest"
		case *msgs.Msg_ForwardRequest:
			stepTypeText = "ForwardRequest"
		case *msgs.Msg_RequestAck:
			stepTypeText = "RequestAck"
		default:
			panic("unknown message type")
		}
		if excludeByType(stepTypeText, a.stepTypes, a.notStepTypes) {
			return false
		}
	case *state.Event_StateTransferComplete:
		eventTypeText = "StateTransferComplete"
	case *state.Event_StateTransferFailed:
		eventTypeText = "StateTransferFailed"
	default:
		panic(fmt.Sprintf("Unknown event type '%T'", event.StateEvent.Type))
	}

	return true
}

func (a *arguments) execute(output io.Writer) error {
	defer a.input.Close()

	s := newStateMachines(output, a.logLevel)

	reader, err := eventlog.NewReader(a.input)
	if err != nil {
		return errors.WithMessage(err, "bad input file")
	}

	statusIndices := map[uint64]struct{}{}
	for _, index := range a.statusIndices {
		statusIndices[index] = struct{}{}
	}

	index := uint64(0)
	for {
		event, err := reader.ReadEvent()
		if err != nil {
			if err == io.EOF {
				break
			}

			return errors.WithMessage(err, "failed reading input")
		}

		index++

		if excludedByNodeID(event, a.nodeIDs) {
			continue
		}

		_, statusIndex := statusIndices[index]

		// We always print the event if the status index matches,
		// otherwise the output could be quite confusing
		if statusIndex || a.shouldPrint(event) {
			text, err := textFormat(event, !a.verboseText)
			if err != nil {
				return errors.WithMessage(err, "could not marshal event")
			}

			fmt.Fprintf(output, "% 6d %s\n", index, string(text))
		}

		if a.interactive {
			actions, err := s.apply(event)
			if err != nil {
				return err
			}

			if actions != nil {
				iter := actions.Iterator()
				for action := iter.Next(); action != nil; action = iter.Next() {
					text, err := textFormat(action, !a.verboseText)
					if err != nil {
						return errors.WithMessage(err, "could not marshal actions")
					}
					fmt.Fprintf(output, "       actions: %s\n", string(text))
				}
			}

			// note, config options enforce that is statusIndex is set, so is interactive
			if statusIndex {
				fmt.Fprint(output, s.status(event).Pretty())
				fmt.Fprint(output, "\n")
			}
		}
	}

	if a.interactive {
		nodeIDs := a.nodeIDs
		if nodeIDs == nil {
			for id := range s.nodes {
				nodeIDs = append(nodeIDs, id)
			}
			sort.Slice(nodeIDs, func(i, j int) bool {
				return nodeIDs[i] < nodeIDs[j]
			})
		}

		for _, nodeID := range nodeIDs {
			fmt.Fprintf(output, "Node %d successfully completed execution in %v\n", nodeID, s.nodes[nodeID].executionTime)
		}
	}

	return nil
}

func parseArgs(args []string) (*arguments, error) {
	app := kingpin.New("mircat", "Utility for processing Mir state event logs.")
	input := app.Flag("input", "The input file to read (defaults to stdin).").Default(os.Stdin.Name()).File()
	interactive := app.Flag("interactive", "Whether to apply this log to a Mir state machine.").Default("false").Bool()
	nodeIDs := app.Flag("nodeID", "Report events from this nodeID only (useful for interleaved logs), may be repeated").Uint64List()
	eventTypes := app.Flag("eventType", "Which event types to report.").Enums(allEventTypes...)
	notEventTypes := app.Flag("notEventType", "Which eventtypes to exclude. (Cannot combine with --eventTypes)").Enums(allEventTypes...)
	stepTypes := app.Flag("stepType", "Which step message types to report.").Enums(allMsgTypes...)
	notStepTypes := app.Flag("notStepType", "Which step message types to exclude. (Cannot combine with --stepTypes)").Enums(allMsgTypes...)
	verboseText := app.Flag("verboseText", "Whether to be verbose (output full bytes) in the text frmatting.").Default("false").Bool()
	statusIndices := app.Flag("statusIndex", "Print node status at given index in the log (repeatable).").Uint64List()
	logLevel := app.Flag("logLevel", "When run in interactive mode, the log level for the state machine with which to output.").Enum("debug", "info", "warn", "error")

	_, err := app.Parse(args)
	if err != nil {
		return nil, err
	}

	switch {
	case *eventTypes != nil && *notEventTypes != nil:
		return nil, errors.Errorf("cannot set both --eventType and --notEventType")
	case *stepTypes != nil && *notStepTypes != nil:
		return nil, errors.Errorf("cannot set both --stepType and --notStepType")
	case *statusIndices != nil && !*interactive:
		return nil, errors.Errorf("cannot set status indices for non-interactive playback")
	case *logLevel != "" && !*interactive:
		return nil, errors.Errorf("cannot set logLevel for non-interactive playback")
	}

	mirLogLevel := statemachine.LevelInfo

	switch *logLevel {
	case "debug":
		mirLogLevel = statemachine.LevelDebug
	case "info":
		mirLogLevel = statemachine.LevelInfo
	case "warn":
		mirLogLevel = statemachine.LevelWarn
	case "error":
		mirLogLevel = statemachine.LevelError
	}

	return &arguments{
		input:         *input,
		interactive:   *interactive,
		nodeIDs:       *nodeIDs,
		eventTypes:    *eventTypes,
		logLevel:      mirLogLevel,
		notEventTypes: *notEventTypes,
		stepTypes:     *stepTypes,
		notStepTypes:  *notStepTypes,
		verboseText:   *verboseText,
		statusIndices: *statusIndices,
	}, nil
}

func main() {
	kingpin.Version("0.0.1")
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("failed to parse arguments, %s, try --help", err)
	}
	err = args.execute(os.Stdout)
	if err != nil {
		fmt.Println("")
		kingpin.Fatalf("%s", err)
	}
}
