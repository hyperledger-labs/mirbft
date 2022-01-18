package main

//handles the processing, display and retrieval of events from a given eventlog file

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/recordingpb"
	"io"
	"os"
	"reflect"
	"strings"
)

type eventMetadata struct {
	time   int64
	nodeID uint64
	index  uint64
}

// extracts events from eventlog entries and
// forwards them for display
func processEvents(args *arguments) error {

	//new reader
	reader, err := eventlog.NewReader(args.srcFile)

	if err != nil {
		return err
	}

	index := uint64(0) // a counter set to track the log indices

	for entry, err := reader.ReadEntry(); err == nil; entry, err = reader.ReadEntry() {
		metadata := eventMetadata{
			nodeID: entry.NodeId,
			time:   entry.Time,
		}
		//getting events from entry
		for _, event := range entry.Events {
			metadata.index = index

			if _, ok := args.includedEvents[eventName(event)]; ok {
				// If event type has been selected for displaying

				switch e := event.Type.(type) {
				case *eventpb.Event_Iss:
					// Only display selected sub-types of the ISS Event
					if _, ok := args.includedIssEvents[issEventName(e.Iss)]; ok {
						displayEvent(event, metadata)
					}
				default:
					displayEvent(event, metadata)
				}
			}

			index++
		}
	}
	return nil
}

// Displays one event according to its type.
func displayEvent(event *eventpb.Event, metadata eventMetadata) {

	switch e := event.Type.(type) {
	case *eventpb.Event_Iss:
		display(fmt.Sprintf("%s : %s", eventName(event), issEventName(e.Iss)), event.String(), metadata)
	default:
		display(eventName(event), event.String(), metadata)
	}
}

// Returns the list of event names present in the given eventlog file,
// along with the total number of events present in the file.
func getEventList(file *os.File) (map[string]struct{}, map[string]struct{}, int, error) {
	events := make(map[string]struct{})
	issEvents := make(map[string]struct{})

	defer func(file *os.File, offset int64, whence int) {
		_, _ = file.Seek(offset, whence) // resets the file offset for successive reading
	}(file, 0, 0)

	reader, err := eventlog.NewReader(file)
	if err != nil {
		return nil, nil, 0, err
	}

	cnt := 0 // Counts the total number of events in the event log.
	var entry *recordingpb.Entry
	for entry, err = reader.ReadEntry(); err == nil; entry, err = reader.ReadEntry() {
		// For each entry of the event log

		for _, event := range entry.Events {
			// For each Event in the entry
			cnt++

			// Add the Event name to the set of known Events.
			events[eventName(event)] = struct{}{}
			switch e := event.Type.(type) {
			case *eventpb.Event_Iss:
				// For ISS Events, also add the type of the ISS event to a set of known ISS events.
				issEvents[issEventName(e.Iss)] = struct{}{}
			}
		}
	}
	if err != io.EOF {
		return nil, nil, cnt, fmt.Errorf("failed reading event log: %w", err)
	}

	return events, issEvents, cnt, nil
}

// eventName returns a string name of an Event.
func eventName(event *eventpb.Event) string {
	return strings.ReplaceAll(
		reflect.TypeOf(event.Type).Elem().Name(), //gets the type's name i.e. Event_Tick , Event_Iss,etc
		"Event_", "")
}

// issEventName returns a string name of an ISS event.
func issEventName(issEvent *isspb.ISSEvent) string {
	return strings.ReplaceAll(
		reflect.TypeOf(issEvent.Type).Elem().Name(), //gets the type's name i.e. ISSEvent_sb , ISSEvent_PersistCheckpoint,etc
		"ISSEvent_", "") //replaces the given substring from the name
}
