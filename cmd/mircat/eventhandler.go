/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// mircat is a package for reviewing Mir state machine recordings.
// It understands the format encoded via github.com/hyperledger-labs/mirbft/eventlog
// and is able to parse and filter these log files.  It is also able to
// play them against an identical version of the state machine for problem
// reproduction and debugging.
package main

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/eventlog"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"os"
	"reflect"
	"strings"
)

type eventMetadata struct {
	time   int64
	nodeID uint64
	index  uint64
}

//checks if a value is present in a given list
func includedIn(value string, includeList []string) bool {

	for _, includeName := range includeList {
		if includeName == value {
			return true
		}
	}
	return false
}

// extracts events from eventlog entries and
// forwards them for display
func processEvent(args *arguments) error {

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
			displayEvent(event, metadata, args)
			index++
		}
	}
	return nil
}

//displays events based on the user selection
func displayEvent(event *eventpb.Event, metadata eventMetadata, args *arguments) {
	eventType := getEventType(event, false)
	if eventType == "Iss" {
		issEventType := getEventType(event, true)
		if includedIn(issEventType, args.includedIssEvents) { //checks if the given issEventType is selected by the user
			display(fmt.Sprintf("Iss : %s", issEventType), event.String(), metadata)
		}
	} else {
		if includedIn(eventType, args.includedEvents) { //checks if the given eventType is selected by the user
			display(eventType, event.String(), metadata)
		}
	}
}

//returns the list of events present in the given eventlog
func getEventList(file *os.File) (error, []string, []string) {
	var eventList []string
	var issEventList []string

	defer func(file *os.File, offset int64, whence int) {
		_, _ = file.Seek(offset, whence) // resets the file offset for successive reading
	}(file, 0, 0)

	reader, err := eventlog.NewReader(file)
	if err != nil {
		return err, nil, nil
	}
	for entry, err := reader.ReadEntry(); err == nil; entry, err = reader.ReadEntry() {
		for _, event := range entry.Events {
			eventType := getEventType(event, false)
			if eventType == "Iss" {
				issEventType := getEventType(event, true)
				if !includedIn(issEventType, issEventList) { //if event type is not present in the list
					issEventList = append(issEventList, issEventType) // append it to list
				}
			}
			if !includedIn(eventType, eventList) {
				eventList = append(eventList, eventType)
			}
		}
	}
	return nil, eventList, issEventList
}

//returns the type of event as string
func getEventType(event *eventpb.Event, isISS bool) string {
	if isISS {

		return strings.ReplaceAll(
			reflect.TypeOf(event.GetIss().GetType()).Elem().Name(), //gets the type's name i.e. ISSEvent_sb , ISSEvent_PersistCheckpoint,etc
			"ISSEvent_", "") //replaces the given substring from the name
	}
	return strings.ReplaceAll(
		reflect.TypeOf(event.GetType()).Elem().Name(), //gets the type's name i.e. Event_Tick , Event_Iss,etc
		"Event_", "")
}
