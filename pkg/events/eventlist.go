/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package events

import (
	"container/list"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
)

// EventList represents a list of Events, e.g. as produced by a module.
type EventList struct {

	// The internal list is intentionally left uninitialized until its actual use.
	// This probably speeds up appending empty lists to other lists.
	list *list.List
}

// Len returns the number of events in the EventList.
func (el *EventList) Len() int {
	if el.list == nil {
		return 0
	}
	return el.list.Len()
}

// PushBack appends an event to the end of the list.
// Returns the EventList itself, for the convenience of chaining multiple calls to PushBack.
func (el *EventList) PushBack(event *eventpb.Event) *EventList {
	if el.list == nil {
		el.list = list.New()
	}

	el.list.PushBack(event)
	return el
}

// PushBackList appends all events in newEvents to the end of the current EventList.
func (el *EventList) PushBackList(newEvents *EventList) *EventList {
	if newEvents.list != nil {
		if el.list == nil {
			el.list = list.New()
		}
		// TODO: Check out possible inefficiency. This implementation actually (shallowly) copies the elements from
		//       newEvents.list to el.list. Most of the time it would be enough to simply wire together the two lists.
		//       This would probably require a custom linked list implementation, since list.List does not seem to
		//       support it.
		el.list.PushBackList(newEvents.list)
	}

	return el
}

// Slice returns a slice representation of the current state of the list.
// The returned slice only contains pointers to the events in this list, no deep copying is performed.
// Any modifications performed on the events will affect the contents of both the EventList and the returned slice.
func (el *EventList) Slice() []*eventpb.Event {

	// Create empty result slice.
	events := make([]*eventpb.Event, 0, el.Len())

	// Populate result slice by appending events one by one.
	iter := el.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		events = append(events, event)
	}

	// Return populated result slice.
	return events
}

// StripFollowUps removes all follow-up Events from the Events in the list and returns them.
// Note that StripFollowUps modifies the events in the list by calling Strip on each event in the EventList.
// After StripFollowUps returns, all events in the list will have no follow-ups.
// StripFollowUps accumulates all those follow-up Events in a new EventList that it returns.
func (el *EventList) StripFollowUps() *EventList {
	// Create list of follow-up Events.
	followUps := EventList{}

	// Populate list by follow-up events
	iter := el.Iterator()
	for event := iter.Next(); event != nil; event = iter.Next() {
		followUps.PushBackList(Strip(event))
	}

	// Return populated list of follow-up events.
	return &followUps
}

// Iterator returns a pointer to an EventListIterator object used to iterate over the events in this list,
// starting from the beginning of the list.
func (el *EventList) Iterator() *EventListIterator {
	if el.list == nil {
		return &EventListIterator{}
	}

	return &EventListIterator{
		currentElement: el.list.Front(),
	}
}

// EventListIterator is an object returned from EventList.Iterator
// used to iterate over the elements (Events) of an EventList using the iterator's Next method.
type EventListIterator struct {
	currentElement *list.Element
}

// Next will return the next Event until the end of the associated EventList is encountered.
// Thereafter, it will return nil.
func (eli *EventListIterator) Next() *eventpb.Event {

	// Return nil if list has been exhausted.
	if eli.currentElement == nil {
		return nil
	}

	// Obtain current element and move on to the next one.
	result := eli.currentElement.Value.(*eventpb.Event)
	eli.currentElement = eli.currentElement.Next()

	// Return current element.
	return result
}
