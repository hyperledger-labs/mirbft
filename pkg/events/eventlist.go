/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package events

import (
	"container/list"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
)

// EventList represents a list of Events, e.g. as produced by a module.
type EventList struct {
	list *list.List
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

// PushBack appends an event to the end of the list.
// Returns the EventList itself, for the convenience of chaining multiple calls to PushBack.
func (el *EventList) PushBack(event *state.Event) *EventList {
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

// Len returns the number of events in the EventList.
func (el *EventList) Len() int {
	if el.list == nil {
		return 0
	}
	return el.list.Len()
}

// EventListIterator is an object returned from EventList.Iterator
// used to iterate over the elements (Events) of an EventList using the iterator's Next method.
type EventListIterator struct {
	currentElement *list.Element
}

// Next will return the next Event until the end of the associated EventList is encountered.
// Thereafter, it will return nil.
func (eli *EventListIterator) Next() *state.Event {

	// Return nil if list has been exhausted.
	if eli.currentElement == nil {
		return nil
	}

	// Obtain current element and move on to the next one.
	result := eli.currentElement.Value.(*state.Event)
	eli.currentElement = eli.currentElement.Next()

	// Return current element.
	return result
}
