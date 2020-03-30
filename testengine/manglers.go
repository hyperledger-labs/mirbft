/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
	"github.com/golang/protobuf/proto"
)

// EventMangling is meant to be an easy way to construct test descriptions.
// Each method of EventMangling returns itself so that the constraints are easy
// to concatenate.  For instance:
//   Drop().AtPercent(10).Messages().FromNodes(1,3)
// will drop ten percent of the messages from nodes 1 and 3.
// Note that order is important here.  Another perfectly valid string would be:
//   Drop().Messages().FromNodes(1,3).AtPercent(10)
// But here, because filters are applied last to first, on 10 percent of events,
// if they are from nodes 1 and 3, they will be dropped.
type EventMangling struct {
	Mangler Mangler
}

func (em *EventMangling) BeforeStep(random int, el *EventLog) {
	em.Mangler.BeforeStep(random, el)
}

func (em *EventMangling) AtPercent(percent int) *EventMangling {
	em.Mangler = &ProbabilisticMangler{
		Percentage: percent,
		Mangler:    em.Mangler,
	}

	return em
}

func (em *EventMangling) Messages() *EventMangling {
	em.Mangler = &EventTypeFilterMangler{
		Type:    "Receive",
		Mangler: em.Mangler,
	}

	return em
}

func (em *EventMangling) ForNodes(nodes ...uint64) *EventMangling {
	compositeMangler := &CompositeMangler{
		Manglers: make([]Mangler, len(nodes)),
	}

	for i, node := range nodes {
		compositeMangler.Manglers[i] = &MsgSourceFilterMangler{
			Mangler: em.Mangler,
			Source:  node,
		}
	}

	em.Mangler = compositeMangler

	return em
}

func (em *EventMangling) FromNodes(nodes ...uint64) *EventMangling {
	compositeMangler := &CompositeMangler{
		Manglers: make([]Mangler, len(nodes)),
	}

	for i, node := range nodes {
		compositeMangler.Manglers[i] = &MsgSourceFilterMangler{
			Mangler: em.Mangler,
			Source:  node,
		}
	}

	em.Mangler = compositeMangler

	return em
}

func (em *EventMangling) When(when func() bool) *EventMangling {
	em.Mangler = &ConditionalMangler{
		Mangler:   em.Mangler,
		Condition: when,
	}

	return em
}

func Drop() *EventMangling {
	return &EventMangling{
		Mangler: DropMangler{},
	}
}

func Jitter(maxDelay int) *EventMangling {
	return &EventMangling{
		Mangler: &JitterMangler{
			MaxDelay: maxDelay,
		},
	}
}

type DropMangler struct{}

func (DropMangler) BeforeStep(random int, el *EventLog) {
	el.NextEventLogEntry.Event.Dropped = true
}

type DuplicateMangler struct {
	Delay uint64
}

func (dm *DuplicateMangler) BeforeStep(random int, el *EventLog) {
	clone := proto.Clone(el.NextEventLogEntry.Event).(*tpb.Event)
	clone.Time += dm.Delay
	el.Insert(clone)
	// TODO clone.Duplicate = true
}

// JitterMangler will delay events a random amount of time, up to MaxDelay
type JitterMangler struct {
	MaxDelay int
}

func (jm *JitterMangler) BeforeStep(random int, el *EventLog) {
	delay := random % jm.MaxDelay

	el.NextEventLogEntry.Event.Time += uint64(delay)
	event := el.NextEventLogEntry
	if event.Next != nil && event.Next.Event.Time < event.Event.Time {
		el.NextEventLogEntry = event.Next
		if el.FirstEventLogEntry == event {
			el.FirstEventLogEntry = event.Next
		}
	}

	for event.Next != nil && event.Next.Event.Time < event.Event.Time {
		firstEvent := event
		secondEvent := event.Next
		thirdEvent := event.Next.Next

		// Connect the second event to the event before the first
		if firstEvent.Prev != nil {
			firstEvent.Prev.Next = secondEvent
		}
		secondEvent.Prev = firstEvent.Prev

		// Connect the first event after the second event
		secondEvent.Next = firstEvent
		firstEvent.Prev = secondEvent

		// Connect the first event to the third event
		if thirdEvent != nil {
			thirdEvent.Prev = firstEvent
		}
		firstEvent.Next = thirdEvent
	}

	// TODO event.Jitter = delay
}

type EventTypeFilterMangler struct {
	Type    string
	Mangler Mangler
}

func (etfm *EventTypeFilterMangler) BeforeStep(random int, el *EventLog) {
	event := el.NextEventLogEntry.Event
	switch etfm.Type {
	case "Receive":
		_, ok := event.Type.(*tpb.Event_Receive_)
		if ok {
			etfm.Mangler.BeforeStep(random, el)
		}
	case "Tick":
		_, ok := event.Type.(*tpb.Event_Tick_)
		if ok {
			etfm.Mangler.BeforeStep(random, el)
		}
	case "Process":
		_, ok := event.Type.(*tpb.Event_Process_)
		if ok {
			etfm.Mangler.BeforeStep(random, el)
		}
	case "Apply":
		_, ok := event.Type.(*tpb.Event_Apply_)
		if ok {
			etfm.Mangler.BeforeStep(random, el)
		}
	case "Propose":
		_, ok := event.Type.(*tpb.Event_Propose_)
		if ok {
			etfm.Mangler.BeforeStep(random, el)
		}
	default:
		panic("unknown type for filtering in mangler")
	}

}

type MsgSourceFilterMangler struct {
	Mangler Mangler
	Source  uint64
}

func (msfm *MsgSourceFilterMangler) BeforeStep(random int, el *EventLog) {
	event := el.NextEventLogEntry.Event

	recv, ok := event.Type.(*tpb.Event_Receive_)
	if !ok {
		return
	}

	if recv.Receive.Source != msfm.Source {
		return
	}

	msfm.Mangler.BeforeStep(random, el)
}

type ConditionalMangler struct {
	Mangler   Mangler
	Condition func() bool
}

func (cm *ConditionalMangler) BeforeStep(random int, el *EventLog) {
	if !cm.Condition() {
		return
	}

	cm.Mangler.BeforeStep(random, el)
}

type ProbabilisticMangler struct {
	Mangler    Mangler
	Percentage int
}

func (pm *ProbabilisticMangler) BeforeStep(random int, el *EventLog) {
	if random%100 > pm.Percentage {
		return
	}

	pm.Mangler.BeforeStep(random, el)
}

type CompositeMangler struct {
	Manglers []Mangler
}

func (cm *CompositeMangler) BeforeStep(random int, el *EventLog) {
	for _, mangler := range cm.Manglers {
		mangler.BeforeStep(random, el)
	}
}
