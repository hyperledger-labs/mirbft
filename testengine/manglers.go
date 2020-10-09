/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"

	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"
	"google.golang.org/protobuf/proto"
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

func (em *EventMangling) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	return em.Mangler.Mangle(random, event)
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

func (em *EventMangling) OfMsgTypes(msgTypes ...string) *EventMangling {
	compositeMangler := &CompositeMangler{
		Manglers: make([]Mangler, len(msgTypes)),
	}

	for i, msgType := range msgTypes {
		compositeMangler.Manglers[i] = &MsgTypeFilterMangler{
			Mangler: em.Mangler,
			Type:    msgType,
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

func Duplicate(maxDelay int) *EventMangling {
	return &EventMangling{
		Mangler: &DuplicateMangler{
			MaxDelay: maxDelay,
		},
	}
}

type DropMangler struct{}

func (DropMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	return nil
}

type DuplicateMangler struct {
	MaxDelay int
}

func (dm *DuplicateMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	clone := proto.Clone(event).(*rpb.RecordedEvent)
	delay := int64(random % dm.MaxDelay)
	clone.Time += delay
	return []*rpb.RecordedEvent{event, clone}
}

// JitterMangler will delay events a random amount of time, up to MaxDelay
type JitterMangler struct {
	MaxDelay int
}

func (jm *JitterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	delay := int64(random % jm.MaxDelay)
	event.Time += delay
	return []*rpb.RecordedEvent{event}
}

type EventTypeFilterMangler struct {
	Type    string
	Mangler Mangler
}

func (etfm *EventTypeFilterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	switch etfm.Type {
	case "Receive":
		_, ok := event.StateEvent.Type.(*pb.StateEvent_Step)
		if ok {
			return etfm.Mangler.Mangle(random, event)
		}
	case "Tick":
		_, ok := event.StateEvent.Type.(*pb.StateEvent_Tick)
		if ok {
			return etfm.Mangler.Mangle(random, event)
		}
	case "Process":
		_, ok := event.StateEvent.Type.(*pb.StateEvent_ActionsReceived)
		if ok {
			return etfm.Mangler.Mangle(random, event)
		}
	case "Apply":
		_, ok := event.StateEvent.Type.(*pb.StateEvent_AddResults)
		if ok {
			return etfm.Mangler.Mangle(random, event)
		}
	case "Propose":
		_, ok := event.StateEvent.Type.(*pb.StateEvent_Propose)
		if ok {
			return etfm.Mangler.Mangle(random, event)
		}
	default:
	}
	return []*rpb.RecordedEvent{event}
}

type MsgSourceFilterMangler struct {
	Mangler Mangler
	Source  uint64
}

func (msfm *MsgSourceFilterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	recv, ok := event.StateEvent.Type.(*pb.StateEvent_Step)
	if !ok {
		return []*rpb.RecordedEvent{event}
	}

	if recv.Step.Source != msfm.Source {
		return []*rpb.RecordedEvent{event}
	}

	if recv.Step.Source == event.NodeId {
		// Never allow messages from a node to itself to be mangled
		return []*rpb.RecordedEvent{event}
	}

	return msfm.Mangler.Mangle(random, event)
}

type MsgTypeFilterMangler struct {
	Mangler Mangler
	Type    string
}

func (mtfm *MsgTypeFilterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	recv, ok := event.StateEvent.Type.(*pb.StateEvent_Step)
	if !ok {
		return []*rpb.RecordedEvent{event}
	}

	msgType := fmt.Sprintf("%T", recv.Step.Msg.Type)[len("*mirbftpb.Msg_"):]
	if msgType != mtfm.Type {
		return []*rpb.RecordedEvent{event}
	}

	if recv.Step.Source == event.NodeId {
		// Never allow messages from a node to itself to be mangled
		return []*rpb.RecordedEvent{event}
	}

	return mtfm.Mangler.Mangle(random, event)
}

type ConditionalMangler struct {
	Mangler   Mangler
	Condition func() bool
}

func (cm *ConditionalMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	if !cm.Condition() {
		return []*rpb.RecordedEvent{event}
	}

	return cm.Mangler.Mangle(random, event)
}

type ProbabilisticMangler struct {
	Mangler    Mangler
	Percentage int
}

func (pm *ProbabilisticMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	if random%100 > pm.Percentage {
		return []*rpb.RecordedEvent{event}
	}

	return pm.Mangler.Mangle(random, event)
}

type CompositeMangler struct {
	Manglers []Mangler
}

func (cm *CompositeMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	events := []*rpb.RecordedEvent{event}
	for _, mangler := range cm.Manglers {
		var newEvents []*rpb.RecordedEvent
		for _, event := range events {
			newEvents = append(newEvents, mangler.Mangle(random, event)...)
		}
		events = newEvents
	}
	return events
}
