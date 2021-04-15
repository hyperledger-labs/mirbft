/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"bytes"
	"container/list"
	"fmt"
	"math/rand"

	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
)

type Event struct {
	Target                uint64
	Time                  int64
	Initialize            *EventInitialize
	MsgReceived           *EventMsgReceived
	ClientProposal        *EventClientProposal
	ProcessWALActions     *statemachine.ActionList
	ProcessNetActions     *statemachine.ActionList
	ProcessHashActions    *statemachine.ActionList
	ProcessClientActions  *statemachine.ActionList
	ProcessAppActions     *statemachine.ActionList
	ProcessReqStoreEvents *statemachine.EventList
	ProcessResultEvents   *statemachine.EventList
	Tick                  *EventTick
}

type EventInitialize struct {
	InitParms *state.EventInitialParameters
}

type EventMsgReceived struct {
	Source uint64
	Msg    *msgs.Msg
}

type EventClientProposal struct {
	ClientID uint64
	ReqNo    uint64
	Data     []byte
}

type EventProcessActions statemachine.ActionList

type EventProcessEvents statemachine.EventList

type EventTick struct{}

type EventQueue struct {
	// List is a list of *Event messages, in order of time.
	List *list.List

	// FakeTime is the current 'time' according to this log.
	FakeTime int64

	// Rand is a source of randomness for the manglers
	Rand *rand.Rand

	// Mangler is invoked on each event when it is first inserted
	Mangler Mangler

	// Mangled tracks which events have already been mangled to prevent loops
	Mangled map[*Event]struct{}
}

func (l *EventQueue) ConsumeEvent() *Event {
	for {
		event := l.List.Remove(l.List.Front()).(*Event)

		_, ok := l.Mangled[event]
		if ok || l.Mangler == nil {
			delete(l.Mangled, event)
			l.FakeTime = event.Time
			return event
		}

		mangleResults := l.Mangler.Mangle(l.Rand.Int(), event)
		for _, result := range mangleResults {
			if l.Mangled == nil {
				l.Mangled = map[*Event]struct{}{}
			}

			if !result.Remangle {
				l.Mangled[result.Event] = struct{}{}
			}

			l.InsertEvent(result.Event)
		}

	}
}

func (l *EventQueue) InsertInitialize(target uint64, initParms *state.EventInitialParameters, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target: target,
			Initialize: &EventInitialize{
				InitParms: initParms,
			},
			Time: l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertTickEvent(target uint64, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target: target,
			Tick:   &EventTick{},
			Time:   l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertMsgReceived(target, source uint64, msg *msgs.Msg, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target: target,
			MsgReceived: &EventMsgReceived{
				Source: source,
				Msg:    msg,
			},
			Time: l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertClientProposal(target, clientID, reqNo uint64, data []byte, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target: target,
			ClientProposal: &EventClientProposal{
				ClientID: clientID,
				ReqNo:    reqNo,
				Data:     data,
			},
			Time: l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessReqStoreEvents(target uint64, events *statemachine.EventList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:                target,
			ProcessReqStoreEvents: events,
			Time:                  l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessResultEvents(target uint64, events *statemachine.EventList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:              target,
			ProcessResultEvents: events,
			Time:                l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessWALActions(target uint64, actions *statemachine.ActionList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:            target,
			ProcessWALActions: actions,
			Time:              l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessNetActions(target uint64, actions *statemachine.ActionList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:            target,
			ProcessNetActions: actions,
			Time:              l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessClientActions(target uint64, actions *statemachine.ActionList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:               target,
			ProcessClientActions: actions,
			Time:                 l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessHashActions(target uint64, actions *statemachine.ActionList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:             target,
			ProcessHashActions: actions,
			Time:               l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertProcessAppActions(target uint64, actions *statemachine.ActionList, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:            target,
			ProcessAppActions: actions,
			Time:              l.FakeTime + fromNow,
		},
	)
}

func (l *EventQueue) InsertEvent(event *Event) {
	if event.Time < l.FakeTime {
		panic("attempted to modify the past")
	}

	for el := l.List.Front(); el != nil; el = el.Next() {
		if el.Value.(*Event).Time > event.Time {
			l.List.InsertBefore(event, el)
			return
		}
	}

	l.List.PushBack(event)
}

func (l *EventQueue) Status() string {
	count := l.List.Len()
	if count == 0 {
		return "Empty EventQueue"
	}

	el := l.List.Back()
	var buf bytes.Buffer
	for i := 0; i < 50; i++ {
		event := el.Value.(*Event)
		var subEvent string
		switch {
		case event.Initialize != nil:
			subEvent = "Initialize"
		case event.MsgReceived != nil:
			subEvent = "MsgReceived"
		case event.ClientProposal != nil:
			subEvent = "ClientProposal"
		case event.ProcessWALActions != nil:
			subEvent = "ProcessWALActions"
		case event.ProcessNetActions != nil:
			subEvent = "ProcessNetActions"
		case event.ProcessHashActions != nil:
			subEvent = "ProcessHashActions"
		case event.ProcessClientActions != nil:
			subEvent = "ProcessClientActions"
		case event.ProcessAppActions != nil:
			subEvent = "ProcessAppActions"
		case event.ProcessReqStoreEvents != nil:
			subEvent = "ProcessReqStoreEvents"
		case event.ProcessResultEvents != nil:
			subEvent = "ProcessResultEvents"
		case event.Tick != nil:
			subEvent = "Tick"
		default:
			panic("unexpected event type")
		}

		fmt.Fprintf(&buf, "[node=%d, event_type=%s time=%d] %+v\n", event.Target, subEvent, event.Time, subEvent)
		el = el.Prev()
		if i >= count || el == nil {
			fmt.Fprintf(&buf, "\nCompleted eventlog summary of %d events\n", count)
			return buf.String()
		}
	}

	fmt.Fprintf(&buf, "\n ... skipping %d entries ... \n", count-50)
	return buf.String()
}
