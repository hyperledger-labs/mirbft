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

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
)

type Event struct {
	Target         uint64
	Time           int64
	Initialize     *EventInitialize
	MsgReceived    *EventMsgReceived
	ClientProposal *EventClientProposal
	ProcessActions *EventProcessActions
	ProcessEvents  *EventProcessEvents
	Tick           *EventTick
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

type EventProcessActions struct{}

type EventProcessEvents struct{}

type EventTick struct{}

type EventLog struct {
	// List is a list of *Event messages, in order of time.
	List *list.List

	// FakeTime is the current 'time' according to this log.
	FakeTime int64

	// Rand is a source of randomness for the manglers
	Rand *rand.Rand
}

func (l *EventLog) ConsumeEvent() *Event {
	event := l.List.Remove(l.List.Front()).(*Event)
	l.FakeTime = event.Time
	return event
}

func (l *EventLog) InsertInitialize(target uint64, initParms *state.EventInitialParameters, fromNow int64) {
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

func (l *EventLog) InsertTickEvent(target uint64, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target: target,
			Tick:   &EventTick{},
			Time:   l.FakeTime + fromNow,
		},
	)
}

func (l *EventLog) InsertMsgReceived(target, source uint64, msg *msgs.Msg, fromNow int64) {
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

func (l *EventLog) InsertClientProposal(target, clientID, reqNo uint64, data []byte, fromNow int64) {
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

func (l *EventLog) InsertProcessEvents(target uint64, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:        target,
			ProcessEvents: &EventProcessEvents{},
			Time:          l.FakeTime + fromNow,
		},
	)
}

func (l *EventLog) InsertProcessActions(target uint64, fromNow int64) {
	l.InsertEvent(
		&Event{
			Target:         target,
			ProcessActions: &EventProcessActions{},
			Time:           l.FakeTime + fromNow,
		},
	)
}

func (l *EventLog) InsertEvent(event *Event) {
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

func (l *EventLog) Status() string {
	count := l.List.Len()
	if count == 0 {
		return "Empty EventLog"
	}

	el := l.List.Back()
	var buf bytes.Buffer
	for i := 0; i < 50; i++ {
		event := el.Value.(*Event)
		var subEvent interface{}
		switch {
		case event.Initialize != nil:
			subEvent = event.Initialize
		case event.MsgReceived != nil:
			subEvent = event.MsgReceived
		case event.ClientProposal != nil:
			subEvent = event.ClientProposal
		case event.ProcessActions != nil:
			subEvent = event.ProcessActions
		case event.ProcessEvents != nil:
			subEvent = event.ProcessEvents
		case event.Tick != nil:
			subEvent = event.Tick
		default:
			panic("unexpected event type")
		}

		fmt.Fprintf(&buf, "[node=%d, event_type=%T time=%d] %+v\n", event.Target, subEvent, event.Time, subEvent)
		el = el.Prev()
		if i >= count || el == nil {
			fmt.Fprintf(&buf, "\nCompleted eventlog summary of %d events\n", count)
			return buf.String()
		}
	}

	fmt.Fprintf(&buf, "\n ... skipping %d entries ... \n", count-50)
	return buf.String()
}
