/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
)

type EventLogEntry struct {
	Event *tpb.Event
	Next  *EventLogEntry
}

type EventLog struct {
	InitialConfig      *pb.NetworkConfig
	NodeConfigs        []*tpb.NodeConfig
	FirstEventLogEntry *EventLogEntry
	NextEventLogEntry  *EventLogEntry
	FakeTime           uint64
}

func (l *EventLog) ConsumeAndAdvance() *tpb.Event {
	nele := l.NextEventLogEntry
	if nele == nil {
		return nil
	}

	l.FakeTime = nele.Event.Time
	l.NextEventLogEntry = nele.Next
	return nele.Event
}

func (l *EventLog) InsertApply(target uint64, apply *tpb.Event_Apply, fromNow uint64) {
	l.Insert(&tpb.Event{
		Target: target,
		Time:   l.FakeTime + fromNow,
		Type: &tpb.Event_Apply_{
			Apply: apply,
		},
	})
}

func (l *EventLog) InsertTick(target uint64, fromNow uint64) {
	l.Insert(&tpb.Event{
		Target: target,
		Time:   l.FakeTime + fromNow,
		Type: &tpb.Event_Tick_{
			Tick: &tpb.Event_Tick{},
		},
	})
}

func (l *EventLog) InsertRecv(target uint64, source uint64, msg *pb.Msg, fromNow uint64) {
	l.Insert(&tpb.Event{
		Target: target,
		Time:   l.FakeTime + fromNow,
		Type: &tpb.Event_Receive_{
			Receive: &tpb.Event_Receive{
				Source: source,
				Msg:    msg,
			},
		},
	})
}

func (l *EventLog) InsertProcess(target uint64, fromNow uint64) {
	l.Insert(&tpb.Event{
		Target: target,
		Time:   l.FakeTime + fromNow,
		Type:   &tpb.Event_Process_{},
	})
}

func (l *EventLog) InsertPropose(target uint64, requestData *pb.RequestData, fromNow uint64) {
	l.Insert(&tpb.Event{
		Target: target,
		Time:   l.FakeTime + fromNow,
		Type: &tpb.Event_Propose_{
			Propose: &tpb.Event_Propose{
				RequestData: requestData,
			},
		},
	})
}

func (l *EventLog) Insert(event *tpb.Event) {
	logEntry := &EventLogEntry{
		Event: event,
	}

	if l.NextEventLogEntry == nil || event.Time < l.NextEventLogEntry.Event.Time {
		logEntry.Next = l.NextEventLogEntry
		l.NextEventLogEntry = logEntry
		return
	}

	currentEntry := l.NextEventLogEntry
	for {
		if currentEntry.Next == nil || currentEntry.Next.Event.Time > event.Time {
			logEntry.Next = currentEntry.Next
			currentEntry.Next = logEntry
			return
		}
		currentEntry = currentEntry.Next
	}
}
