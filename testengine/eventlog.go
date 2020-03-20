/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type EventLogEntry struct {
	Event *tpb.Event
	Next  *EventLogEntry
	Prev  *EventLogEntry
}

type EventLog struct {
	Name               string
	Description        string
	InitialConfig      *pb.NetworkConfig
	NodeConfigs        []*tpb.NodeConfig
	FirstEventLogEntry *EventLogEntry
	NextEventLogEntry  *EventLogEntry
	LastConsumed       *EventLogEntry
	FakeTime           uint64
}

func writePrefixedProto(dest io.Writer, msg proto.Message) error {
	lenBuf := make([]byte, binary.MaxVarintLen64)
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	n := binary.PutVarint(lenBuf, int64(len(msgBytes)))
	if _, err = dest.Write(lenBuf[:n]); err != nil {
		return err
	}

	if _, err = dest.Write(msgBytes); err != nil {
		return err
	}

	return nil
}

func (l *EventLog) Write(dest io.Writer) error {
	if err := writePrefixedProto(dest, &tpb.LogEntry{
		Type: &tpb.LogEntry_Scenario{
			Scenario: &tpb.ScenarioConfig{
				Name:                 l.Name,
				Description:          l.Description,
				InitialNetworkConfig: l.InitialConfig,
				NodeConfigs:          l.NodeConfigs,
			},
		},
	}); err != nil {
		return errors.WithMessage(err, "could not serialize scenario")
	}

	for logEntry := l.FirstEventLogEntry; logEntry != nil; logEntry = logEntry.Next {
		if logEntry.Event == nil {
			panic(fmt.Sprintf("log-entry has nil event %v", logEntry))
		}

		if err := writePrefixedProto(dest, &tpb.LogEntry{
			Type: &tpb.LogEntry_Event{
				Event: logEntry.Event,
			},
		}); err != nil {
			return errors.WithMessagef(err, "could not serialize event %+v", logEntry)
		}
	}

	return nil
}

func ReadEventLog(source io.Reader) (el *EventLog, err error) {
	reader := bufio.NewReader(source)
	buffer := bytes.NewBuffer(make([]byte, 10000))

	var eventLog *EventLog
	var scenario *tpb.ScenarioConfig

	for {
		l, err := binary.ReadVarint(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// fmt.Printf("Read varint, next message of length %d", l)

		buffer.Reset()
		if _, err := io.CopyN(buffer, reader, l); err != nil {
			return nil, err
		}
		// fmt.Printf("Read varint, next message of length %d", l)

		pLogEntry := &tpb.LogEntry{}
		err = proto.Unmarshal(buffer.Bytes(), pLogEntry)
		if err != nil {
			return nil, errors.WithMessage(err, "could not decode log entry")
		}

		if pLogEntry.Type == nil {
			return nil, errors.Errorf("log entry type is nil")
		}

		switch {
		case eventLog == nil:
			scenarioType, ok := pLogEntry.Type.(*tpb.LogEntry_Scenario)
			if !ok {
				return nil, errors.Errorf("expected first log entry to be of type scenario, got %T", pLogEntry.Type)
			}

			scenario = scenarioType.Scenario

			eventLog = &EventLog{
				Name:          scenario.Name,
				Description:   scenario.Description,
				InitialConfig: scenario.InitialNetworkConfig,
				NodeConfigs:   scenario.NodeConfigs,
			}
		default:
			eventType, ok := pLogEntry.Type.(*tpb.LogEntry_Event)
			if !ok {
				return nil, errors.Errorf("expected non-first log entry to be of type event, got %T", pLogEntry.Type)
			}

			if eventType.Event == nil {
				return nil, errors.Errorf("all events must be non-nil")
			}

			eventLogEntry := &EventLogEntry{
				Event: eventType.Event,
			}

			if eventLog.FirstEventLogEntry == nil {
				eventLog.FirstEventLogEntry = eventLogEntry
				eventLog.NextEventLogEntry = eventLogEntry
			} else {
				eventLogEntry.Prev = eventLog.NextEventLogEntry
				eventLog.NextEventLogEntry.Next = eventLogEntry
				eventLog.NextEventLogEntry = eventLogEntry
			}
		}
	}

	if eventLog == nil {
		return nil, errors.Errorf("file ended before initial scenario")
	}

	// Reset the cursor to the beginning of the log
	eventLog.NextEventLogEntry = eventLog.FirstEventLogEntry

	return eventLog, nil
}

func (l *EventLog) ConsumeAndAdvance() *tpb.Event {
	nele := l.NextEventLogEntry
	if nele == nil {
		return nil
	}

	l.FakeTime = nele.Event.Time
	l.NextEventLogEntry = nele.Next
	l.LastConsumed = nele
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
		Type: &tpb.Event_Process_{
			Process: &tpb.Event_Process{},
		},
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

	if l.FirstEventLogEntry == nil {
		l.FirstEventLogEntry = logEntry
		l.NextEventLogEntry = logEntry
		return
	}

	if l.NextEventLogEntry == nil {
		l.NextEventLogEntry = logEntry
		l.LastConsumed.Next = logEntry
		logEntry.Prev = l.LastConsumed
		return
	}

	currentEntry := l.NextEventLogEntry
	for {
		if currentEntry.Event.Time > event.Time {
			logEntry.Next = currentEntry
			logEntry.Prev = currentEntry.Prev
			currentEntry.Prev = logEntry
			if logEntry.Prev != nil {
				logEntry.Prev.Next = logEntry
			}
			if currentEntry == l.NextEventLogEntry {
				l.NextEventLogEntry = logEntry
			}
			if currentEntry == l.FirstEventLogEntry {
				l.FirstEventLogEntry = logEntry
			}
			return
		}

		if currentEntry.Next == nil {
			currentEntry.Next = logEntry
			logEntry.Prev = currentEntry
			return
		}
		currentEntry = currentEntry.Next
	}
}

func (l *EventLog) Count() int {
	total := 0
	for event := l.FirstEventLogEntry; event != nil; event = event.Next {
		total++
	}
	return total
}
