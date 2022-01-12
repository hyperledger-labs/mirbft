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
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
)

type eventMetadata struct {
	time   int64
	nodeID uint64
	index  uint64
}

func IncludedIn(value string, includeList []string) bool {

	for _, includeName := range includeList {
		if includeName == value {
			return true
		}
	}
	return false
}

func ReadEvent(args *arguments) error {
	reader, err := eventlog.NewReader(args.srcFile)
	if err != nil {
		return err
	}
	index := uint64(0)
	for event, err := reader.ReadEntry(); err == nil; event, err = reader.ReadEntry() {
		metadata := eventMetadata{
			nodeID: event.NodeId,
			time:   event.Time,
		}
		for _, value := range event.Events {
			metadata.index = index
			processEvent(value, metadata, args)
			index++
		}
	}
	return nil
}

func processEvent(event *eventpb.Event, metadata eventMetadata, args *arguments) {
	var eventTypeText string
	var isseventTypeText string
	switch event.Type.(type) {
	case *eventpb.Event_Init:
		eventTypeText = "Initialize"
	case *eventpb.Event_Tick:
		eventTypeText = "Tick"
	case *eventpb.Event_Deliver:
		eventTypeText = "Deliver"
	case *eventpb.Event_WalAppend:
		eventTypeText = "WalAppend"
	case *eventpb.Event_HashResult:
		eventTypeText = "HashResult"
	case *eventpb.Event_WalEntry:
		eventTypeText = "WalEntry"
	case *eventpb.Event_WalTruncate:
		eventTypeText = "WalTruncate"
	case *eventpb.Event_Request:
		eventTypeText = "ActionsReceived"
	case *eventpb.Event_HashRequest:
		eventTypeText = "HashRequest"
	case *eventpb.Event_MessageReceived:
		eventTypeText = "MessageReceived"
	case *eventpb.Event_Iss:
		eventTypeText = "Iss"
	case *eventpb.Event_VerifyRequestSig:
		eventTypeText = "VerifyRequestSig"
	case *eventpb.Event_RequestSigVerified:
		eventTypeText = "RequestSigVerified"
	case *eventpb.Event_StoreVerifiedRequest:
		eventTypeText = "StoreVerifiedRequest"
	case *eventpb.Event_AppSnapshotRequest:
		eventTypeText = "AppSnapshotRequest"
	case *eventpb.Event_AppSnapshot:
		eventTypeText = "AppSnapshot"
	case *eventpb.Event_SendMessage:
		eventTypeText = "SendMessage"
	case *eventpb.Event_RequestReady:
		eventTypeText = "RequestReady"
	case *eventpb.Event_StoreDummyRequest:
		eventTypeText = "StoreDummyRequest"
	case *eventpb.Event_AnnounceDummyBatch:
		eventTypeText = "AnnounceDummyBatch"
	case *eventpb.Event_PersistDummyBatch:
		eventTypeText = "PersistDummyBatch"
	default:
		panic(fmt.Sprintf("Unknown event type '%T'", event.Type))
	}

	if eventTypeText == "Iss" {
		switch event.GetIss().GetType().(type) {
		case *isspb.ISSEvent_PersistCheckpoint:
			isseventTypeText = "PersistCheckpoint"
		case *isspb.ISSEvent_StableCheckpoint:
			isseventTypeText = "StableCheckpoint"
		case *isspb.ISSEvent_PersistStableCheckpoint:
			isseventTypeText = "PersistStableCheckpoint"
		case *isspb.ISSEvent_Sb:
			isseventTypeText = "Sb"
		default:
			panic(fmt.Sprintf("Unknown event type '%T'", event.Type))

		}
		if IncludedIn(isseventTypeText, args.includedIssEvents) {
			DisplayEvent(fmt.Sprintf("Iss : %s", isseventTypeText), event.String(), metadata)
		}
	} else {
		if IncludedIn(eventTypeText, args.includedEvents) {
			DisplayEvent(eventTypeText, event.String(), metadata)
		}
	}
}
