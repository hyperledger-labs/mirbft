/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
)

// SilencingMangler will delete any event which is a 'Receive' from
// a particular silenced node.  It is effectively as if all packets
// are being dropped from this source.
type SilencingMangler struct {
	NodeToSilence uint64
}

func (sm *SilencingMangler) BeforeStep(random int, el *EventLog) {
	currentEntry := el.NextEventLogEntry

	recv, ok := currentEntry.Event.Type.(*tpb.Event_Receive_)
	if !ok {
		return
	}

	if recv.Receive.Source != sm.NodeToSilence {
		return
	}

	if el.NextEventLogEntry.Prev != nil {
		el.NextEventLogEntry.Prev.Next = currentEntry.Next
	}
	if currentEntry.Next != nil {
		currentEntry.Prev = currentEntry.Prev
	}
	el.NextEventLogEntry = currentEntry.Next
}
