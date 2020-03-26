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
	event := el.NextEventLogEntry.Event

	recv, ok := event.Type.(*tpb.Event_Receive_)
	if !ok {
		return
	}

	if recv.Receive.Source != sm.NodeToSilence {
		return
	}

	event.Dropped = true
}

type ProposeDropper struct {
	NodeToDropAt uint64
}

func (pd *ProposeDropper) BeforeStep(random int, el *EventLog) {
	event := el.NextEventLogEntry.Event

	_, ok := event.Type.(*tpb.Event_Propose_)
	if !ok {
		return
	}

	if event.Target != pd.NodeToDropAt {
		return
	}

	event.Dropped = true
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
