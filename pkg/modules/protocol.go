/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
)

// Protocol represents the logic of a protocol.
// Similarly to the application, a protocol is modeled as a state machine.
// It takes a sequence of Events (e.g. incoming messages, ticks of the clock, etc.) as input.
// The processing of each input may result in a (deterministic) modification of the protocol state
// and a list of output events for further processing by other modules.
type Protocol interface {

	// ApplyEvent applies a single input event to the protocol, making it advance its state
	// and potentially output a list of output events that the application of the input event results in.
	ApplyEvent(event *eventpb.Event) *events.EventList

	// Status returns the current state of the protocol.
	// Mostly for debugging purposes.
	Status() (s *statuspb.ProtocolStatus, err error)
}
