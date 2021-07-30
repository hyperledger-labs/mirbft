/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package clients

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/status"
)

type ClientTracker struct {
}

// ApplyEvent processes an event incoming to the ClientTracker module
// and produces a (potentially empty) list of new events to be processed by the node.
func (ct *ClientTracker) ApplyEvent(event *state.Event) *events.EventList {
	switch e := event.Type.(type) {

	case *state.Event_Request:
		// Request received from a client. Have the digest computed.

		req := e.Request
		fmt.Println("Received request.")
		return (&events.EventList{}).PushBack(hashRequest(req))

	case *state.Event_HashResult:
		// Digest for a client request.
		// Persist request and insert it into the state machine.
		// TODO: Implement request number watermarks.

		digest := e.HashResult.Digest
		fmt.Printf("Received digest: %x\n", digest)
		return &events.EventList{}

	default:
		panic(fmt.Sprintf("unknown event: %T", event.Type))
	}
}

// TODO: Implement and document.
func (ct *ClientTracker) Status() (s *status.StateMachine, err error) {
	return nil, nil
}

// Returns a HashRequest Event to be sent to the Hasher module for hashing a client request.
func hashRequest(req *msgs.Request) *state.Event {

	// Encode all data to be hashed.
	clientIDBuf := make([]byte, 8)
	reqNoBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(clientIDBuf, req.ClientId)
	binary.LittleEndian.PutUint64(reqNoBuf, req.ReqNo)

	// Return hash request event.
	return &state.Event{Type: &state.Event_HashRequest{HashRequest: &state.EventHashRequest{
		Data:   [][]byte{clientIDBuf, reqNoBuf, req.Data},
		Origin: &state.HashOrigin{Type: &state.HashOrigin_Request{Request: req}},
	}}}
}
