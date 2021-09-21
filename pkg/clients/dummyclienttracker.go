/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package clients

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/status"
)

type DummyClientTracker struct {
}

// ApplyEvent processes an event incoming to the ClientTracker module
// and produces a (potentially empty) list of new events to be processed by the node.
func (ct *DummyClientTracker) ApplyEvent(event *eventpb.Event) *events.EventList {
	switch e := event.Type.(type) {

	case *eventpb.Event_Request:
		// Request received from a client. Have the digest computed.

		req := e.Request
		fmt.Println("Received request.")
		return (&events.EventList{}).PushBack(hashRequest(req))

	case *eventpb.Event_HashResult:
		// Digest for a client request.
		// Persist request and announce it to the protocol.
		// TODO: Implement request number watermarks and authentication.

		digest := e.HashResult.Digest
		fmt.Printf("Received digest: %x\n", digest)

		// Create a request reference and submit it to the protocol state machine as a request ready to be processed.
		// TODO: postpone this until the request is stored and authenticated.
		req := e.HashResult.Origin.Type.(*eventpb.HashOrigin_Request).Request
		reqRef := &messagepb.RequestRef{
			ClientId: req.ClientId,
			ReqNo:    req.ReqNo,
			Digest:   digest,
		}
		return (&events.EventList{}).PushBack(events.RequestReady(reqRef))

	default:
		panic(fmt.Sprintf("unknown event: %T", event.Type))
	}
}

// TODO: Implement and document.
func (ct *DummyClientTracker) Status() (s *status.StateMachine, err error) {
	return nil, nil
}

// Returns a HashRequest Event to be sent to the Hasher module for hashing a client request.
func hashRequest(req *messagepb.Request) *eventpb.Event {

	// Encode all data to be hashed.
	clientIDBuf := make([]byte, 8)
	reqNoBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(clientIDBuf, req.ClientId)
	binary.LittleEndian.PutUint64(reqNoBuf, req.ReqNo)

	// Return hash request event.
	return events.HashRequest(
		[][]byte{clientIDBuf, reqNoBuf, req.Data},
		&eventpb.HashOrigin{Type: &eventpb.HashOrigin_Request{Request: req}},
	)
}
