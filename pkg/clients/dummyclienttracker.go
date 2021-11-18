/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package clients

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	"github.com/hyperledger-labs/mirbft/pkg/serializing"
)

type DummyClientTracker struct {
}

// ApplyEvent processes an event incoming to the SigningClientTracker module
// and produces a (potentially empty) list of new events to be processed by the node.
func (ct *DummyClientTracker) ApplyEvent(event *eventpb.Event) *events.EventList {
	switch e := event.Type.(type) {

	case *eventpb.Event_Request:
		// Request received from a client. Have the digest computed.

		req := e.Request
		return (&events.EventList{}).PushBack(events.HashRequest(
			serializing.RequestForHash(req),
			&eventpb.HashOrigin{Type: &eventpb.HashOrigin_Request{Request: req}},
		))

	case *eventpb.Event_HashResult:
		// Digest for a client request.
		// Persist request and announce it to the protocol.
		// TODO: Implement request number watermarks and authentication.

		digest := e.HashResult.Digest
		//fmt.Printf("Received digest: %x\n", digest)

		// Create a request reference and submit it to the protocol state machine as a request ready to be processed.
		// TODO: postpone this until the request is stored and authenticated.
		req := e.HashResult.Origin.Type.(*eventpb.HashOrigin_Request).Request
		reqRef := &requestpb.RequestRef{
			ClientId: req.ClientId,
			ReqNo:    req.ReqNo,
			Digest:   digest,
		}
		return (&events.EventList{}).PushBack(events.StoreDummyRequest(reqRef, req.Data))

	default:
		panic(fmt.Sprintf("unknown event: %T", event.Type))
	}
}

// TODO: Implement and document.
func (ct *DummyClientTracker) Status() (s *statuspb.ClientTrackerStatus, err error) {
	return nil, nil
}
