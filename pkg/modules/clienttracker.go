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

// TODO: Document this.

type ClientTracker interface {
	ApplyEvent(event *eventpb.Event) *events.EventList
	Status() (s *statuspb.ClientTrackerStatus, err error)
}
