// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orderer

import (
	"sync"

	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// The Orderer is responsible for ordering log Entries, i.e., creating the mapping between Entries and sequence numbers.
// It receives Segments (representing parts of the log) from the Manager and "fills" these parts of the log with
// Entries. The Orderer can process multiple Segments in parallel, executing multiple instances of an ordering protocol
// (or even different protocols) - one for each Segment.
// When a (final and irreversible) assignment of an Entry to a sequence number is made by the ordering protocol,
// the protocol calls announcer.Announce() to announce the decision.
type Orderer interface {

	// Initializes the orderer.
	// Attaches a Manager to the Orderer. All the Segments that are created by the Manager and in which this node is
	// involved (as a leader, as a follower, or both) will be handled by this Orderer.
	// Init() must be called before the Manager is started, so the Orderer does not miss any segments.
	// Init() must be called before Strat() is called.
	// After Init() returns, the Orderer mus be ready to process incoming messages, but must not send any itself
	// (sending messages only can start after the call to Start()).
	Init(manager manager.Manager)

	// Message handler function. To be assigned to the messenger.OrdererMsgHandler variable, which the messenger calls
	// on reception of a message belonging to the ordering subprotocol.
	HandleMessage(message *pb.ProtocolMessage)

	// Must be called when an entry is being inserted to the log otherwise than by the orderer itself,
	// e.g. by the state transfer protocol.
	HandleEntry(entry *log.Entry)

	// Satrts the Orderer. Meant to be run as a separate goroutine. Decrements the passed wait group on termination.
	// Before starting the Orderer, it must be attached to the Manager and its message handler must be registered with
	// the messanger.
	Start(group *sync.WaitGroup)

	// Signs the data with the private key (share) of the orderer.
	Sign(data []byte) ([]byte, error)

	// Verifies that the signature on the (unhashed) data with public key of the sender is correct.
	// SenderID must correspond to an orderer, whose key is known in system's membership config.
	CheckSig(data []byte, senderID int32, signature []byte) error
}
