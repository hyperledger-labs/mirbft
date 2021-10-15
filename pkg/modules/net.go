/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// ReceivedMessage represents a message received over the network.
// This is the type of structures written to the channel returned by Net.ReceiveChan().
type ReceivedMessage struct {

	// Numeric ID of the node that sent the message.
	// Since the message is already authenticated by the Net module implementation, this information can be relied on.
	Sender t.NodeID

	// The received message itself.
	Msg *messagepb.Message
}

// The Net module provides a simple abstract interface for sending messages to and receiving messages from other nodes.
// It abstracts away all network-related data and only deals with abstract numeric node IDs of senders and receivers
// at the interface. The messages returned from the Receive() must be (the library assumes them to be) authenticated.
// TODO: Deal explicitly with membership somewhere (not necessarily here).
// Note that the Net module's Receive() method is (currently? TODO)
// one of two ways of injecting messages in the Node.
// Alternatively, the Node.Step() method can be used directly to inject incoming messages.
// The user might choose to use the latter option, using the Net module only for sending.
// In such a case, the implementation of the Receive() method of the used Net module must simply block
// until interrupted by the passed channel (see documentation of Receive()).
type Net interface {

	// Send sends msg to the node with ID dest.
	// Concurrent calls to Send are not (yet? TODO) supported.
	Send(dest t.NodeID, msg *messagepb.Message) error

	// ReceiveChan returns a channel to which the Net module writes all received messages and sender IDs
	// (Both the message itself and the sender ID are part of the ReceivedMessage struct.)
	ReceiveChan() <-chan ReceivedMessage
}
