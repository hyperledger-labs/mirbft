/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import "github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"

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
	Send(dest uint64, msg *messagepb.Message)

	// Receive blocks until a message is received by the Net module and returns
	// the numeric ID of the sender and the message itself.
	// If an error occurs, Receive() returns (0, nil, non-nil error).
	// Receive() can be interrupted by closing stopChan, in which case it returns immediately.
	// In this case, it may either return a sender ID and a non-nil message
	// (if a message arrived concurrently with closing stopChan),
	// or (more probably) the tuple (0, nil, nil).
	Receive(stopChan <-chan struct{}) (source uint64, msg *messagepb.Message, err error)
}
