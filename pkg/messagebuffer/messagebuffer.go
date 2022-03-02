/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package messagebuffer implements a backlog for messages that have been received but cannot yet be processed.
// Any protobuf-serializable message can be stored, and it is the calling code's responsibility
// to keep track of the actual types of the messages it stores and retrieves.
//
// On reception of a message that the node is not yet ready to process
// (e.g., in the ISS protocol, a message from a future epoch
// received from another node that already transitioned to that epoch),
// the message is stored in a buffer for later processing (e.g., when this node also transitions to that epoch).
// The buffer has a maximal capacity, after which it starts evicting the oldest messages when storing new ones,
// such that the capacity constraint is never exceeded.
// Effectively, the most recently received messages that together do not exceed the capacity are stored.
//
// The buffer can be iterated over, selecting the messages that can be stored or safely ignored.
package messagebuffer

import (
	"container/list"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"google.golang.org/protobuf/proto"
)

// Applicable is an enum-style type representing the status of a message stored in the message buffer.
// When iterating over the buffer, the iterator must be provided a function that returns a value of this type.
// The iterator then applies this function to each message and decides,
// based on the return value, what to do with the message.
type Applicable int

const (

	// Past message, usually outdated, no need to process it at all.
	// Iterator action: Delete without processing.
	Past Applicable = iota

	// Current message, should be applied now.
	// Iterator action: apply message and remove it from the buffer.
	Current

	// Future message, cannot be applied yet, but might need to be applied in the future.
	// Iterator action: Keep message in buffer, but do not process it.
	Future

	// Invalid message, cannot be processed.
	// Iterator action: Delete without processing.
	Invalid
)

// MessageBuffer represents a message buffer, buffering messages from a single node.
type MessageBuffer struct {

	// ID of the node sending the messages stored by this buffer.
	nodeID t.NodeID

	// Maximal number of bytes of message data this MessageBuffer can store.
	// Can be changed using the Resize method.
	capacity int

	// Number of bytes occupied by currently stored messages.
	size int

	// List of messages currently stored by this MessageBuffer.
	messages *list.List

	// Logger for outputting debugging messages.
	logger logging.Logger
}

// New returns a newly allocated and initialized MessageBuffer for the given nodeID and with the given initial capacity.
func New(nodeID t.NodeID, capacity int, logger logging.Logger) *MessageBuffer {
	return &MessageBuffer{
		nodeID:   nodeID,
		logger:   logger,
		capacity: capacity,
		size:     0,
		messages: list.New(),
	}
}

// NewBuffers returns multiple buffers, one for each node listed in nodeIDs.
// The total capacity is divided equally among all buffers, i.e., each buffer's capacity is totalCapacity/len(nodeIDs)
// (using integer division, thus the resulting capacities might sum up to less than totalCapacity).
// In the current implementation, only the payload of the stored message is counted towards the capacity,
// disregarding the overhead of the buffer implementation itself.
// The returned buffers are stored in a map, indexed by node IDs.
func NewBuffers(nodeIDs []t.NodeID, totalCapacity int, logger logging.Logger) map[t.NodeID]*MessageBuffer {

	// Allocate a new map for storing the buffers.
	buffers := make(map[t.NodeID]*MessageBuffer)

	// For each of the given node IDs
	for _, nodeID := range nodeIDs {

		// Create a new MessageBuffer.
		buffers[nodeID] = New(nodeID,
			totalCapacity/len(nodeIDs),
			logging.Decorate(logger, "", "source", nodeID),
		)
	}

	// Return the new buffers.
	return buffers
}

// Store stores a given message in the MessageBuffer, if capacity allows it.
// Returns true if the message has been successfully stored, false otherwise.
// If msg is larger than the buffer capacity,
// the message is not stored and the contents of the buffer is left untouched.
// Otherwise, as many least recently added messages are removed from the buffer as is necessary for storing msg.
// Note that this implies that there is no guarantee that msg will remain in the buffer until it is explicitly consumed.
// If store is invoked again with some other messages, msg can be pushed out of the buffer.
func (mb *MessageBuffer) Store(msg proto.Message) bool {

	// Calculate size of the message to store.
	msgSize := proto.Size(msg)

	// If message does not fit in the buffer, even if all its contents were removed, return immediately.
	if msgSize > mb.capacity {
		mb.logger.Log(logging.LevelWarn, "Ignoring message larger than capacity.",
			"source", mb.nodeID, "capacity", mb.capacity, "msgSize", msgSize)
		return false
	}

	// Remove as many old messages as necessary to create enough space for the new message.
	for mb.size+msgSize > mb.capacity {
		e := mb.messages.Front()
		mb.remove(e)
		mb.logger.Log(logging.LevelWarn, "Dropped old message, storing newer message.", "source", mb.nodeID)
	}

	// Add message to buffer and update current size.
	mb.messages.PushBack(msg)
	mb.size += msgSize
	return true
}

// Resize changes the capacity of the MessageBuffer to newCapacity.
// If newCapacity is smaller than the current capacity,
// Resize removes as many least recently added messages
// as is necessary for the buffer size not to exceed the new capacity.
// E.g., if the most recently added message is larger than newCapacity, the buffer will be empty after Resize returns.
func (mb *MessageBuffer) Resize(newCapacity int) {

	// Update buffer capacity
	mb.capacity = newCapacity

	// While the new capacity is exceeded
	for mb.size > mb.capacity {

		// Remove least recently added message.
		e := mb.messages.Front()
		mb.remove(e)
		mb.logger.Log(logging.LevelWarn, "Dropped message when resizing buffer.", "source", mb.nodeID)
	}
}

// remove removes the given element (holding one message) of the internal message list
// and updates the current buffer size accordingly.
func (mb *MessageBuffer) remove(e *list.Element) proto.Message {
	msg := mb.messages.Remove(e).(proto.Message)
	mb.size -= proto.Size(msg)
	return msg
}

// Iterate iterates over all messages in the MessageBuffer and applies a and removes selected ones,
// according the provided filter function.
// Iterate takes two function arguments:
// - filter is applied to every message in the buffer and performs the following actions based on its output
//   (see description of the Applicable type):
//   - Past:    Remove message from the buffer.
//   - Current: Remove message from the buffer and call apply with the message and its sender as arguments.
//   - Future:  Do nothing.
//   - Invalid: Remove message from the buffer.
// - apply is called with every message for which filter returns the Current value.
func (mb *MessageBuffer) Iterate(
	filter func(source t.NodeID, msg proto.Message) Applicable,
	apply func(source t.NodeID, msg proto.Message),
) {

	// Start with the element storing the least recently added message.
	e := mb.messages.Front()

	// While there are more messages in the list,
	for e != nil {

		// Extract the message from and retain a pointer to the current list element
		msg := e.Value.(proto.Message)
		currentElement := e
		e = e.Next()

		// Perform the appropriate action on the message,
		// based on the outcome of the user-provided filter function.
		switch filter(mb.nodeID, msg) {
		case Past:
			mb.remove(currentElement)
		case Current:
			mb.remove(currentElement)
			apply(mb.nodeID, msg)
		case Future:
			// Skip future messages
		case Invalid:
			mb.remove(currentElement)
		}
	}
}
