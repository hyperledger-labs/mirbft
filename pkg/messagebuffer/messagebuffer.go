/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package messagebuffer

import (
	"container/list"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type Applicable int

const (
	Past Applicable = iota
	Current
	Future
	Invalid
)

type MessageBuffer struct {
	nodeID   t.NodeID
	logger   logging.Logger
	capacity int
	size     int
	messages *list.List
}

func New(nodeID t.NodeID, capacity int, logger logging.Logger) *MessageBuffer {
	return &MessageBuffer{
		nodeID:   nodeID,
		logger:   logger,
		capacity: capacity,
		size:     0,
		messages: list.New(),
	}
}

func NewBuffers(nodeIDs []t.NodeID, totalCapacity int, logger logging.Logger) map[t.NodeID]*MessageBuffer {
	buffers := make(map[t.NodeID]*MessageBuffer)
	for _, nodeID := range nodeIDs {
		buffers[nodeID] = New(nodeID,
			totalCapacity/len(nodeIDs),
			logging.Decorate(logger, "", "source", nodeID),
		)
	}
	return buffers
}

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

	// Add message to buffer.
	mb.messages.PushBack(msg)
	mb.size += msgSize
	return true
}

func (mb *MessageBuffer) Resize(newCapacity int) {
	mb.capacity = newCapacity

	for mb.size > mb.capacity {
		e := mb.messages.Front()
		mb.remove(e)
		mb.logger.Log(logging.LevelWarn, "Dropped message when resizing buffer.", "source", mb.nodeID)
	}
}

func (mb *MessageBuffer) remove(e *list.Element) proto.Message {
	msg := mb.messages.Remove(e).(proto.Message)
	mb.size -= proto.Size(msg)
	return msg
}

func (mb *MessageBuffer) Iterate(
	filter func(source t.NodeID, msg proto.Message) Applicable,
	apply func(source t.NodeID, msg proto.Message),
) {
	e := mb.messages.Front()
	for e != nil {
		msg := e.Value.(proto.Message)
		currentElement := e
		e = e.Next()
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
