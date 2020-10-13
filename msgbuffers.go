/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"google.golang.org/protobuf/proto"
)

type nodeBuffers struct {
	logger   Logger
	myConfig *pb.StateEvent_InitialParameters
	nodeMap  map[nodeID]*nodeBuffer
}

func newNodeBuffers(myConfig *pb.StateEvent_InitialParameters, logger Logger) *nodeBuffers {
	return &nodeBuffers{
		logger:   logger,
		myConfig: myConfig,
		nodeMap:  map[nodeID]*nodeBuffer{},
	}
}

func (nbs *nodeBuffers) nodeBuffer(source nodeID) *nodeBuffer {
	nb, ok := nbs.nodeMap[source]
	if !ok {
		nb = &nodeBuffer{
			id:       source,
			logger:   nbs.logger,
			myConfig: nbs.myConfig,
		}
	}

	return nb
}

type nodeBuffer struct {
	id        nodeID
	logger    Logger
	myConfig  *pb.StateEvent_InitialParameters
	totalSize int
}

func (nb *nodeBuffer) logDrop(component string, msg *pb.Msg) {
	nb.logger.Warn(fmt.Sprintf("dropping buffered msg component=%s, msgType=%T\n", component, msg.Type))
}

func (nb *nodeBuffer) msgRemoved(msg *pb.Msg) {
	nb.totalSize -= proto.Size(msg)
}

func (nb *nodeBuffer) msgStored(msg *pb.Msg) {
	nb.totalSize += proto.Size(msg)
}

func (nb *nodeBuffer) overCapacity() bool {
	return nb.totalSize > int(nb.myConfig.BufferSize)
}

type applyable int

const (
	past applyable = iota
	current
	future
	invalid
)

type msgBuffer struct {
	// component is used for logging and status only
	component  string
	buffer     *list.List
	nodeBuffer *nodeBuffer
}

func newMsgBuffer(component string, nodeBuffer *nodeBuffer) *msgBuffer {
	return &msgBuffer{
		component:  component,
		buffer:     list.New(),
		nodeBuffer: nodeBuffer,
	}
}

func (mb *msgBuffer) store(msg *pb.Msg) {
	// If there is not configured room to buffer, and we have anything
	// in our buffer, flush it first.  This isn't really 'fair',
	// but the handwaving says that buffers should accumulate messages
	// of similar size, so, once we're out of room, buffers stay basically
	// the same length.  Maybe we will want to change this strategy.
	for mb.nodeBuffer.overCapacity() && mb.buffer.Len() > 0 {
		e := mb.buffer.Front()
		oldMsg := mb.buffer.Remove(e).(*pb.Msg)
		mb.nodeBuffer.logDrop(mb.component, oldMsg)
		mb.nodeBuffer.msgRemoved(oldMsg)
	}
	mb.buffer.PushBack(msg)
	mb.nodeBuffer.msgStored(msg)
}

func (mb *msgBuffer) next(filter func(*pb.Msg) applyable) *pb.Msg {
	e := mb.buffer.Front()
	if e == nil {
		return nil
	}

	for e != nil {
		msg := e.Value.(*pb.Msg)
		switch filter(msg) {
		case past:
			x := e
			e = e.Next() // get next before removing current
			mb.buffer.Remove(x)
			mb.nodeBuffer.msgRemoved(msg)
		case current:
			mb.buffer.Remove(e)
			mb.nodeBuffer.msgRemoved(msg)
			return msg
		case future:
			e = e.Next()
		case invalid:
			x := e
			e = e.Next() // get next before removing current
			mb.buffer.Remove(x)
			mb.nodeBuffer.msgRemoved(msg)
		}
	}

	return nil
}
