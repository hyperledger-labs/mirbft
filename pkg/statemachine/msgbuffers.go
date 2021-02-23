/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemachine

import (
	"container/list"
	"fmt"
	"github.com/IBM/mirbft/pkg/status"

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
			msgBufs:  map[*msgBuffer]struct{}{},
		}
	}

	return nb
}

func (nbs *nodeBuffers) status() []*status.NodeBuffer {
	var stats []*status.NodeBuffer
	for _, nb := range nbs.nodeMap {
		stats = append(stats, nb.status())
	}
	return stats
}

type nodeBuffer struct {
	id        nodeID
	logger    Logger
	myConfig  *pb.StateEvent_InitialParameters
	totalSize int

	// Set of pointers to msgBuffers tracked by this nodeBuffer.
	// Used for logging and status only.
	msgBufs map[*msgBuffer]struct{}
}

func (nb *nodeBuffer) logDrop(component string, msg *pb.Msg) {
	nb.logger.Log(LevelWarn, "dropping buffered msg", "component", component, "type", fmt.Sprintf("%T", msg.Type))
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

func (nb *nodeBuffer) addMsgBuffer(msgBuf *msgBuffer) {
	nb.msgBufs[msgBuf] = struct{}{}
}

func (nb *nodeBuffer) removeMsgBuffer(msgBuf *msgBuffer) {
	delete(nb.msgBufs, msgBuf)
}

func (nb *nodeBuffer) status() *status.NodeBuffer {
	var msgBufStatuses []*status.MsgBuffer
	totalMsgs := 0

	for mb := range nb.msgBufs {
		mbs := mb.status()
		msgBufStatuses = append(msgBufStatuses, mbs)
		totalMsgs += mbs.Msgs
	}

	return &status.NodeBuffer{
		ID:         uint64(nb.id),
		Size:       nb.totalSize,
		Msgs:       totalMsgs,
		MsgBuffers: msgBufStatuses,
	}
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
		oldMsg := mb.remove(e)
		mb.nodeBuffer.logDrop(mb.component, oldMsg)
	}
	mb.buffer.PushBack(msg)
	mb.nodeBuffer.msgStored(msg)
	if mb.buffer.Len() == 1 {
		// If this is the first message in this msgBuffer,
		// register this msgBuffer with the nodeBuffer.
		// Used for status reporting only.
		mb.nodeBuffer.addMsgBuffer(mb)
	}
}

func (mb *msgBuffer) remove(e *list.Element) *pb.Msg {
	msg := mb.buffer.Remove(e).(*pb.Msg)
	mb.nodeBuffer.msgRemoved(msg)
	if mb.buffer.Len() == 0 {
		// If the last message was removed,
		// deregister msgBuffer from nodeBuffer.
		// Used for status reporting only.
		mb.nodeBuffer.removeMsgBuffer(mb)
	}
	return msg
}

func (mb *msgBuffer) next(filter func(source nodeID, msg *pb.Msg) applyable) *pb.Msg {
	e := mb.buffer.Front()
	if e == nil {
		return nil
	}

	for e != nil {
		msg := e.Value.(*pb.Msg)
		switch filter(mb.nodeBuffer.id, msg) {
		case past:
			x := e
			e = e.Next() // get next before removing current
			mb.remove(x)
		case current:
			mb.remove(e)
			return msg
		case future:
			e = e.Next()
		case invalid:
			x := e
			e = e.Next() // get next before removing current
			mb.remove(x)
		}
	}

	return nil
}

func (mb *msgBuffer) iterate(
	filter func(source nodeID, msg *pb.Msg) applyable,
	apply func(source nodeID, msg *pb.Msg),
) {
	e := mb.buffer.Front()
	for e != nil {
		msg := e.Value.(*pb.Msg)
		x := e
		e = e.Next()
		switch filter(mb.nodeBuffer.id, msg) {
		case past:
			mb.remove(x)
		case current:
			mb.remove(x)
			apply(mb.nodeBuffer.id, msg)
		case future:
		case invalid:
			mb.remove(x)
		}
	}
}

func (mb *msgBuffer) status() *status.MsgBuffer {
	totalSize := 0
	for e := mb.buffer.Front(); e != nil; e = e.Next() {
		totalSize += proto.Size(e.Value.(*pb.Msg))
	}

	return &status.MsgBuffer{
		Component: mb.component,
		Size:      totalSize,
		Msgs:      mb.buffer.Len(),
	}
}
