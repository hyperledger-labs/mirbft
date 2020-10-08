/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type applyable int

const (
	past applyable = iota
	current
	future
	invalid
)

// TODO base this buffer on size, not count
type msgBuffer struct {
	buffer   *list.List
	logger   Logger
	myConfig *pb.StateEvent_InitialParameters
}

func newMsgBuffer(myConfig *pb.StateEvent_InitialParameters, logger Logger) *msgBuffer {
	return &msgBuffer{
		buffer:   list.New(),
		logger:   logger,
		myConfig: myConfig,
	}
}

func (mb *msgBuffer) store(msg *pb.Msg) {
	mb.buffer.PushBack(msg)
	if uint32(mb.buffer.Len()) > mb.myConfig.BufferSize {
		e := mb.buffer.Front()
		oldMsg := mb.buffer.Remove(e).(*pb.Msg)
		mb.logger.Warn(fmt.Sprintf("dropping message of type %T", oldMsg.Type))
	}
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
		case current:
			mb.buffer.Remove(e)
			return msg
		case future:
			e = e.Next()
		case invalid:
			x := e
			e = e.Next() // get next before removing current
			mb.buffer.Remove(x)
		}
	}

	return nil
}
