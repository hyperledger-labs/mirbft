/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: This is the original old code with very few modifications.
//       Go through all of it, comment what is to be kept and delete what is not needed.

package deploytest

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"sync"
)

type FakeLink struct {
	FakeTransport *FakeTransport
	Source        t.NodeID
}

func (fl *FakeLink) Send(dest t.NodeID, msg *messagepb.Message) error {
	fl.FakeTransport.Send(fl.Source, dest, msg)
	return nil
}

func (fl *FakeLink) ReceiveChan() <-chan modules.ReceivedMessage {
	return fl.FakeTransport.NodeSinks[fl.Source]
}

type FakeTransport struct {
	// Buffers is source x dest
	Buffers   [][]chan *messagepb.Message
	NodeSinks []chan modules.ReceivedMessage
	WaitGroup sync.WaitGroup
	DoneC     chan struct{}
}

func NewFakeTransport(nodes int) *FakeTransport {
	buffers := make([][]chan *messagepb.Message, nodes)
	nodeSinks := make([]chan modules.ReceivedMessage, nodes)
	for i := 0; i < nodes; i++ {
		buffers[i] = make([]chan *messagepb.Message, nodes)
		for j := 0; j < nodes; j++ {
			if i == j {
				continue
			}
			buffers[i][j] = make(chan *messagepb.Message, 10000)
		}
		nodeSinks[i] = make(chan modules.ReceivedMessage)
	}

	return &FakeTransport{
		Buffers:   buffers,
		NodeSinks: nodeSinks,
		DoneC:     make(chan struct{}),
	}
}

func (ft *FakeTransport) Send(source, dest t.NodeID, msg *messagepb.Message) {
	select {
	case ft.Buffers[int(source)][int(dest)] <- msg:
	default:
		fmt.Printf("Warning: Dropping message %T from %d to %d\n", msg.Type, source, dest)
	}
}

func (ft *FakeTransport) Link(source t.NodeID) *FakeLink {
	return &FakeLink{
		Source:        source,
		FakeTransport: ft,
	}
}

func (ft *FakeTransport) RecvC(dest t.NodeID) <-chan modules.ReceivedMessage {
	return ft.NodeSinks[int(dest)]
}

func (ft *FakeTransport) Start() {
	for i, sourceBuffers := range ft.Buffers {
		for j, buffer := range sourceBuffers {
			if i == j {
				continue
			}

			ft.WaitGroup.Add(1)
			go func(i, j int, buffer chan *messagepb.Message) {
				// fmt.Printf("Starting drain thread from %d to %d\n", i, j)
				defer ft.WaitGroup.Done()
				for {
					select {
					case msg := <-buffer:
						// fmt.Printf("Sending message from %d to %d\n", i, j)
						select {
						case ft.NodeSinks[j] <- modules.ReceivedMessage{
							Sender: t.NodeID(i),
							Msg:    msg,
						}:
						case <-ft.DoneC:
							return
						}
					case <-ft.DoneC:
						return
					}
				}
			}(i, j, buffer)
		}
	}
}

func (ft *FakeTransport) Stop() {
	close(ft.DoneC)
	ft.WaitGroup.Wait()
}
