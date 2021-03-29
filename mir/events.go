/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mir

import (
	pb "github.com/IBM/mirbft/protos"
)

type Executable interface {
	Execute(*Dispatcher)
}

//Timer

type Timer struct {
	tf      func()
	execute bool
}

func (t *Timer) Cancel() {
	t.execute = false
}

func (t *Timer) Execute(adapter *Dispatcher) {
	if t.execute {
		t.tf()
	}
}

//Batch

type batchEvent struct {
	batch []*pb.Request
	seq   pb.SeqView
}

func (m *batchEvent) Execute(adapter *Dispatcher) {
	adapter.handleNewBatchToPropose(m.batch, m.seq)
}

//Message

type msgEvent struct {
	msg *pb.Msg
	src uint64
}

func (m *msgEvent) Execute(adapter *Dispatcher) {
	adapter.handleMessage(m.msg, m.src)
}

//Request

type requestEvent struct {
	msg *pb.Msg
	src uint64
}

func (r *requestEvent) Execute(adapter *Dispatcher) {
	//adapter.handleRequest(r.msg)
	adapter.Request(r.msg, r.src)
}

//Connection

type connectionEvent struct {
	peerid uint64
}

func (c *connectionEvent) Execute(adapter *Dispatcher) {
	adapter.handleConnection(c.peerid)
}

//Backlog
type backlogEvent struct {
	process bool
	src     uint64
}

func (b *backlogEvent) Execute(adapter *Dispatcher) {
	adapter.handleBacklogRequest(b.process, b.src)
}

//Completion
type completedEvent struct {
	seq uint64
}

func (c *completedEvent) Execute(adapter *Dispatcher) {
	adapter.handleCompleted(c.seq)
}

//stopping
type stopEvent struct {
	stop uint64
}

func (c *stopEvent) Execute(adapter *Dispatcher) {
	adapter.handleStop(c.stop)
}
