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
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// TODO limit backlog size?

// The backlog of a pbft instance
// Holds a map entry for each view
type pbftBacklog struct {
	pi          *pbftInstance
	backlogMsgs map[int32][]*pb.ProtocolMessage
}

func newPbftBacklog(pi *pbftInstance) *pbftBacklog {
	b := &pbftBacklog{
		pi:          pi,
		backlogMsgs: make(map[int32][]*pb.ProtocolMessage),
	}
	return b
}

func (b *pbftBacklog) addMessage(msg *pb.ProtocolMessage, view int32) {
	if _, ok := b.backlogMsgs[view]; !ok {
		b.backlogMsgs[view] = make([]*pb.ProtocolMessage, 0, 0)
	}
	b.backlogMsgs[view] = append(b.backlogMsgs[view], msg)
}

// Returns the messages of the new view to their queues
func (b *pbftBacklog) process(view int32) {
	for _, msg := range b.backlogMsgs[view] {
		b.pi.serializer.channel <- msg
	}
}

// Delete all entries from previous views
func (b *pbftBacklog) prune(view int32) {
	for backlogView, _ := range b.backlogMsgs {
		if backlogView < view {
			delete(b.backlogMsgs, backlogView)
		}
	}
}
