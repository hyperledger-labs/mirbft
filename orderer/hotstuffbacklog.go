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

// TODO limit backlog size

import (
	logger "github.com/rs/zerolog/log"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// Backlogs messages by height
type hotStuffBacklog struct {
	hi          *hotStuffInstance
	backlogMsgs map[int32][]*pb.ProtocolMessage
}

func newHotStuffBacklog(hi *hotStuffInstance) *hotStuffBacklog {
	b := &hotStuffBacklog{
		hi:          hi,
		backlogMsgs: make(map[int32][]*pb.ProtocolMessage),
	}
	return b
}

func (b *hotStuffBacklog) add(height int32, msg *pb.ProtocolMessage) {
	logger.Debug().
		Int32("height", height).
		Int32("sn", msg.Sn).
		Int32("senderId", msg.SenderId).
		Msg("Adding message to HotStuff backlog")
	if _, ok := b.backlogMsgs[height]; !ok {
		b.backlogMsgs[msg.Sn] = make([]*pb.ProtocolMessage, 0, 0)
	}
	b.backlogMsgs[height] = append(b.backlogMsgs[height], msg)
}

// Returns the messages of the height  to their queues
func (b *hotStuffBacklog) process(height int32) {
	for _, msg := range b.backlogMsgs[height] {
		b.hi.serializer.channel <- msg
	}
}
