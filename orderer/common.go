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
	"sync/atomic"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/manager"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

const backlogSize = 10000
const channelSize = 10000 //we need buffered channel that writers don't hang after reader stops

type ordererChannel struct {
	channel chan *pb.ProtocolMessage
	stopped int32
}

func newOrdererChannel(size int) *ordererChannel {
	return &ordererChannel{
		channel: make(chan *pb.ProtocolMessage, size),
		stopped: 0,
	}
}

func (oc *ordererChannel) stop() {
	// The channel oc.channel is never closed (as it might have concurrent writers).
	// It will be eventually garbage collected when no goroutines reference it any more, whether it is closed or not.
	// Instead we use the stopped flag to make sure we don't write to oc.channel any more.
	// Once the ordererChannel is stopped, messages written to it might never be read. This is not a problem, however,
	// since the ordererChannel is only stopped when any further messages written to it are irrelevant.
	// We only need to make sure that the channel has sufficient capacity to store the (irrelevant) messages concurrent
	// writers might write while the channel is being stopped.
	atomic.StoreInt32(&oc.stopped, 1)
}

func (oc *ordererChannel) serialize(value *pb.ProtocolMessage) (closed bool) {
	if atomic.LoadInt32(&oc.stopped) == 0 {
		oc.channel <- value
		return false
	} else {
		return true
	}
}

type backlog struct {
	epochLast   int32
	gc          chan int32
	messages    *ordererChannel
	subscribers chan backlogSubscriber
	backlog     map[int32][]*pb.ProtocolMessage
	dispatcher  map[int32]*ordererChannel
}

type backlogSubscriber struct {
	segment    manager.Segment
	serializer *ordererChannel
}

func newBacklog() backlog {
	b := backlog{
		epochLast:   -1,
		dispatcher:  make(map[int32]*ordererChannel),
		backlog:     make(map[int32][]*pb.ProtocolMessage),
		gc:          make(chan int32),
		subscribers: make(chan backlogSubscriber, backlogSize),
		messages:    newOrdererChannel(backlogSize),
	}
	go b.process()
	return b
}

// Returns false if the value was not added because the channel is closed
func (b *backlog) add(value *pb.ProtocolMessage) bool {
	return b.messages.serialize(value)
}

func (b *backlog) process() {
	logger.Debug().Msg("Starting backlog.")

	var ok = true // set to false if any the message channels is stopChannel
	var msg *pb.ProtocolMessage
	var s backlogSubscriber
	var epochLast int32

	for ok {
		select {
		case s, ok = <-b.subscribers:
			if ok {
				b.dispatchToInstance(s)
			}
		default:
			select {
			case msg, ok = <-b.messages.channel:
				if ok {
					b.appendToBacklog(msg)
				}
			case s, ok = <-b.subscribers:
				if ok {
					b.dispatchToInstance(s)
				}
			case epochLast, ok = <-b.gc:
				if ok {
					b.garbageCollect(epochLast)
				}
			}
		}
	}
}

func (b *backlog) appendToBacklog(msg *pb.ProtocolMessage) {
	// If the message is from a previous epoch discard it
	if msg.Sn <= b.epochLast {
		logger.Debug().
			Int32("sn", msg.Sn).
			Int32("senderID", msg.SenderId).
			Msg("Message from previous epoch. Not adding to backlog.")
		return
	}
	logger.Debug().
		Int32("sn", msg.Sn).
		Int32("senderID", msg.SenderId).
		Msg("Adding message to backlog.")

	// If we already have access to the serializer of the Pbft instance put the message there
	if serilizer, ok := b.dispatcher[msg.Sn]; ok {
		serilizer.serialize(msg)
	}

	// Else create a map entry in the backlog if not available
	var entry []*pb.ProtocolMessage
	var ok bool
	if entry, ok = b.backlog[msg.Sn]; !ok {
		entry = make([]*pb.ProtocolMessage, 0, 0)
	}

	// And put the message there
	entry = append(entry, msg)
	b.backlog[msg.Sn] = entry
}

func (b *backlog) dispatchToInstance(s backlogSubscriber) {
	logger.Debug().
		Int("segID", s.segment.SegID()).
		Msg("Subscribing to backlog.")
	// Add a serializer to the dispatcher for each sn of the segment
	for _, sn := range s.segment.SNs() {
		b.dispatcher[sn] = s.serializer
		// If there are already messages in the backlog for ths sequence number
		if entry, ok := b.backlog[sn]; ok {
			// Dump them in the serializer
			logger.Debug().
				Int("segID", s.segment.SegID()).Int32("sn", sn).Msgf("Found %d in backlog.", len(entry))
			for _, msg := range entry {
				logger.Debug().
					Int("segID", s.segment.SegID()).Int32("sn", sn).Int32("senderID", msg.SenderId).Msg("Retrieved message from backlog.")
				s.serializer.serialize(msg)
			}
		}
	}
}

func (b *backlog) garbageCollect(epochLast int32) {
	b.epochLast = epochLast
	for sn, _ := range b.backlog {
		if sn <= epochLast {
			delete(b.backlog, sn)
		}
	}
	for sn, _ := range b.dispatcher {
		if sn <= epochLast {
			delete(b.dispatcher, sn)
		}
	}
}

// Common helper methods

func min(a, b int32) int32 {
	if a <= b {
		return a
	}
	return b
}

func segmentLeader(seg manager.Segment, view int32) int32 {
	return seg.Leaders()[view%int32(len(seg.Leaders()))]
}
