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
	"fmt"
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/announcer"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/tracing"
	"github.com/hyperledger-labs/mirbft/validator"
)

// Represents a dummy Orderer implementation (a stub).
// For each sequence number in each Segment received from the Manager, the leader (defined to be the first node in the
// Segment's leader list, never changes) sends a single message with dummy data to the followers. A follower receiving
// such a message directly announces a log Entry with that data as the decision for that sequence number.
type DummyOrderer struct {

	// Channel to which the Manager pushes new Segments.
	segmentChan chan manager.Segment
}

// This function is called by the messenger each time an Orderer-issued message is received over the network.
// DummyOrderer only knows one such message, through which the leader proposes a decision for a sequence number.
// DummyOrderer directly announces the contents of the message as the decision, removing the corresponding requests
// from the Bucket of pending requests.
func (do *DummyOrderer) HandleMessage(msg *pb.ProtocolMessage) {

	// Convenience variable
	senderID := msg.SenderId

	// Check the tye of the message.
	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_Dummy:

		if !validator.Validate(m.Dummy.Sn, m.Dummy.Batch, senderID) {
			logger.Fatal().Msg("Implement behavior when validation fails!")
		}

		// DummyOrderer only knows the "Dummy" message
		dummyMsg := m.Dummy

		// Add an event to the trace
		tracing.MainTrace.Event(tracing.PREPREPARE, int64(dummyMsg.Sn), 0)

		// Process requests in the received message
		batch := &request.Batch{Requests: make([]*request.Request, len(dummyMsg.Batch.Requests), len(dummyMsg.Batch.Requests))}
		for i, reqMsg := range dummyMsg.Batch.Requests {
			if req := request.AddReqMsg(reqMsg); req == nil {
				logger.Warn().
					Int32("sn", dummyMsg.Sn).
					Int32("clId", reqMsg.RequestId.ClientId).
					Int32("clSn", reqMsg.RequestId.ClientSn).
					Msg("Failed to add / locate request received from leader.")

				// WARNING: We are abusing the request package here!
				// (But since we're in the dummy orderer, everything is allowed.)
				// Normally this would mean that we are very behind (and we should fetch state)
				// or the leader is faulty. We should ignore the request and the whole batch.
				// Here, however, we circumvent the request package and create our own dummy Request struct
				// only for the purpose of delivering it.
				batch.Requests[i] = request.UglyUglyDummyRegisterRequest(reqMsg)
			} else {
				batch.Requests[i] = req
			}
		}

		request.RemoveBatch(batch)

		// Announce decision.
		announcer.Announce(&log.Entry{Sn: dummyMsg.Sn, Batch: dummyMsg.Batch})
	default:
		logger.Error().
			Str("msg", fmt.Sprint(m)).
			Msg("DummyOrderer cannot handle message. Unknown message type.")
	}

}

func (do *DummyOrderer) HandleEntry(entry *log.Entry) {
	panic("DummyOrderer does not yet implement HandleEntry().")
}

// Initializes the DummyOrderer by subscribing to new segments issued by the Manager.
func (do *DummyOrderer) Init(mngr manager.Manager) {
	do.segmentChan = mngr.SubscribeOrderer()
	// TODO initialize a backlog to store messages that arrive before the orderer starts
}

// Starts the DummyOrderer. Listens on the channel where the Manager issues new Segemnts and starts a goroutine to
// handle each of them.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (do *DummyOrderer) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	for s, ok := <-do.segmentChan; ok; s, ok = <-do.segmentChan {

		logger.Info().
			Int("segId", s.SegID()).
			Int32("firstSN", s.FirstSN()).
			Int32("lastSN", s.LastSN()).
			Int32("startsAfter", s.StartsAfter()).
			Int32("len", s.Len()).
			Msg("DummyOrderer received new a segment.")

		go do.runSegment(s)
	}
}

// Runs the dummy ordering algorithm for a Segment.
// The algorithm only consists of the leader sending a proposal to the followers (which accept it unconditionally).
func (do *DummyOrderer) runSegment(seg manager.Segment) {

	logger.Info().Int("segID", seg.SegID()).Msg("Running segment.")

	// If I am the leader
	if do.leading(seg) {

		// Wait until the log progressed far enough
		logger.Info().
			Int("segID", seg.SegID()).
			Int32("sn", seg.StartsAfter()).
			Msg("Waiting for log entry.")
		log.WaitForEntry(seg.StartsAfter())
		logger.Info().Int("segID", seg.SegID()).Msg("Leading segment.")

		// Send a proposal for each sequence number in the Segment.
		for _, sn := range seg.SNs() {
			do.proposeSN(seg, sn)
		}
	}
}

// Returns true if this node is the leader of a segment.
// In dummy ordering, the first node of the leader list is always the leader.
func (do *DummyOrderer) leading(seg manager.Segment) bool {
	return seg.Leaders()[0] == membership.OwnID
}

// Proposes a new (dummy) value for sequence number sn in Segment segment by sending a proposal message to all
// followers of the segment.
func (do *DummyOrderer) proposeSN(segment manager.Segment, sn int32) {

	logger.Trace().Int32("sn", sn).Msg("Creating proposal.")

	// Create request batch and mark it as "in flight"
	batch := segment.Buckets().CutBatch(config.Config.BatchSize, config.Config.BatchTimeout)

	// Create message
	orderMsg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Msg: &pb.ProtocolMessage_Dummy{Dummy: &pb.DummyOrdererMsg{
			Sn:    sn,
			Batch: batch.Message(),
		}},
	}
	tracing.MainTrace.Event(tracing.PROPOSE, int64(sn), 0)
	logger.Debug().
		Int32("sn", sn).
		Int("nReq", len(orderMsg.
			Msg.(*pb.ProtocolMessage_Dummy).Dummy.
			Batch.Requests)).
		Msg("Proposing.")

	// Enqueue the message for all followers
	for _, nodeID := range segment.Followers() {
		messenger.EnqueueMsg(orderMsg, nodeID)
	}
}

func (do *DummyOrderer) Sign(data []byte) ([]byte, error) {
	return nil, nil
}

func (do *DummyOrderer) CheckSig(data []byte, senderID int32, signature []byte) error {
	return nil
}
