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

package checkpoint

import (
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// Represents a simple implementation of a Checkpointer.
// Whenever the local log reaches a checkpoint, SimpleCheckpointer sends a message to all other nodes.
// When a node receives a quorum of such messages, it creates a stable checkpoint.
type SimpleCheckpointer struct {
	// The channel announcing the checkpoint sequence numbers (provided by the manager on subscription).
	snChannel chan int32

	// Saves from which sequence numbers checkpoint messages have been received from which nodes.
	// pendingCheckpoints[5][6] == true means that node 6 sent a checkpoint message for sequence number 5
	pendingCheckpoints map[int32]map[int32]bool

	// All incoming checkpoint messages are funneled through this channel and processed sequentially.
	messageSerializer chan *receivedMessage
}

// Returns a new initialized SimpleCheckpointer.
func NewSimpleCheckpointer() *SimpleCheckpointer {
	return &SimpleCheckpointer{
		pendingCheckpoints: make(map[int32]map[int32]bool),
		messageSerializer:  make(chan *receivedMessage, messageSerializerBuffer),
	}
}

// Initializes SimpleCheckpointer.
// Subscribes to the Manager that issues sequence numbers
// at which SimpleCheckpointer will trigger a new instance of the checkpointing protocol.
func (c *SimpleCheckpointer) Init(mngr manager.Manager) {
	c.snChannel = mngr.SubscribeCheckpointer()
}

// Handles a received checkpoint message.
// Instead of being called directly by a messenger thread that handles a received message, sequentialMessageHandler is
// only executed sequentially by a single thread started from Start(). The messenger thread calls HandleMessage, which
// is only used feed all messages in a channel from which the caller of sequentialMessageHandler reads.
func (c *SimpleCheckpointer) sequentialMessageHandler(msg *pb.CheckpointMsg, senderID int32) {
	logger.Trace().Int32("sn", msg.Sn).Int32("from", senderID).Msg("Handling checkpoint message.")

	// Only process messages that are not obsolete.
	// A message is obsolete if there already exists a stable checkpoint with a higher or equal sequence number.
	if log.GetCheckpoint() == nil || log.GetCheckpoint().Sn < msg.Sn {

		// If this is the first checkpoint message for this sequence number, allocate a new map for the SN.
		if c.pendingCheckpoints[msg.Sn] == nil {
			c.pendingCheckpoints[msg.Sn] = make(map[int32]bool)
		}

		// Register received checkpoint message.
		c.pendingCheckpoints[msg.Sn][senderID] = true

		// If enough messages have been received to create a stable checkpoint
		if len(c.pendingCheckpoints[msg.Sn]) >= membership.Quorum() {
			logger.Info().Int32("sn", msg.Sn).Msg("New stable checkpoint.")

			// Generate dummy empty proof
			proof := make(map[int32][]byte)
			for peerID, _ := range c.pendingCheckpoints[msg.Sn] {
				proof[peerID] = []byte{}
			}

			// Submit new stable checkpoint to the log
			log.CommitCheckpoint(&pb.StableCheckpoint{
				Sn:    msg.Sn,
				Proof: proof,
			})

			// Delete local data related to previous checkpoints.
			c.pruneOldCheckpoints(msg.Sn)
		}
	}
}

// Handles a message received by the messenger (this method is registered with the messanger as the message handler
// callback). It only feeds all messages to a channel for further sequential processing.
func (c *SimpleCheckpointer) HandleMessage(msg *pb.CheckpointMsg, senderID int32) {
	c.messageSerializer <- &receivedMessage{
		msg:      msg,
		senderID: senderID,
	}
}

// Starts SimpleCheckpointer. After the call to Start(), SimpleCheckpointer starts:
// - Processing incoming messages
// - Observing the log and triggering new instances of the checkpointing protocol when instruted by the Manager
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (c *SimpleCheckpointer) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	// Start a separate message processing thread. Only this thread will process all checkpoint messages.
	syncChan := make(chan bool) // Anonymous goroutine writes a value here when done.
	go func() {
		// Read messages from the serializer and execute the sequential handler function
		for rec, ok := <-c.messageSerializer; ok; rec, ok = <-c.messageSerializer {
			c.sequentialMessageHandler(rec.msg, rec.senderID)
		}

		// Notify parent that processing is done.
		syncChan <- true
	}()

	// Listen on snChannel until the manager announces a sequence number that should be checkpointed.
	for sn, ok := <-c.snChannel; ok; sn, ok = <-c.snChannel {
		c.launchInstance(sn)
	}

	// Wait for message processing goroutine.
	<-syncChan
}

// Starts an instance of the checkpointing protocol. For SimpleCheckpointer, this only means sending a checkpoint
// message to everyone (including oneself).
func (c *SimpleCheckpointer) launchInstance(sn int32) {

	// Create a checkpoint message.
	checkpointMsg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Msg: &pb.ProtocolMessage_Checkpoint{Checkpoint: &pb.CheckpointMsg{
			Sn:     sn,
			Digest: []byte("This is a checkpoint message"),
		}},
	}

	// Send checkpoint message to all nodes.
	for _, nodeID := range membership.AllNodeIDs() {
		messenger.EnqueuePriorityMsg(checkpointMsg, nodeID)
	}
}

// Delete local data related to checkpoint with sequence numbers lower than stableSN.
func (c *SimpleCheckpointer) pruneOldCheckpoints(stableSN int32) {
	for pendingSN := range c.pendingCheckpoints {
		if pendingSN <= stableSN {
			logger.Debug().Int32("sn", pendingSN).Msg("Removing old checkpoint data.")
			delete(c.pendingCheckpoints, pendingSN)
		}
	}
}

func (c *SimpleCheckpointer) GetPendingCheckpoints() []*pb.CheckpointMsg {
	cps := make([]*pb.CheckpointMsg, 0, 0)
	for sn, cp := range c.pendingCheckpoints {
		if cp == nil {
			continue
		}
		if cp[membership.OwnID] {
			cps = append(cps, &pb.CheckpointMsg{Sn: sn})
		}
	}
	return cps
}
