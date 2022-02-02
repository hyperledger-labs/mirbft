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
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"

	"fmt"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// Represents a simple implementation of a Checkpointer.
// Whenever the local log reaches a checkpoint, SigningCheckpointer sends a signed message to all other nodes
// containing the Merkle tree root of the batches between this and the last checkpoint.
// When a node receives a quorum of such messages, it creates a stable checkpoint.
type SigningCheckpointer struct {
	// The last local (not necessarily stable) checkpoint
	lastCp int32

	// The channel announcing the checkpoint sequence numbers (provided by the manager on subscription).
	snChannel chan int32

	// Saves from which sequence numbers checkpoint messages have been received from which nodes.
	// pendingCheckpoints[5][6] == true means that node 6 sent a checkpoint message for sequence number 5
	pendingCheckpoints map[int32]map[int32]*pb.CheckpointMsg

	// All incoming checkpoint messages are funneled through this channel and processed sequentially.
	messageSerializer chan *receivedMessage
}

// Returns a new initialized SimpleCheckpointer.
func NewSigningCheckpointer() *SigningCheckpointer {
	return &SigningCheckpointer{
		pendingCheckpoints: make(map[int32]map[int32]*pb.CheckpointMsg),
		messageSerializer:  make(chan *receivedMessage, messageSerializerBuffer),
	}
}

// Initializes SimpleCheckpointer.
// Subscribes to the Manager that issues sequence numbers
// at which SimpleCheckpointer will trigger a new instance of the checkpointing protocol.
func (c *SigningCheckpointer) Init(mngr manager.Manager) {
	c.snChannel = mngr.SubscribeCheckpointer()
}

// Handles a received checkpoint message.
// Instead of being called directly by a messenger thread that handles a received message, sequentialMessageHandler is
// only executed sequentially by a single thread started from Start(). The messenger thread calls HandleMessage, which
// is only used feed all messages in a channel from which the caller of sequentialMessageHandler reads.
func (c *SigningCheckpointer) sequentialMessageHandler(msg *pb.CheckpointMsg, senderID int32) {
	logger.Trace().
		Int32("sn", msg.Sn).
		Int32("from", senderID).
		//Str("digest", crypto.BytesToStr(msg.Digest)).
		Msg("Handling CHECKPOINT message.")

	// Only process messages that are not obsolete.
	// A message is obsolete if there already exists a stable checkpoint with a higher or equal sequence number.
	if log.GetCheckpoint() != nil && log.GetCheckpoint().Sn > msg.Sn {
		return
	}

	// Validate the checkpoint signature
	err := validateCheckpointSignature(senderID, msg)
	if err != nil {
		logger.Error().
			Err(err).
			Int32("checkpoint", msg.Sn).
			Int32("senderID", membership.OwnID).
			Msg(err.Error())
	}

	// If this is the first checkpoint message for this sequence number, allocate a new map for the SN.
	if c.pendingCheckpoints[msg.Sn] == nil {
		c.pendingCheckpoints[msg.Sn] = make(map[int32]*pb.CheckpointMsg)
	}

	// Register received checkpoint message.
	c.pendingCheckpoints[msg.Sn][senderID] = msg

	// Check there are enough valid checkpoint messages for a stable checkpoint
	c.checkForStableCheckpoint(msg.Sn)
}

// Checks if enough matching messages have been received to create a stable checkpoint
// We require a (strong) quorum (2f+1). A weak quorum (f+1) is also enough
// but for a state transfer protocol a stong quorum allows for the following optimization:
// A node can transfer missing batches from one out of these 2f+1 nodes, while receiving  confirmations of hashes
// of these batches from f additional nodes to prevent ingress of batches from a Byzantine node.
func (c *SigningCheckpointer) checkForStableCheckpoint(sn int32) {
	if len(c.pendingCheckpoints[sn]) < membership.Quorum() {
		return
	}

	// Count checkpoint messages with matching digest -- there should be at least 2f+1 of them
	matching := make(map[string]map[int32]*pb.CheckpointMsg)
	for senderID, msg := range c.pendingCheckpoints[sn] {
		if _, ok := matching[crypto.BytesToStr(msg.Digest)]; !ok {
			matching[crypto.BytesToStr(msg.Digest)] = make(map[int32]*pb.CheckpointMsg)
		}
		matching[crypto.BytesToStr(msg.Digest)][senderID] = msg
	}

	// DEBUG
	if len(matching) > 1 {
		for digest, msgs := range matching {
			logger.Error().Str("digest", digest).Int("n", len(msgs)).Msg("Mismatching checkpoint digest.")
		}
		panic("Mismatching checkpoint messages.")
	}

	// Look for a set of 2f+1 matching checkpoint messages
	var stableCheckpointMsgs map[int32]*pb.CheckpointMsg
	for _, msgSet := range matching {
		if len(msgSet) >= membership.Quorum() {
			stableCheckpointMsgs = msgSet
			break
		}
	}
	if stableCheckpointMsgs == nil {
		return
	}

	logger.Info().Int32("sn", sn).Msg("New stable checkpoint.")

	// Create a proof from the signatures on the matching checkpoint digest
	proof := make(map[int32][]byte)
	for senderID, msg := range stableCheckpointMsgs {
		proof[senderID] = msg.Signature
	}

	// Submit new stable checkpoint to the log
	log.CommitCheckpoint(&pb.StableCheckpoint{
		Sn:    sn,
		Proof: proof,
	})

	// Delete local data related to previous checkpoints.
	c.pruneOldCheckpoints(sn)
}

// Handles a message received by the messenger (this method is registered with the messanger as the message handler
// callback). It only feeds all messages to a channel for further sequential processing.
func (c *SigningCheckpointer) HandleMessage(msg *pb.CheckpointMsg, senderID int32) {
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
func (c *SigningCheckpointer) Start(wg *sync.WaitGroup) {
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
func (c *SigningCheckpointer) launchInstance(sn int32) {

	// Create a checkpoint message.
	checkpointMsg, err := makeSignedCheckpoint(sn, c.lastCp)
	if err != nil {
		logger.Error().
			Err(err).
			Int32("checkpoint", sn).
			Int32("senderID", membership.OwnID).
			Msg(err.Error())
	}

	// Send checkpoint message to all nodes.
	for _, nodeID := range membership.AllNodeIDs() {
		messenger.EnqueuePriorityMsg(checkpointMsg, nodeID)
	}

	// Update the last local checkpoint
	c.lastCp = sn
}

// Delete local data related to checkpoint with sequence numbers lower than stableSN.
func (c *SigningCheckpointer) pruneOldCheckpoints(stableSN int32) {
	for pendingSN := range c.pendingCheckpoints {

		if pendingSN <= stableSN {
			logger.Debug().Int32("sn", pendingSN).Msg("Removing old checkpoint data.")
			delete(c.pendingCheckpoints, pendingSN)
		}
	}
}

func (c *SigningCheckpointer) GetPendingCheckpoints() []*pb.CheckpointMsg {
	cps := make([]*pb.CheckpointMsg, 0, 0)
	for sn, cp := range c.pendingCheckpoints {
		if cp == nil {
			continue
		}
		if cp[membership.OwnID] != nil {
			cps = append(cps, &pb.CheckpointMsg{Sn: sn})
		}
	}
	return cps
}

func makeSignedCheckpoint(sn, last int32) (*pb.ProtocolMessage, error) {
	// Checkpoint digest is the Merkle tree root of the digests of the batches from the previous checkpoint sequence
	// number (excluding) up until (including) the current checkpoint sequence number.
	batchDigests := make([][]byte, 0, 0)
	for i := last + 1; i <= sn; i++ {
		entry := log.GetEntry(i)
		batchDigests = append(batchDigests, entry.Digest)
	}
	digest := crypto.MerkleHashDigests(batchDigests)

	// Sing the checkpoint message
	sk, err := crypto.PrivateKeyFromBytes(membership.OwnPrivKey)
	if err != nil {
		return nil, fmt.Errorf("could not sign checkpoint message: %s", err)
	}
	hash := crypto.Hash(digest)
	signature, err := crypto.Sign(hash, sk)
	if err != nil {
		return nil, fmt.Errorf("could not sign checkpoint message: %s", err)
	}

	// Create the checkpoint message
	checkpointMsg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Msg: &pb.ProtocolMessage_Checkpoint{Checkpoint: &pb.CheckpointMsg{
			Sn:        sn,
			Digest:    digest,
			Signature: signature,
		}},
	}
	return checkpointMsg, nil
}

func validateCheckpointSignature(senderID int32, msg *pb.CheckpointMsg) error {
	pk, err := crypto.PublicKeyFromBytes(membership.NodeIdentity(senderID).PubKey)
	if err != nil {
		return fmt.Errorf("could not verify checkpoint signature: %s", err)
	}
	hash := crypto.Hash(msg.Digest)
	err = crypto.CheckSig(hash, pk, msg.Signature)
	if err != nil {
		return fmt.Errorf("could not verify checkpoint signature: %s", err)
	}
	return nil
}
