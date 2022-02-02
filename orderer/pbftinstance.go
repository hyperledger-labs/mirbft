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
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/announcer"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/statetransfer"
	"github.com/hyperledger-labs/mirbft/tracing"
)

const (
	catchupDelay = 400 * time.Millisecond
)

// TODO: Consolidate the segment-internal and the global checkpoints.

// Represents a PBFT instance implementation.
// PBFT instance is responsible for ordering sequence numbers from a single segment
type pbftInstance struct {
	view              int32                          // The view of the pbft instance
	segment           manager.Segment                // The segment of the instance
	orderer           *PbftOrderer                   // The pbft orderer
	batches           map[int32]map[int32]*pbftBatch // Protocol state per view per sequence number
	checkpointMsgs    map[int32]*pb.PbftCheckpoint   // Stores the received checkpoint messages
	checkpointDigests map[string][]int32             // Nodes that sent a checkpoint messages with a certain digest
	finalDigests      map[int32][]byte               // Digests batches obtained from a checkpoint (indexed by SN). Used for fetched state verification (not yet).
	checkpointTimer   *time.Timer                    // Timer for the segment checkpoint.
	viewChange        map[int32]*viewChangeInfo      // Information about view changes
	viewChangeTimeout time.Duration                  // View change duration timeout
	inViewChange      bool                           // True in view change mode, accepting only piority messages
	backlog           *pbftBacklog                   // A backlog for future views
	serializer        *ordererChannel                // Channel of common case messages
	priority          *ordererChannel                // Channel of priority messages
	cutBatch          chan struct{}                  // Channel for synchronizing batch cutting
	stopProp          sync.Once
	//	next              int // The index  of the next to be proposed SN
	startTs int64 // Timestamp of the start of the instance. Used for estimating duration of segment.
}

type pbftBatch struct {
	preprepareMsg   *pb.PbftPreprepare
	prepareMsgs     map[int32]*pb.PbftPrepare // Prepare messages received. Should be append only to prevent double voting.
	commitMsgs      map[int32]*pb.PbftCommit  // Commit messages received. Should be append only to prevent double voting.
	validCommitMsgs []*pb.PbftCommit          // Valid commit messages received. Should be append only to prevent double voting.
	lastCommitTs    int64
	batch           *request.Batch
	digest          []byte      // The digest of the proposal (preprepare) message
	preprepared     bool        // Is true if proposal (preprepare) received
	prepared        bool        // Is true if 2f unique prepare messages and a matching proposal received
	committed       bool        // Is true if 2f+1 unique commit messages and a matching proposal received
	viewChangeTimer *time.Timer // Timer to start a view change
}

type viewChangeInfo struct {
	view                       int32
	checkpoint                 *pb.CheckpointMsg        // checkpoint used for newView (-1 if there is none).
	s                          map[int32]*viewChangeMsg // ViewChange messages, one entry per node
	newView                    *pb.PbftNewView          // The new view message
	newViewTimer               *time.Timer              // Timer to start a view change
	enoughViewChanges          bool                     // When this flag is set, no more view changes are accepted.
	fetchingMissingPreprepares bool                     // Ignore incoming missing preprepares if this flag is false.
	reproposeBatches           map[int32]*pbftBatch     // PBFT batches to use when constructing the xset.
	// We abuse the pbftBatch data structure here to be able to store the digests
	// of missing batches. Other fields than digest and preprepareMsg are not used.
}

type viewChangeMsg struct {
	viewchange *pb.PbftViewChange
	signature  []byte
}

func (pi *pbftInstance) newViewChangeInfo(view int32) {
	viewChange := &viewChangeInfo{
		view: view,
		s:    make(map[int32]*viewChangeMsg),
		fetchingMissingPreprepares: false,
	}
	pi.viewChange[view] = viewChange

}

func (pi *pbftInstance) setNewViewTimer(view int32) {
	timeoutMsg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       -1, // SN -1 indicates that this is not a batch timeout, but a new view or checkpoint timeout.
		Msg: &pb.ProtocolMessage_Timeout{
			Timeout: &pb.Timeout{
				Sn:   -1,
				View: view,
			}},
	}

	pi.viewChange[view].newViewTimer = time.AfterFunc(pi.viewChangeTimeout, func() { pi.serializer.serialize(timeoutMsg) })
}

func (pi *pbftInstance) setCheckpointTimer() {
	// TODO: Consolidate the timers.

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       -1,
		Msg: &pb.ProtocolMessage_Timeout{
			Timeout: &pb.Timeout{
				Sn:   -1,
				View: pi.view,
			}},
	}

	if pi.checkpointTimer != nil {
		logger.Warn().Int32("view", pi.view).Msg("Overriding checkpoint timer.")
	}

	pi.checkpointTimer = time.AfterFunc(pi.viewChangeTimeout, func() { pi.serializer.serialize(msg) })
}

// Start initializes the Pbft instance
func (pi *pbftInstance) init(seg manager.Segment, orderer *PbftOrderer) {
	// Next indext of sn of the segment to propose
	// pi.next = 0

	// Attach segment to the instance
	pi.segment = seg

	// Attach orderer to the instance
	pi.orderer = orderer

	// Initialize backlog
	pi.backlog = newPbftBacklog(pi)

	//Initialize view change log
	pi.viewChange = make(map[int32]*viewChangeInfo)

	// Initialise protocol state
	pi.batches = make(map[int32]map[int32]*pbftBatch)
	pi.checkpointMsgs = make(map[int32]*pb.PbftCheckpoint)
	pi.checkpointDigests = make(map[string][]int32)
	// Non initializing final digests. Checked for nil in the code.
	pi.startView(0)

	// Initalize channel (Needs to happen before calling pi.setViewChangeTimer() to avoid a race condition.)
	pi.serializer = newOrdererChannel(channelSize)
	pi.priority = newOrdererChannel(channelSize)

	// Batch cutting synchronizer
	pi.cutBatch = make(chan struct{})

	// Start a timer for the first sequence number in the segment
	pi.setViewChangeTimer(seg.FirstSN(), 0)

	// Enable message handling
	pi.inViewChange = false

	// Set the starting timestamp
	pi.startTs = time.Now().UnixNano()
}

func (pi *pbftInstance) lead() {

	logger.Debug().Int("segID", pi.segment.SegID()).Msg("Leading segment.")
	batchSize := pi.segment.BatchSize()

	// Simulate a straggler.
	if membership.SimulatedCrashes[membership.OwnID] != nil && config.Config.CrashTiming == "Straggler" {
		config.Config.BatchTimeoutMs = int(0.5*float64(config.Config.ViewChangeTimeoutMs))
		config.Config.BatchTimeout = time.Duration(config.Config.BatchTimeoutMs) * time.Millisecond
		logger.Info().Str("byzantine", config.Config.CrashTiming).Int("batchTimeout", config.Config.BatchTimeoutMs)
		// we set the batchsize to an infinate practically size, so that we always wait for the timeout
		batchSize = 1000000000
	}



	// Send a proposal for each sequence number in the Segment.
	for _, sn := range pi.segment.SNs() {

		// Wait for a batch to be ready.
		// We must not cut the batch now, as, in case of a view change,
		// it might get stuck in the instance serializer buffer without being processed.
		// The actual batch cutting happens when handling this event (and thus on the critical path of the instance).
		// However, as we know that the batch is ready (by having waited here), we will set the timeout of the actual
		// batch cutting to 0. The signatures still need to be verified though, but the configuration option of early
		// request verification should alleviate this problem.
		logger.Debug().Int("batchSize", pi.segment.BatchSize()).Msg("Waiting for batch.")
		pi.segment.Buckets().WaitForRequests(batchSize, config.Config.BatchTimeout)
		logger.Debug().Int("batchSize", pi.segment.BatchSize()).Msg("Batch ready.")

		// Create message to serve as a placeholder for proposing a batch.
		msg := &pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       sn,
			Msg: &pb.ProtocolMessage_Newseqno{
				Newseqno: &pb.PbftPreprepare{
					Sn: sn,
					// In general, the view must be set by the serial processing thread.
					// Setting it here results in a race condition and maybe even incorrect in a corner case.
					// Currently, however, batches are only proposed for view 0.
					View:   0,
					Leader: membership.OwnID,
					Batch:  nil, // This will be filled in by the PBFT instance when this message is serialized.
				},
			},
		}
		pi.serializer.serialize(msg)

		// Wait until the batch is actually cut. Otherwise this goroutine would just loop quickly through
		// all sequence numbers as soon as there is more than BatchSize requests in the buckets.
		<-pi.cutBatch
	}
}

// Proposes a new value for sequence number sn in Segment segment by sending a proposal message to all
// followers of the segment.
func (pi *pbftInstance) proposeSN(preprepare *pb.PbftPreprepare, sn int32) {

	// Simulate a crash if configured so.
	if membership.SimulatedCrashes[membership.OwnID] != nil {

		if (config.Config.CrashTiming == "EpochStart" && sn == pi.segment.FirstSN()) ||
			(config.Config.CrashTiming == "EpochEnd" && sn == pi.segment.LastSN()) {

			logger.Info().Str("crashTiming", config.Config.CrashTiming).Msg("Simulating node crash.")
			messenger.Crashed = true

		}

	}

	// New batches are proposed only in view 0
	if pi.view > 0 {
		return
	}

	// Simulate a straggler.
	batchSize := pi.segment.BatchSize()
	if membership.SimulatedCrashes[membership.OwnID] != nil && config.Config.CrashTiming == "Straggler" {
			// we cut an empty batch to maximize damage
			batchSize = 0
	}

	// Create the actual request batch. The timeout is 0, since the we already waited for the batch in pi.lead().
	batch := pi.segment.Buckets().CutBatch(batchSize, 0)

	// Notify batch cutting goroutine that it can start waiting for the next batch.
	pi.cutBatch <- struct{}{}

	if config.Config.SignRequests {
		// TODO: Do something useful with the result of signature verification
		if err := batch.CheckSignatures(); err != nil {
			logger.Error().Msg("Signature verification of request in freshly cut batch failed.")
		}
	}
	batch.MarkInFlight()
	preprepare.Batch = batch.Message()

	// This is technically not necessary, as new batches are only proposed in view 0
	preprepare.View = pi.view

	logger.Info().Int32("sn", sn).
		Int32("view", pi.view).
		Int32("senderID", membership.OwnID).
		Int("nReq", len(preprepare.Batch.Requests)).
		Msg("Sending PREPREPARE.")

	// Add message to own log
	digest := pbftDigest(preprepare)
	pi.batches[pi.view][sn].digest = digest
	pi.batches[pi.view][sn].preprepareMsg = preprepare
	pi.batches[pi.view][sn].batch = batch
	pi.batches[pi.view][sn].preprepared = true

	// This value will be overwritten by receivers.
	// Setting it here, as this counts as local "reception" of the preprepare.
	// The timestamp is not part of the digest.
	preprepare.Ts = time.Now().UnixNano()

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_Preprepare{
			Preprepare: preprepare,
		},
	}

	tracing.MainTrace.Event(tracing.PROPOSE, int64(sn), int64(len(batch.Requests)))

	// Enqueue the message for all followers
	for _, nodeID := range pi.segment.Followers() {
		if nodeID != membership.OwnID {
			messenger.EnqueuePriorityMsg(msg, nodeID)
		}
	}
}

func (pi *pbftInstance) handlePreprepare(preprepare *pb.PbftPreprepare, msg *pb.ProtocolMessage) error {
	// Convenience variables
	sn := msg.Sn
	senderID := msg.SenderId

	logger.Info().Int32("sn", sn).
		Int32("senderID", senderID).
		Int("nReq", len(preprepare.Batch.Requests)).
		Msg("Handling PREPREPARE.")

	if sn != preprepare.Sn {
		return fmt.Errorf("malformed message form %d: header sequence number doesn't match", senderID)
	}
	if senderID != preprepare.Leader {
		pi.sendViewChange()
		return fmt.Errorf("malformed message: sender %d does not match leader %d", senderID, preprepare.Leader)
	}
	if !isLeading(pi.segment, preprepare.Leader, preprepare.View) {
		pi.sendViewChange()
		return fmt.Errorf("invalid leader %d for instance %d", preprepare.Leader, pi.segment.SegID())
	}
	// If the message is from a previous view, ignore
	if pi.view > preprepare.View {
		return fmt.Errorf("old view number %d, we are in view %d", preprepare.View, pi.view)
	}
	// If the message is for a future view, or if we are still inactive add in backlog
	if pi.view < preprepare.View || pi.inViewChange {
		pi.backlog.addMessage(msg, preprepare.View)
		logger.Debug().Int32("sn", sn).Int32("senderID", senderID).
			Msgf("Not yet active in view %d.", preprepare.View)
		return nil
	}
	// Must appear after trying to backlog the message.
	// Otherwise message that should be backlogged could be rejected, if the new view state is not yet initialized.
	if _, ok := pi.batches[pi.view][sn]; !ok {
		return fmt.Errorf("instance %d does not handle sequence number %d", pi.segment.SegID(), preprepare.Sn)
	}
	batch := pi.batches[pi.view][sn]
	// Check whether the batch has been already committed (this can be the case due to state transfer)
	if batch.committed {
		logger.Debug().Msg("Ignoring PREPREPARE message. Batch already committed.")
		return nil
	}
	// Check that no other batch is preprepared for the same sequence number in this view
	if batch.preprepareMsg != nil {
		pi.sendViewChange()
		return fmt.Errorf("duplicate preprepare from %d for sn %d", senderID, sn)
	}

	// Check that proposal requests are valid
	batch.batch = request.NewBatch(preprepare.Batch)
	if batch.batch == nil {
		logger.Error().Int32("peerId", senderID).Int32("sn", sn).Msg("Invalid requests in proposal.")
		pi.sendViewChange()
		return fmt.Errorf("proposal from %d contains invalid requests", senderID)
	}
	// Check that proposal does not contain preprepared ("in flight") requests.
	if err := batch.batch.CheckInFlight(); err != nil {
		return fmt.Errorf("proposal from %d contains in flight requests: %s", senderID, err.Error())
	}
	// Check that proposal does not contain requests that do not match the current active bucket
	if err := batch.batch.CheckBucket(pi.segment.Buckets().GetBucketIDs()); err != nil {
		return fmt.Errorf("proposal from %d contains in requests from invalid bucket: %s", senderID, err.Error())
	}
	// Mark requests as preprepared
	batch.batch.MarkInFlight()

	// Create new batch
	digest := pbftDigest(preprepare)
	batch.digest = digest
	batch.preprepareMsg = preprepare
	batch.preprepared = true

	pi.sendPrepare(batch)

	if !batch.prepared && isPrepared(batch) {
		batch.prepared = true
		pi.sendCommit(batch)
	}

	if !batch.committed && batch.CheckCommits() {

		////  TODO: Remove this!
		//// DEBUG
		//// Make 2 peers not commit anything in view 0
		//if (int32(pi.segment.SegID()) == membership.OwnID + 1 || int32(pi.segment.SegID()) == membership.OwnID + 2) && pi.view == 0 {
		//	logger.Warn().Int32("sn", sn).Int("segID", pi.segment.SegID()).Int32("ownID", membership.OwnID).Msg("DEBUG: not committing!")
		//	return nil
		//}

		pi.announce(batch, sn, preprepare.Batch, preprepare.Aborted, preprepare.Ts, batch.lastCommitTs)
	}

	return nil
}

func (pi *pbftInstance) sendPrepare(batch *pbftBatch) {

	//// DEBUG
	//if membership.OwnID < 21 && batch.preprepareMsg.Sn == 0 {
	//	return
	//}

	logger.Debug().Int32("sn", batch.preprepareMsg.Sn).
		Int32("view", pi.view).
		Int32("senderID", membership.OwnID).
		Msg("Sending PREPARE.")

	// Create message
	prepare := &pb.PbftPrepare{
		Sn:     batch.preprepareMsg.Sn,
		View:   pi.view,
		Digest: batch.digest,
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       batch.preprepareMsg.Sn,
		Msg: &pb.ProtocolMessage_Prepare{
			Prepare: prepare,
		},
	}

	// Add message to own log
	batch.prepareMsgs[membership.OwnID] = prepare

	// Enqueue the message for all other nodes
	for _, nodeID := range pi.segment.Followers() {
		if nodeID == membership.OwnID {
			continue
		}
		messenger.EnqueueMsg(msg, nodeID)
	}

}

func (pi *pbftInstance) handlePrepare(prepare *pb.PbftPrepare, msg *pb.ProtocolMessage) error {
	// Convenience variables
	sn := msg.Sn
	senderID := msg.SenderId

	if sn != prepare.Sn {
		return fmt.Errorf("malformed message from %d: header sequence number doesn't match", senderID)
	}
	// If the message is from a previous view, ignore
	if pi.view > prepare.View {
		return fmt.Errorf("old view number %d, we are in view %d", prepare.View, pi.view)
	}
	// If the message is for a future view, or if we are still inactive add in backlog
	if pi.view < prepare.View || pi.inViewChange {
		pi.backlog.addMessage(msg, prepare.View)
		logger.Debug().Int32("sn", sn).Int32("senderID", senderID).
			Msgf("Not yet active in view %d.", prepare.View)
		return nil
	}
	// Must appear after trying to backlog the message.
	// Otherwise message that should be backlogged could be rejected, if the new view state is not yet initialized.
	if _, ok := pi.batches[pi.view][sn]; !ok {
		return fmt.Errorf("instance %d does not handle sequence numer %d", pi.segment.SegID(), prepare.Sn)
	}

	batch := pi.batches[pi.view][sn]
	if _, ok := batch.prepareMsgs[senderID]; ok {
		return fmt.Errorf("duplicate prepare message from %d", senderID)
	}
	batch.prepareMsgs[senderID] = prepare

	if !batch.prepared && isPrepared(batch) {
		batch.prepared = true
		pi.sendCommit(batch)
	}

	if !batch.committed && batch.CheckCommits() {

		////  TODO: Remove this!
		//// DEBUG
		//// Make 2 peers not commit anything in view 0
		//if (int32(pi.segment.SegID()) == membership.OwnID + 1 || int32(pi.segment.SegID()) == membership.OwnID + 2) && pi.view == 0 {
		//	logger.Warn().Int32("sn", sn).Int("segID", pi.segment.SegID()).Int32("ownID", membership.OwnID).Msg("DEBUG: not committing!")
		//	return nil
		//}

		pi.announce(batch, sn, batch.preprepareMsg.Batch, batch.preprepareMsg.Aborted, batch.preprepareMsg.Ts, batch.lastCommitTs)
	}

	return nil
}

func (pi *pbftInstance) sendCommit(batch *pbftBatch) {
	logger.Debug().Int32("sn", batch.preprepareMsg.Sn).
		Int32("view", pi.view).
		Int32("senderID", membership.OwnID).
		Msg("Sending COMMIT.")

	// Create message
	commit := &pb.PbftCommit{
		Sn:     batch.preprepareMsg.Sn,
		View:   pi.view,
		Digest: batch.digest,
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       batch.preprepareMsg.Sn,
		Msg: &pb.ProtocolMessage_Commit{
			Commit: commit,
		},
	}

	// This value will be overwritten by receivers.
	// Setting it here, as this counts as local "reception" of the commit.
	// The timestamp is not part of the digest.
	commit.Ts = time.Now().UnixNano()

	// Add message to own log
	batch.commitMsgs[membership.OwnID] = commit

	// Enqueue the message for all other nodes
	for _, nodeID := range pi.segment.Followers() {
		if nodeID == membership.OwnID {
			continue
		}
		messenger.EnqueueMsg(msg, nodeID)
	}
}

func (pi *pbftInstance) handleCommit(commit *pb.PbftCommit, msg *pb.ProtocolMessage) error {
	// Convenience variables
	sn := msg.Sn
	senderID := msg.SenderId

	//logger.Trace().Int32("sn", sn).
	//	Int32("view", pi.view).
	//	Int32("senderID", senderID).
	//	Msg("Handling COMMIT.")

	if sn != commit.Sn {
		return fmt.Errorf("malformed message from %d: header sequence number doesn't match", senderID)
	}
	// If the message is from a previous view, ignore
	if pi.view > commit.View {
		return fmt.Errorf("old view number %d, we are in view %d", commit.View, pi.view)
	}
	// If the message is for a future view, or if we are still inactive add in backlog
	if pi.view < commit.View || pi.inViewChange {
		pi.backlog.addMessage(msg, commit.View)
		logger.Debug().Int32("sn", sn).Int32("senderID", senderID).
			Msgf("Not yet active in view %d.", commit.View)
		return nil
	}
	// Must appear after trying to backlog the message.
	// Otherwise message that should be backlogged could be rejected, if the new view state is not yet initialized.
	if _, ok := pi.batches[pi.view][commit.Sn]; !ok {
		return fmt.Errorf("instance %d does not handle sequence numer %d", pi.segment.SegID(), commit.Sn)
	}

	batch := pi.batches[pi.view][sn]
	// It is important to test for ok and not for the message not being nil,
	// as an explicit nil entry indicates a received and already validated message.
	if _, ok := batch.commitMsgs[senderID]; ok {
		return fmt.Errorf("duplicate commit message from %d", senderID)
	}
	batch.commitMsgs[senderID] = commit

	if !batch.committed && batch.CheckCommits() {

		////  TODO: Remove this!
		//// DEBUG
		//// Make 2 peers not commit anything in view 0
		//if (int32(pi.segment.SegID()) == membership.OwnID + 1 || int32(pi.segment.SegID()) == membership.OwnID + 2) && pi.view == 0 {
		//	logger.Warn().Int32("sn", sn).Int("segID", pi.segment.SegID()).Int32("ownID", membership.OwnID).Msg("DEBUG: not committing!")
		//	return nil
		//}

		pi.announce(batch, sn, batch.preprepareMsg.Batch, batch.preprepareMsg.Aborted, batch.preprepareMsg.Ts, batch.lastCommitTs)
	}

	return nil
}

func (pi *pbftInstance) handleMissingEntry(msg *pb.MissingEntry) {
	logger.Info().
		Int32("view", pi.view).
		Int32("sn", msg.Sn).
		Int("segID", pi.segment.SegID()).
		Msg("Handling MissingEntry.")

	batch := pi.batches[pi.view][msg.Sn]

	if !batch.committed {

		// TODO: Properly verify the incoming entry.
		//       Make sure that this is done appropriately whether the entry was fetched based on a high-level
		//       checkpoin or a segment-level checkpoint.

		if batch.batch != nil {
			batch.batch.Resurrect()
		}
		batch.batch = request.NewBatch(msg.Batch)
		batch.batch.MarkInFlight()
		batch.digest = msg.Digest
		// We must not touch the preprepared or prepared flag to prevent potential segfaults,
		// as the prepare messages and the preprepare message might still be absent.

		pi.announce(batch, msg.Sn, msg.Batch, msg.Aborted, pi.startTs, time.Now().UnixNano())
	}
}

func (pi *pbftInstance) announce(batch *pbftBatch, sn int32, reqBatch *pb.Batch, aborted bool, proposeTs int64, commitTs int64) {
	if batch.viewChangeTimer != nil {
		notFired := batch.viewChangeTimer.Stop()
		if !notFired {
			// This is harmelss, since the timeout, even though generated, will be ignored.
			logger.Warn().Int32("sn", sn).Msg("Timer fired concurrently with being canceled.")
		}
	}

	// Mark batch as committed.
	batch.committed = true

	// Remove batch requests
	request.RemoveBatch(batch.batch)

	logEntry := &log.Entry{
		Sn:        sn,
		Batch:     reqBatch,
		ProposeTs: proposeTs,
		CommitTs:  commitTs,
		Aborted:   aborted,
		Digest:    batch.digest,
	}
	// If the batch was aborted suspect the first leader of the segment
	if logEntry.Aborted {
		logEntry.Suspect = segmentLeader(pi.segment, 0)
	}
	// Announce decision.
	announcer.Announce(logEntry)

	// Start new view change timeout
	// for the fist uncommitted sequence number in the segment
	finished := true // Will be set to false if any SN is still uncommitted
	for _, sn := range pi.segment.SNs() {
		if !pi.batches[pi.view][sn].committed {
			pi.setViewChangeTimer(sn, 0)
			finished = false
			break
		}
	}

	// Submit own checkpoint message if all entries of the segment just have been committed.
	if finished {

		pi.sendCheckpoint()

		// If no segment checkpoint exists yet, start a timer for a view change if the checkpoint is not created soon.
		// This is required to help other peers that might be stuck in a future view. The high-level checkpoints are
		// not sufficient for this, as multiple segments might be blocking each other.
		if pi.finalDigests == nil {
			pi.setCheckpointTimer()
		}
	}
}

func (pi *pbftInstance) sendCheckpoint() {

	logger.Info().
		Int("segID", pi.segment.SegID()).
		Int32("lastSn", pi.segment.LastSN()).
		Msg("Sending segment checkpoint.")

	// Compute a Merkle hash of all the batches in the segment.
	digests := make([][]byte, pi.segment.Len())
	for i, sn := range pi.segment.SNs() {
		digests[i] = pi.batches[pi.view][sn].digest
	}

	chkpMsg := &pb.PbftCheckpoint{
		Digests: digests,
	}
	// Create checkpoint message
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       pi.segment.LastSN(),
		Msg:      &pb.ProtocolMessage_PbftCheckpoint{PbftCheckpoint: chkpMsg},
	}

	// Send message to all other peers
	for _, peerID := range pi.segment.Followers() {
		if peerID != membership.OwnID {
			messenger.EnqueueMsg(msg, peerID)
		}
	}

	// Insert message in own message log.
	// Technically this is not necessary, as the received checkpoint messages are only relevant for state transfer.
	// Since we already committed everything, we will never need that.
	if err := pi.handlePBFTCheckpoint(chkpMsg, membership.OwnID); err != nil {
		logger.Fatal().Err(err).Msg("Failed to handle own checkpoint.")
	}
}

func (pi *pbftInstance) handlePBFTCheckpoint(msg *pb.PbftCheckpoint, senderID int32) error {

	logger.Debug().
		Int("segID", pi.segment.SegID()).
		Int32("lastSn", pi.segment.LastSN()).
		Msg("Handling PBFT checkpoint.")

	if _, ok := pi.checkpointMsgs[senderID]; ok {
		return fmt.Errorf("discarding duplicate pbft checkpoint message from %d", senderID)
	}

	digestStr := crypto.BytesToStr(crypto.ParallelDataArrayHash(msg.Digests))

	pi.checkpointDigests[digestStr] = append(pi.checkpointDigests[digestStr], senderID)

	// Purposefully using == and not >=, so that the body of the condition is only executed once.
	if len(pi.checkpointDigests[digestStr]) == membership.Quorum() {

		logger.Info().
			Int("segID", pi.segment.SegID()).
			Int32("lastSn", pi.segment.LastSN()).
			Msg("PBFT checkpoint ready. Scheduling catchup.")

		// Cancel checkpoint timer if running
		if pi.checkpointTimer != nil {
			if !pi.checkpointTimer.Stop() {
				logger.Debug().
					Int32("view", pi.view).
					Int("segId", pi.segment.SegID()).
					Msg("Checkpoint timer fired concurrently with being canceled.")
			}
			pi.checkpointTimer = nil
		}

		// Save final batch digests obtained from checkpoint
		pi.finalDigests = make(map[int32][]byte)
		for i, sn := range pi.segment.SNs() {
			pi.finalDigests[sn] = msg.Digests[i]
		}

		// Try to catch up after some delay
		time.AfterFunc(catchupDelay, func() {
			pi.serializer.serialize(&pb.ProtocolMessage{
				SenderId: membership.OwnID,
				Sn:       pi.segment.LastSN(),
				Msg:      &pb.ProtocolMessage_PbftCatchup{PbftCatchup: &pb.PbftCatchUp{}},
			})
		})
	}

	return nil
}

func (pi *pbftInstance) catchUp() {

	// Find the list of nodes that agreed on the checkpoint
	var sources []int32
	for _, s := range pi.checkpointDigests {
		if len(s) >= membership.Quorum() {
			sources = s
			break
		}
	}

	// Ask for each sequence number that is not committed yet
	requests := 0
	for sn, batch := range pi.batches[pi.view] {
		if !batch.committed {
			requests++
			go statetransfer.FetchMissingEntry(sn, sources)
		}
	}

	logger.Info().
		Int("segID", pi.segment.SegID()).
		Int32("lastSn", pi.segment.LastSN()).
		Int("missingSns", requests).
		Msg("PBFT catching up.")
}

func (pi *pbftInstance) sendViewChange() {
	if config.Config.DisabledViewChange {
		tracing.MainTrace.Stop()
		logger.Fatal().Int("segID", pi.segment.SegID()).Msg("VIEWCHANGE disabled, peer exits.")
	}
	//Advance view
	pi.inViewChange = true
	pi.startView(pi.view + 1)

	p := make(map[int32]*pb.PbftPrepare)
	q := make(map[int32]*pb.PbftPrepare)

	// TODO: Make it possible for the Qset to contain more than one entry.
	//       According to the algorithm, if different batches have been preprepared in different views
	//       for a sequence nubmer, each schould have an entry in the Qset. Note that even for ISS an empty
	//       "aborted" batch is also a valid (and different) batch from this perspective.

	for v := int32(0); v < pi.view; v++ {
		if _, ok := pi.batches[v]; !ok {
			continue
		}
		for _, batch := range pi.batches[v] {
			if batch.prepared {
				p[batch.preprepareMsg.Sn] = &pb.PbftPrepare{Sn: batch.preprepareMsg.Sn, View: batch.preprepareMsg.View, Digest: batch.digest}
			}
			if batch.preprepareMsg != nil {
				q[batch.preprepareMsg.Sn] = &pb.PbftPrepare{Sn: batch.preprepareMsg.Sn, View: batch.preprepareMsg.View, Digest: batch.digest}
			}
		}
	}

	// TODO: get H and Cset from checkpoints
	// TODO: check if there is a checkpoint available
	// We keep the convention that the stable checkpoint is the first in the ccet array

	// TODO: We don't support non-stable checkpoints yet. When we do, cset must be a list of the stable and all non-stable
	//       checkpoints *within this segment*
	checkpoint := int32(-1) // This is -1 and not 0, because the number of a checkpoint indicates the last included SN.
	//if log.GetCheckpoint() != nil {
	//	checkpoint = log.GetCheckpoint().Sn
	//}

	cset := make([]*pb.CheckpointMsg, 0, 0)
	cset = append(cset, &pb.CheckpointMsg{Sn: checkpoint})
	viewchange := &pb.PbftViewChange{
		H:        checkpoint,
		Cset:     cset,
		View:     pi.view,
		Qset:     q,
		Pset:     p,
		SenderId: membership.OwnID,
	}
	data, err := proto.Marshal(viewchange)
	if err != nil {
		logger.Error().Err(err)
	}
	signature, err := pi.orderer.Sign(data)
	if err != nil {
		logger.Error().Err(err)
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		// The seq no should be a sequence number of the segment so that the
		// Orderer can dispatch the message to the correct instance.
		Sn: pi.segment.LastSN(),
		Msg: &pb.ProtocolMessage_Viewchange{
			Viewchange: &pb.SignedMsg{
				Data:      data,
				Signature: signature,
			},
		},
	}

	// Create an entry for view change if not already existing
	if _, ok := pi.viewChange[pi.view]; !ok {
		pi.newViewChangeInfo(pi.view)
	}

	// Set timer for trying the next view, if no VIEWCHANGE arrives before
	pi.setNewViewTimer(pi.view)

	nextLeaderID := pi.segment.Leaders()[pi.view%int32(len(pi.segment.Leaders()))]

	logger.Info().Int32("view", pi.view).
		Int("segID", pi.segment.SegID()).
		Int32("senderID", membership.OwnID).
		Int32("receiverID", nextLeaderID).
		Msg("Requesting VIEWCHANGE.")

	logger.Trace().Int("segID", pi.segment.SegID()).Msgf("PSet %v", p)
	logger.Trace().Int("segID", pi.segment.SegID()).Msgf("QSet %v", q)

	// If this instance is leading the segment in the new view,
	// put message in own log and check if a new view message can be sent
	if nextLeaderID == membership.OwnID {
		pi.viewChange[pi.view].s[membership.OwnID] = &viewChangeMsg{viewchange: viewchange, signature: signature}
		logger.Debug().
			Int32("currentView", pi.view).
			Int32("msgView", viewchange.View).
			Int("numViewChanges", len(pi.viewChange[pi.view].s)).
			Msg("Adding view change message.")

		pi.maybeSendNewView(pi.view)
		// Otherwise, send a new view to the leader.
		// View change messages are signed, so should we just send to next leader
	} else {
		messenger.EnqueuePriorityMsg(msg, nextLeaderID)
	}
}

func (pi *pbftInstance) handleViewChange(signed *pb.SignedMsg, senderID int32) error {
	// Validate signature
	err := pi.orderer.CheckSig(signed.Data, senderID, signed.Signature)
	if err != nil {
		return fmt.Errorf("invalid message signature from %d", senderID)
	}
	// Extract viewchange message from data
	viewchange := &pb.PbftViewChange{}
	err = proto.Unmarshal(signed.Data, viewchange)
	if err != nil {
		return fmt.Errorf("invalid message format from %d", senderID)
	}

	logger.Info().
		Int32("ownView", pi.view).
		Int32("msgView", viewchange.View).
		Int32("senderID", senderID).
		Int("segID", pi.segment.SegID()).
		Msg("Handling VIEWCHANGE.")

	// Ensure that the message is not form a previous view change
	view := viewchange.View
	if view < pi.view {
		return fmt.Errorf("old view change from %d for view %d, we are already in view %d", senderID, view, pi.view)
	}

	// Create an entry for view change if not already existing
	if _, ok := pi.viewChange[view]; !ok {
		pi.newViewChangeInfo(view)
	}

	// If enough view changes have already been received for a new view, ignore this one.
	if pi.viewChange[view].enoughViewChanges {
		logger.Debug().
			Int32("senderId", senderID).
			Int32("view", view).
			Msg("Enough view changes received, ignoring view change.")
		return nil
	}

	// Enforce only one view change for each replica per view
	if _, ok := pi.viewChange[view].s[senderID]; ok {
		return fmt.Errorf("duplicate view change for %d from %d", view, senderID)
	}

	pi.viewChange[view].s[senderID] = &viewChangeMsg{viewchange: viewchange, signature: signed.Signature}
	logger.Info().
		Int32("currentView", pi.view).
		Int32("msgView", viewchange.View).
		Int("numViewChanges", len(pi.viewChange[view].s)).
		Msg("Adding view change message.")

	// TODO do we fast forward here?
	//// If there is a view change quorum send a view change message
	//if len(pi.viewChange[view].s) < 2*membership.Faults() + 1 {
	//	pi.sendViewChange()
	//}

	// If this instance is leading the segment in the new view
	if isLeading(pi.segment, membership.OwnID, view) {
		pi.maybeSendNewView(view)
	}
	return nil
}

// TODO request resurection
func (pi *pbftInstance) maybeSendNewView(view int32) {
	// Check that there is an entry for this view (sanity check)
	var vci *viewChangeInfo
	var ok bool
	if vci, ok = pi.viewChange[view]; !ok {
		logger.Warn().Int32("view", view).Msg("No entry for this view.")
		return
	}
	// Check if new view was already constructed. This is a sanity check and should never be true.
	if vci.newView != nil || vci.enoughViewChanges {
		logger.Error().
			Int32("view", view).
			Bool("newViewSent", vci.newView != nil).
			Bool("enoughViewChanges", vci.enoughViewChanges).
			Msg("New view already sent.")
		return
	}
	// Check that enough messages are available
	if len(vci.s) < 2*membership.Faults()+1 {
		logger.Trace().Int32("view", view).Msg("Not enough view change messages.")
		return
	}

	// Find highest stable checkpoint available by a weak quorum
	for _, s := range pi.viewChange[view].s {
		n := s.viewchange.H

		found := 0
		for _, sp := range pi.viewChange[view].s { // Find some other node that has a checkpoint at n
			if sp.viewchange.H > n { // There should be a stable checkpoint below n
				break
			}
			for _, cp := range sp.viewchange.Cset {
				if n == cp.Sn {
					found++
					break
				}
			}
		}
		if found >= membership.Faults()+1 { // A weak quorum of nodes have a checkpoint at n so at least one can send it
			if vci.checkpoint == nil || n >= vci.checkpoint.Sn {
				for _, cm := range s.viewchange.Cset {
					if cm.Sn == n {
						vci.checkpoint = cm
					}
				}
			}
		}
	}

	if vci.checkpoint != nil {
		logger.Trace().Int32("h", vci.checkpoint.Sn).Msg("Highest stable checkpoint available by a weak quorum.")
	} else {
		logger.Trace().Int32("h", vci.checkpoint.Sn).Msg("No checkpoint available in VIEWCHANGE messages.")
	}

	// Compute the decide values to propose
	vci.reproposeBatches = make(map[int32]*pbftBatch)
	a2 := make(map[int32][]int32, 0)      // IDs of peers that contribute to satisfying condition A2, for each SN
	a2Views := make(map[int32][]int32, 0) // For each sequence number, stores the view number of the relevant preprepare
	batchesMissing := false               // Convenience variable set if a missing batch is encountered.

	// Iteration from highest stable checkpoint to the end of the segment
	for _, sn := range pi.segment.SNs() {
		logger.Trace().Int32("sn", sn).Msg("Trying to add to xset")
		if sn <= vci.checkpoint.Sn {
			continue
		}
		found := false // an entry for this sequence number was found
		notInP := 0    // no entry for this sequence number has prepared (condition B)
		for _, m := range pi.viewChange[view].s {
			// No message was prepared for this sequence number
			if p, ok := m.viewchange.Pset[sn]; !ok {
				logger.Trace().Int32("sn", sn).Msg("No message was prepared.")
				if m.viewchange.H < sn {
					notInP++
					logger.Trace().Int32("sn", sn).Int("B", notInP).Msg("There isn't any message in P")
				}
				if notInP >= 2*membership.Faults()+1 {
					found = true
					logger.Debug().Int32("sn", sn).Bool("found", found).Msg("Empty batch")
					emptyPreprepare := &pb.PbftPreprepare{
						Sn:     sn,
						View:   view,
						Leader: membership.OwnID,
						Batch: &pb.Batch{
							Requests: make([]*pb.ClientRequest, 0, 0),
						},
						Aborted: true,

						// This value will be overwritten by receivers.
						// Setting it here, as this counts as local "reception" of the preprepare.
						// The timestamp is not part of the digest.
						// Since there is no original preprepare message, we set the timestamp to
						// when we started the segment.
						Ts: pi.startTs,
					}
					vci.reproposeBatches[sn] = &pbftBatch{
						preprepareMsg: emptyPreprepare,
						batch:         &request.Batch{Requests: make([]*request.Request, 0, 0)},
						digest:        pbftDigest(emptyPreprepare),
						committed:     false,
					}
					goto next
				}
			} else {
				// there is some m in P
				a1 := 0                        // messages that satisfy condition A1
				a2[sn] = make([]int32, 0)      // IDs of peers that contribute to satisfying condition A2
				a2Views[sn] = make([]int32, 0) // View numbers preprepares that contribute to satisfying condition A2
				for _, mp := range pi.viewChange[view].s {
					if mp.viewchange.H < sn { // mp.H must be less than sn
						pEntry, ok := mp.viewchange.Pset[sn]
						// either nothing prepared
						// or (either v' < v or (v' == v and d' == d))
						// which implies that mp does not contradict m
						if !ok || pEntry.View < p.View || (pEntry.View == p.View && bytes.Equal(pEntry.Digest, p.Digest)) {
							a1++
							logger.Trace().Int32("sn", sn).Int("A1", a1).Msg("There is a message in P")
						}
					}

					qEntry, ok := mp.viewchange.Qset[sn]
					if ok && (qEntry.View >= p.View && bytes.Equal(qEntry.Digest, p.Digest)) {
						a2[sn] = append(a2[sn], mp.viewchange.SenderId)
						a2Views[sn] = append(a2Views[sn], p.View)
						logger.Trace().Int32("sn", sn).Int("A2", len(a2[sn])).Msg("There is a message in Q")
					}
					// Enough messages in A1 and A2
					if a1 >= 2*membership.Faults()+1 && len(a2[sn]) >= membership.Faults()+1 {
						found = true
						// Try to find the batch locally
						batch := pi.findBatch(sn, view)
						if batch == nil || !bytes.Equal(batch.digest, m.viewchange.Pset[sn].Digest) {
							logger.Info().
								Int32("view", view).
								Int32("sn", sn).
								Msg("Missing preprepare message.")
							// This is a placeholder batch, no fields except for the digest are even initialized
							// and only the preprepare message will be filled in later when fetched.
							// The preprepare entry being nil meaans that the batch needs to be fetched.
							batch = &pbftBatch{
								digest:    m.viewchange.Pset[sn].Digest,
								committed: false,
							}
							// for convenience, track the sequence numbers for which to ask for batches.
							batchesMissing = true
						} else {
							newPreprepare := &pb.PbftPreprepare{
								Sn:     sn,
								View:   view,
								Leader: membership.OwnID,
								Batch:  batch.preprepareMsg.Batch,
								// This value will be overwritten by receivers.
								// Setting it here, as this counts as local "reception" of the preprepare.
								// The timestamp is not part of the digest.
								// Since there is no original preprepare message, we set the timestamp to
								// when we started the segment.
								Ts: pi.startTs,
							}
							batch = &pbftBatch{
								preprepareMsg: newPreprepare,
								batch:         batch.batch,
								committed:     batch.committed,
								// If the digest is computed over all fields of the preprepare message, this will be different from the local batch's digest.
								digest: pbftDigest(newPreprepare),
							}
						}
						vci.reproposeBatches[sn] = batch
						logger.Debug().Int32("sn", sn).Bool("found", found).Msgf("Batch digest: %x", batch.digest)
						goto next
					}
				}
			}
		}
		if !found {
			logger.Debug().Int32("view", view).Msg("Not enough view change messages to populate xset")
			return
		}
	next:
	}

	// If we reach this point, we have collected enough viewchange messages to start a new view.
	// If we were not yet in a view change, we enter it here (can happen if we did not send a viewchange ourselves).
	pi.inViewChange = true
	pi.startView(view)

	// Set a flag to stop accepting more view change messages
	vci.enoughViewChanges = true

	if batchesMissing {
		pi.askForMissingPrePrepares(vci, a2, a2Views)
	} else {
		pi.sendNewView()
	}
}

func (pi *pbftInstance) askForMissingPrePrepares(vci *viewChangeInfo, sources map[int32][]int32, views map[int32][]int32) {
	logger.Info().
		Int32("view", pi.view).
		Int("segID", pi.segment.SegID()).
		Msg("Asking for missing preprepares.")

	// We need to use this flag to prevent a scenario where a new leader times out while fetching missing preprepares.
	// Without this flag, a missing preprepare that arrives late would still trigger the leader sending a NEWVIEW,
	// even though it would be too late. Moreover, in the current implementation, the NEWVIEW message would be malformed,
	// since the *current* view is consulted when constructing it.
	vci.fetchingMissingPreprepares = true

	for sn, batch := range vci.reproposeBatches {
		if batch.preprepareMsg == nil { // A batch can be nil if we do not
			pi.requestMissingPreprepare(sn, sources[sn], views[sn])
		}
	}
}

func (pi *pbftInstance) requestMissingPreprepare(sn int32, sources []int32, views []int32) {
	// TODO: Send to more than one node (the first in this case) in a smarter way.
	//       Use the connection microbenchmarks to pick the closest peers for requesting the missing data

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_MissingPreprepareReq{MissingPreprepareReq: &pb.PbftMissingPreprepareRequest{
			View: views[0],
		}},
	}

	messenger.EnqueuePriorityMsg(msg, sources[0])
}

func (pi *pbftInstance) handleMissingPreprepareRequest(req *pb.PbftMissingPreprepareRequest, msg *pb.ProtocolMessage) {

	logger.Info().
		Int32("ownView", pi.view).
		Int32("msgView", req.View).
		Int("segID", pi.segment.SegID()).
		Int32("sn", msg.Sn).
		Msg("Handling missing preprepare request.")

	var ok bool

	var batches map[int32]*pbftBatch
	if batches, ok = pi.batches[req.View]; !ok {
		logger.Warn().Int32("sn", msg.Sn).Int32("view", req.View).Msg("Requested batch not present (View).")
		return
	}

	var batch *pbftBatch
	if batch, ok = batches[msg.Sn]; !ok {
		logger.Warn().Int32("sn", msg.Sn).Int32("view", req.View).Msg("Requested batch not present (SN).")
		return
	}

	if batch.preprepareMsg == nil {
		logger.Warn().Int32("sn", msg.Sn).Int32("view", req.View).Msg("Requested batch not present (preprepare).")
		return
	}

	if batch.preprepareMsg != nil {
		response := &pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       msg.Sn,
			Msg: &pb.ProtocolMessage_MissingPreprepare{MissingPreprepare: &pb.PbftMissingPreprepare{
				Preprepare: batch.preprepareMsg,
			}},
		}

		logger.Debug().Int32("sn", msg.Sn).Int32("view", req.View).Msg("Sending missing preprepare message.")
		messenger.EnqueuePriorityMsg(response, msg.SenderId)
	}
}

func (pi *pbftInstance) handleMissingPreprepare(preprepare *pb.PbftPreprepare, msg *pb.ProtocolMessage) {

	logger.Info().
		Int32("ownView", pi.view).
		Int32("msgView", preprepare.View).
		Int("segID", pi.segment.SegID()).
		Int32("sn", msg.Sn).
		Msg("Handling missing preprepare.")

	// TODO: Store valid responses across views, so in case we become leader again, we do not have
	//       to fetch the batch again.

	// Convenience variable: current view change info.
	vci := pi.viewChange[pi.view]

	// Ignore message if we are not in a view change or if we are not looking for missing preprepares any more.
	// Note that we do NOT check the view of the received preprepare message against our current view,
	// as the preprepare might legitimately be from an older view.
	if !pi.inViewChange || !vci.fetchingMissingPreprepares || vci.newView != nil {
		logger.Warn().
			Int32("ownView", pi.view).
			Int32("msgView", preprepare.View).
			Bool("fetchingPreprepares", vci.fetchingMissingPreprepares).
			Bool("inViewChange", pi.inViewChange).
			Bool("newViewSent", vci.newView != nil).
			Msg("Missing preprepare received too late.")
		return
	}

	// Keeps track whether any batches are still missing
	batchesMissing := false

	// Find the batch that is missing a preprepare with this digest and add the preprepare if it matches.
	for sn, batch := range vci.reproposeBatches {
		if msg.Sn == sn {

			// TODO: Adapt this when (if) distinguishing the digest of a preprepare message and the digest of a batch.

			if batch.digest != nil && bytes.Compare(pbftDigest(preprepare), batch.digest) == 0 {
				batch.preprepareMsg = &pb.PbftPreprepare{
					Sn:      sn,
					View:    pi.view,
					Leader:  membership.OwnID,
					Batch:   preprepare.Batch,
					Aborted: preprepare.Aborted,
					Ts:      pi.startTs,
				}
				batch.batch = request.NewBatch(preprepare.Batch)
				if batch == nil {
					panic("Failed to create batch from obtained missing preprepare.")
				}
			} else {
				// In an extreme corner case even this can be true - when we process a message from an old view.
				logger.Warn().
					Bool("localDigestPresent", batch.digest != nil).
					Str("msgDigest", crypto.BytesToStr(pbftDigest(preprepare))).
					Msg("Missing preprepare digest mismatch.")
				return
			}
		}

		// Keep track if any batches are still missing
		if batch.preprepareMsg == nil {
			batchesMissing = true
		}
	}

	// If no more batches are missing, send the new view
	if !batchesMissing {
		vci.fetchingMissingPreprepares = false
		pi.sendNewView()
	}
}

func (pi *pbftInstance) sendNewView() {
	logger.Info().Int32("view", pi.view).
		Int("segID", pi.segment.SegID()).
		Int32("senderID", membership.OwnID).
		Msg("Sending NEWVIEW.")

	tracing.MainTrace.Event(tracing.VIEW_CHANGE, int64(pi.segment.SegID()), int64(pi.view))

	vci := pi.viewChange[pi.view]

	// Create Vset
	vset := make(map[int32]*pb.SignedMsg)
	for i, vc := range vci.s {
		data, err := proto.Marshal(vc.viewchange)
		if err != nil {
			logger.Error().Int32("view", pi.view).Msg("Could not marshall view change message")
			return
		}
		vset[i] = &pb.SignedMsg{Data: data, Signature: vc.signature}
	}

	// Create Xset
	xset := make(map[int32]*pb.PbftPreprepare)
	for sn, batch := range vci.reproposeBatches {
		xset[sn] = batch.preprepareMsg
	}

	// TODO: Optimization: Make the Xset only contain hashes of preprepare messages and add another field that would
	//       contain the actual data. This other field could be "personalized" for each recipient, only containing
	//       those full preprepare messages that the recipient (in its VIEWCHANGE message) did not claim to
	//       have already preprepared. This might have potentially huge impact, as view changes towards the end of
	//       a segment effectively make the new leader retransmit the whole segment.

	// Create newview message.
	// The Xset will be constructed from vci.locaBatches immediately before sending the new view.
	vci.newView = &pb.PbftNewView{
		View:       pi.view,
		Vset:       vset,
		Xset:       xset,
		Checkpoint: vci.checkpoint,
	}

	data, err := proto.Marshal(vci.newView)
	if err != nil {
		logger.Error().Err(err)
		return
	}
	signature, err := pi.orderer.Sign(data)
	if err != nil {
		logger.Error().Err(err)
		return
	}
	signedMsg := &pb.SignedMsg{
		Data:      data,
		Signature: signature,
	}
	// The seq no should be a sequence number of the segment so that the
	// Orderer can dispatch the message to the correct instance.
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       pi.segment.LastSN(),
		Msg: &pb.ProtocolMessage_Newview{
			Newview: signedMsg,
		},
	}

	// TODO wrap following code for initializing a new view into a method: it repeats in the code

	// Stop new view timer if running.
	if vci.newViewTimer != nil {
		if !vci.newViewTimer.Stop() {
			logger.Warn().Int32("view", pi.view).Msg("View change finished late. Timer already fired.")
			return
		}
	}

	// Start new view change timeout
	// for the fist uncommitted sequence number in the segment after the checkpoint.
	for _, sn := range pi.segment.SNs() {
		if (vci.checkpoint == nil || sn > vci.checkpoint.Sn) && !pi.batches[pi.view][sn].committed {
			pi.setViewChangeTimer(sn, 0)
			break
		}
	}

	// Add preprepare messages from xset to own log
	for sn, repropose := range vci.reproposeBatches {
		batch := pi.batches[pi.view][sn]
		batch.preprepareMsg = repropose.preprepareMsg
		batch.preprepared = true
		batch.batch = repropose.batch
		batch.committed = repropose.committed
		batch.digest = repropose.digest

		// Committed batches are already marked as in flight. (This check is redundant, as MarkInFlight is idempotent.)
		if !batch.committed {
			batch.batch.MarkInFlight()
		}
	}

	// Enable protocol message handling for the new view
	pi.inViewChange = false

	// Process messages from backlog for the new view
	pi.backlog.process(pi.view)

	// Enqueue the message and to all except myself.
	for _, nodeID := range pi.segment.Followers() {
		if nodeID != membership.OwnID {
			messenger.EnqueuePriorityMsg(msg, nodeID)
		}
	}
}

func (pi *pbftInstance) handleNewView(signed *pb.SignedMsg, senderID int32) error {
	newview := &pb.PbftNewView{}
	// Validate signature
	err := pi.orderer.CheckSig(signed.Data, senderID, signed.Signature)
	if err != nil {
		return fmt.Errorf("invalid message signature from %d", senderID)
	}
	// Extract newview message from data
	err = proto.Unmarshal(signed.Data, newview)
	if err != nil {
		return fmt.Errorf("invalid message format from %d", senderID)
	}

	logger.Info().
		Int32("currentView", pi.view).
		Int32("msgView", newview.View).
		Int32("senderID", senderID).
		Int("segID", pi.segment.SegID()).
		Msg("Handling NEWVIEW.")

	tracing.MainTrace.Event(tracing.VIEW_CHANGE, int64(pi.segment.SegID()), int64(newview.View))

	// Check if newview message already accepted
	view := newview.View
	if v, ok := pi.viewChange[view]; ok && v.newView != nil {
		logger.Debug().Int32("view", pi.view).Msg("Newview message already accepted")
		return nil
	}
	// Ensure that the message is not form a previous view change
	if view < pi.view {
		return fmt.Errorf("old new view from %d for view %d, we are already in view %d", senderID, view, pi.view)
	}
	// Extract and validate viewchange messages
	sset := make(map[int32]*viewChangeMsg)
	for sender, message := range newview.Vset {
		viewchange := &pb.PbftViewChange{}
		// Validate signature
		err := pi.orderer.CheckSig(message.Data, sender, message.Signature)
		if err != nil {
			pi.sendViewChange()
			return fmt.Errorf("invalid message signature from %d in new view %d from %d", sender, view, senderID)
		}
		// Extract viewchange message from data
		err = proto.Unmarshal(message.Data, viewchange)
		if err != nil {
			pi.sendViewChange()
			return fmt.Errorf("invalid message format from %d in new view %d from %d", sender, view, senderID)
		}
		// Ensure that the message is not form a previous view change
		view := viewchange.View
		if view < newview.View {
			return fmt.Errorf("old view change from %d for view %d in new view %d from %d", sender, viewchange.View, view, senderID)
		}
		// Create sset entry
		sset[sender] = &viewChangeMsg{viewchange: viewchange}
	}

	// Find highest stable checkpoint available by a weak quorum
	h := int32(-1)
	for _, s := range sset {
		n := s.viewchange.H

		found := 0
		for _, sp := range sset { // Find some other node that has a checkpoint at n
			if sp.viewchange.H > n { // There should be a stable checkpoint below n
				break
			}
			for _, cp := range sp.viewchange.Cset {
				if n == cp.Sn {
					found++
					break
				}
			}
		}
		if found >= membership.Faults()+1 { // A weak quorum of nodes have a checkpoint at n so at least one can send it
			if n >= h {
				h = n
			}
		}
	}
	logger.Debug().Int32("h", h).Msg("Highest stable checkpoint available by a weak quorum.")
	if h != newview.Checkpoint.Sn {
		pi.sendViewChange()
		return fmt.Errorf("invalid checkpoint sn")
	}

	// Try to populate the Xset (i.e. the decide values to propose)
	// Iteration from highest stable checkpoint to the end of the segment
	xset := make(map[int32]*pb.PbftPreprepare)
	for _, sn := range pi.segment.SNs() {
		logger.Trace().Int32("sn", sn).Msg("Trying to add to xset")
		if sn <= newview.Checkpoint.Sn {
			continue
		}
		found := false // an entry for this sequence number was found
		notInP := 0    // no entry for this sequence number has prepared (condition B)
		for _, m := range sset {
			// No message was prepared for this sequence number
			if p, ok := m.viewchange.Pset[sn]; !ok {
				if m.viewchange.H < sn {
					notInP++
					logger.Trace().Int32("sn", sn).Int("B", notInP).Msg("There isn't any message in P")
				}
				if notInP >= 2*membership.Faults()+1 {
					// Check if there calculated xset value matches the one in the newview message
					if preprepare := newview.Xset[sn]; preprepare != nil && preprepare.Batch != nil {
						if len(preprepare.Batch.Requests) > 0 {
							pi.sendViewChange()
							return fmt.Errorf("invalid xset: preprepare for sn %d should have empty batch", sn)
						}
						if !preprepare.Aborted {
							return fmt.Errorf("invalid xset: preprepare for sn %d should have empty batch", sn)
						}
						found = true
						xset[sn] = preprepare
						logger.Trace().Int32("sn", sn).Bool("found", found).Msg("Empty batch")
						goto next1
					} else {
						pi.sendViewChange()
						return fmt.Errorf("invalid xset: missing preprepare for sn %d", sn)
					}
				}
			} else {
				// there is some m in P
				a1 := 0 // messages that satisfy condition A1
				a2 := 0 // meesages that satisfy condition A2
				for _, mp := range sset {
					if mp.viewchange.H < sn { // mp.H must be less than sn
						pEntry, ok := mp.viewchange.Pset[sn]
						// either nothing prepared
						// or (either v' < v or (v' == v and d' == d))
						// which implies that mp does not contradict m
						if !ok || pEntry.View < p.View || (pEntry.View == p.View && bytes.Equal(pEntry.Digest, p.Digest)) {
							a1++
							logger.Trace().Int32("sn", sn).Int("A1", a1).Msg("There is a message in P")
						}
					}

					qEntry, ok := mp.viewchange.Qset[sn]
					if ok && (qEntry.View >= p.View && bytes.Equal(qEntry.Digest, p.Digest)) {
						a2++
						logger.Trace().Int32("sn", sn).Int("A2", a2).Msg("There is a message in Q")
					}

					if a1 >= 2*membership.Faults()+1 && a2 >= membership.Faults()+1 {
						// Check if there calculated xset value matches the one in the newview message
						if preprepare := newview.Xset[sn]; preprepare != nil && preprepare.Batch != nil {
							digest := request.BatchDigest(preprepare.Batch)
							pDigest := m.viewchange.Pset[sn].Digest
							if !bytes.Equal(pDigest, digest) {
								pi.sendViewChange()
								return fmt.Errorf("invalid xset: preprepare doesn't much for sn %d", sn)
							}
							found = true
							xset[sn] = preprepare
							logger.Trace().Int32("sn", sn).Bool("found", found).Msgf("Batch digest: %x", digest)
							goto next1
						} else {
							pi.sendViewChange()
							return fmt.Errorf("invalid xset: missing preprepare for sn %d", sn)
						}
					}
				}
			}
		}
		if !found {
			pi.sendViewChange()
			return fmt.Errorf("invalid xset: not enough view change messages to populate xset")
		}
	next1:
	}

	// Validate preprepares in xset
	for sn, preprepare := range xset {
		if preprepare.Sn != sn || preprepare.View != view || preprepare.Leader != senderID {
			pi.sendViewChange()
			return fmt.Errorf("invalid xset: invalid preprepare for sn %d in newview %d form %d", sn, view, senderID)
		}
	}

	// TODO: Make sure all batches get properly resurrected even with cascading view changes.
	//       (maybe already happening, just make sure)

	// Stop new view timer if running.
	if vci, ok := pi.viewChange[view]; ok && vci.newViewTimer != nil {
		if !vci.newViewTimer.Stop() {
			return fmt.Errorf("view change finished too late. Timer already fired")
		}
	}

	// Create an entry for view change if not already existing
	if _, ok := pi.viewChange[view]; !ok {
		pi.newViewChangeInfo(view)
	}

	// Accept new view message
	pi.viewChange[view].newView = newview

	// TODO fetch missing checkpoints if any

	// TODO wrap following code for initializing a new view into a method: it repeats in the code

	// Initialize protocol state for the new view if not yet present (in case the view actually got updated only now).
	pi.startView(view)

	// Start new view change timeout
	// for the fist uncommitted sequence number in the segment
	for _, sn := range pi.segment.SNs() {
		if sn > newview.Checkpoint.Sn && !pi.batches[pi.view][sn].committed {
			pi.setViewChangeTimer(sn, 0)
			break
		}
	}

	// Prepare all messages in xset
	for sn, preprepare := range xset {
		batch := pi.batches[view][sn]

		batch.preprepareMsg = preprepare
		batch.preprepared = true
		// This value will be overwritten by receivers.
		// Setting it here, as this counts as local "reception" of the preprepare.
		// The timestamp is not part of the digest.
		// Since there might not be an original preprepare message, we set the timestamp to
		// when we started the segment.
		batch.preprepareMsg.Ts = pi.startTs

		if !batch.committed {
			batch.digest = pbftDigest(preprepare)
			batch.batch = request.NewBatch(preprepare.Batch)
			batch.batch.MarkInFlight()
		}

		if !isLeading(pi.segment, membership.OwnID, pi.view) {
			pi.sendPrepare(batch)
		}
	}

	// Enable protocol message handling for the new view
	pi.inViewChange = false

	// Process messages from backlog for the new view
	pi.backlog.process(view)

	return nil
}

func (pi *pbftInstance) processSerializedMessages() {
	logger.Info().Int("segID", pi.segment.SegID()).Msg("Starting serialized message processing.")

	for msg := range pi.serializer.channel {
		// To make sure noone writes anymore on closing the segment we write a special value (nil)
		if msg == nil {
			return
		}
		pi.handleMessage(msg)
	}

	// TODO handle first piority events
	//var ok = true 		// set to false if any the message channels is stopChannel
	//var msg *ordererMsg

	//for ok{
	//	select {
	//	// Try priority message if any
	//	case msg, ok = <-pi.priority:
	//		pi.handlePriorityMessage(msg.msg, msg.senderID)
	//		// If no priority message, try any message
	//	default:
	//		select {
	//		case msg, ok = <-pi.priority:
	//			pi.handlePriorityMessage(msg.msg, msg.senderID)
	//		case msg, ok = <-pi.serializer:
	//			pi.handleCommonCaseMessage(msg.msg, msg.senderID)
	//		}
	//	}
	//}
}

func (pi *pbftInstance) handleMessage(msg *pb.ProtocolMessage) {
	// Check the tye of the message.
	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_Preprepare:
		err := pi.handlePreprepare(m.Preprepare, msg)
		if err != nil {
			logger.Debug().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer ignores preprepare message.")
		}
	case *pb.ProtocolMessage_Prepare:
		err := pi.handlePrepare(m.Prepare, msg)
		if err != nil {
			logger.Debug().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer ignores prepare message.")
		}
	case *pb.ProtocolMessage_Commit:
		err := pi.handleCommit(m.Commit, msg)
		if err != nil {
			logger.Debug().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer cannot handle commit message.")
		}
	case *pb.ProtocolMessage_PbftCheckpoint:
		err := pi.handlePBFTCheckpoint(m.PbftCheckpoint, msg.SenderId)
		if err != nil {
			logger.Warn().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer cannot handle PBFT checkpoint message.")
		}
	case *pb.ProtocolMessage_PbftCatchup:
		pi.catchUp()
	case *pb.ProtocolMessage_Newseqno:
		preprepare := m.Newseqno
		pi.proposeSN(preprepare, msg.Sn)
	case *pb.ProtocolMessage_Viewchange:
		signed := m.Viewchange
		err := pi.handleViewChange(signed, msg.SenderId)
		if err != nil {
			logger.Warn().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer cannot handle view change message.")
		}
	case *pb.ProtocolMessage_MissingPreprepareReq:
		pi.handleMissingPreprepareRequest(m.MissingPreprepareReq, msg)
	case *pb.ProtocolMessage_MissingPreprepare:
		pi.handleMissingPreprepare(m.MissingPreprepare.Preprepare, msg)
	case *pb.ProtocolMessage_Newview:
		err := pi.handleNewView(m.Newview, msg.SenderId)
		if err != nil {
			logger.Debug().
				Err(err).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("PbftOrderer cannot handle new view message.")
		}
	case *pb.ProtocolMessage_Timeout:
		// If the timeout sequence number is -1 (this is the case for new view timeouts), there is no pbftBatch.
		// Otherwise, there is always a pbftBatch for every sequence number of the current view.
		if m.Timeout.View < pi.view || (m.Timeout.Sn != -1 && pi.batches[pi.view][m.Timeout.Sn].committed) {
			// If the views in this debug message are the same, that means the request has been committed in the meantime.
			logger.Debug().
				Int32("sn", m.Timeout.Sn).
				Int32("timeoutView", m.Timeout.View).
				Int32("currentView", pi.view).
				Msg("Ignoring outdated timeout.")
		} else {
			logger.Warn().Int32("sn", msg.Sn).
				Int("segId", pi.segment.SegID()).
				Int32("view", m.Timeout.View).
				Msg("Timeout")
			pi.sendViewChange()
		}
	case *pb.ProtocolMessage_MissingEntry:
		pi.handleMissingEntry(m.MissingEntry)
	default:
		logger.Error().
			Str("msg", fmt.Sprint(m)).
			Int32("sn", msg.Sn).
			Int32("senderID", msg.SenderId).
			Msg("PbftOrderer cannot handle message. Unknown message type.")
	}
}

func pbftDigest(preprepare *pb.PbftPreprepare) []byte {
	// TODO: Add the "aborted" and potentially other flags to the digest.

	return request.BatchDigest(preprepare.Batch)
}

// Returns true if this node is the leader of a segment in the current view.
func isLeading(seg manager.Segment, leaderID int32, view int32) bool {
	return seg.Leaders()[view%int32(len(seg.Leaders()))] == leaderID
}

func isPrepared(batch *pbftBatch) bool {
	// Check if the proposal is received
	if !batch.preprepared {
		return false
	}
	// Check if enough unique prepare messages are received
	if len(batch.prepareMsgs) < 2*membership.Faults() {
		return false
	}
	//Check that enough prepare messages match the proposal digest message
	// TODO optimize this by keeping in a separate data structure the already validated messages
	matching := 0
	for senderID, prepare := range batch.prepareMsgs {
		if bytes.Compare(prepare.Digest, batch.digest) != 0 {
			logger.Warn().
				Int32("sn", prepare.Sn).
				Int32("senderID", senderID).
				Msgf("Prepare message does not match preprepare. Had %x and got %x", batch.digest, prepare.Digest)
			continue
		}
		matching++
		if matching >= 2*membership.Faults() {
			break
		}
	}
	if matching < 2*membership.Faults() {
		return false
	}
	return true
}

func (batch *pbftBatch) CheckCommits() bool {
	// Check if the proposal is received
	if !batch.preprepared {
		return false
	}
	// Check if proposal is prepared
	if !batch.prepared {
		return false
	}

	// Process commit messages that have not yet been validated
	// We set already processed entries to nil to avoid processing them again.
	// It is important not to delete the map entry though, to guarantee that we only process
	// a single commit message from each peer.
	for peerID, commit := range batch.commitMsgs {
		if commit != nil && bytes.Compare(commit.Digest, batch.digest) == 0 {

			//logger.Trace().Int32("sn", commit.Sn).Int32("peerId", peerID).Msg("Received valid COMMIT message.")

			batch.validCommitMsgs = append(batch.validCommitMsgs, commit)
			batch.commitMsgs[peerID] = nil

			// Keep track of the timestamp of the last considered message.
			// Note that messages arriving after the batch has been committed are not considered on purpose.
			// This is required for estimating the throughput of a peer.
			if batch.lastCommitTs <= commit.Ts {
				batch.lastCommitTs = commit.Ts
			}
		}
	}

	// Check if enough valid commit messages are received
	if len(batch.validCommitMsgs) >= 2*membership.Faults()+1 {
		return true
	} else {
		return false
	}
}

func (pi *pbftInstance) setViewChangeTimer(sn int32, after time.Duration) {

	// Convenience variable
	batch := pi.batches[pi.view][sn]

	// Don't start the timer if it is already running
	if batch.viewChangeTimer != nil {
		logger.Debug().Int32("sn", sn).Int32("view", pi.view).Msg("Timer already started.")
		return
	} else {
		logger.Debug().Int32("sn", sn).Int32("view", pi.view).Msg("Starting timer.")
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_Timeout{
			Timeout: &pb.Timeout{
				Sn:   sn,
				View: pi.view,
			}},
	}
	batch.viewChangeTimer = time.AfterFunc(pi.viewChangeTimeout + after, func() { pi.serializer.serialize(msg) })
}

// Looks for the most recent batch with a preprepare message with sequence number sn in previous views.
func (pi *pbftInstance) findBatch(sn int32, view int32) *pbftBatch {
	for v := view - 1; v >= 0; v-- {
		if _, ok := pi.batches[v]; !ok {
			logger.Trace().Int32("view", view).Msg("No local data for this view.")
			continue
		}
		if preprepare := pi.batches[v][sn].preprepareMsg; preprepare != nil {
			return pi.batches[v][sn]
		}
	}
	return nil
}

func (pi *pbftInstance) subscribeToBacklog() {
	// Check for backloged messages for this segment
	pi.orderer.backlog.subscribers <- backlogSubscriber{segment: pi.segment, serializer: pi.serializer}
}

// Initialize protocol state for the new view if not yet present.
func (pi *pbftInstance) startView(view int32) {
	if pi.view > view {
		panic("Starting a view older than the current view")
	}

	// Don't start the same view more than once.
	// (which might lead to re-initializing and overwriting the data-structures).
	if pi.view != 0 && pi.view == view {
		return
	}

	lastView := pi.view
	pi.view = view

	// Set the viewchange timeout for this view.
	// (1<<pi.view) = 2 to the power of pi.view (2^pi.view).
	// I.e, in view one, the timeout will be double ViewchangeTimeout*(2^1),
	// in view two, it will be ViewChangeTimeout*(2^2), etc.
	pi.viewChangeTimeout = config.Config.ViewChangeTimeout * (1 << uint(pi.view))

	logger.Info().
		Int32("view", pi.view).
		Int64("timeout", pi.viewChangeTimeout.Nanoseconds()/1000000).
		Int("segID", pi.segment.SegID()).
		Msg("Starting new view.")

	// In ISS, we only propose fresh batches in view 0
	if view > 0 {
		pi.stopProposing()
	}

	if _, ok := pi.batches[view]; !ok {
		pi.batches[view] = make(map[int32]*pbftBatch)
		for i, sn := range pi.segment.SNs() {
			pi.batches[view][sn] = &pbftBatch{
				prepareMsgs: make(map[int32]*pb.PbftPrepare),
				commitMsgs:  make(map[int32]*pb.PbftCommit),
				preprepared: false,
				prepared:    false,
				committed:   false,
			}

			// If we have a median commitTime from previous epochs
			// Set an adaptive timeout for each batch
			if pi.orderer.commitTime != 0 {
				pi.setViewChangeTimer(sn, time.Duration(i) * config.Config.BatchTimeout + pi.orderer.commitTime)
				logger.Info().Int64("initial",int64(config.Config.BatchTimeout)).Int64("advanced",int64(time.Duration(i) * config.Config.BatchTimeout + pi.orderer.commitTime)).Msg("Advanced timeout")
			}

			// Except for at initialization, carry over state from the previous view.
			if view != 0 {
				previousBatch := pi.batches[lastView][sn]
				newBatch := pi.batches[view][sn]
				if previousBatch.committed {
					// Carry over committed batches from the previous view.
					logger.Debug().
						Int32("fromView", lastView).
						Int32("toView", view).
						Int32("sn", sn).
						Int("segId", pi.segment.SegID()).
						Msg("Carrying over committed batch.")
					newBatch.committed = true
					newBatch.digest = previousBatch.digest
					newBatch.batch = previousBatch.batch
				} else if previousBatch.preprepared {
					// If a batch has been preprepared but not committed, resurrect it.
					logger.Debug().
						Int32("fromView", lastView).
						Int32("toView", view).
						Int32("sn", sn).
						Int("segId", pi.segment.SegID()).
						Msg("Resurrecting uncommitted batch.")
					previousBatch.batch.Resurrect()
				} else {
					// If a batch has not even be preprepared, do nothing.
					logger.Debug().
						Int32("fromView", lastView).
						Int32("toView", view).
						Int32("sn", sn).
						Int("segId", pi.segment.SegID()).
						Msg("Batch not preprepared. Not resurrecting.")
				}
			}
		}
	} else {
		panic("Entering same view twice!")
	}
}

func (pi *pbftInstance) stopProposing() {
	pi.stopProp.Do(func() {
		close(pi.cutBatch)
	})
}
