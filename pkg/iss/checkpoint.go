/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: Eventually make the checkpoint tracker a separate package.
//       Then, use an EventService for producing Events.

// TODO: Finish writing proper comments in this file.

package iss

import (
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// checkpointTracker represents the state associated with a single instance of the checkpoint protocol
// (establishing a single stable checkpoint).
type checkpointTracker struct {

	// Epoch to which this checkpoint belongs.
	// It is always the epoch the checkpoint's associated sequence number (seqNr) is part of.
	epoch t.EpochNr

	// Sequence number associated with this checkpoint protocol instance.
	// This checkpoint encompasses seqNr sequence numbers,
	// i.e., seqNr is the first sequence number *not* encompassed by this checkpoint.
	// One can imagine that the checkpoint represents the state of the system just before seqNr,
	// i.e., "between" seqNr-1 and seqNr.
	seqNr t.SeqNr

	// The IDs of nodes to execute this instance of the checkpoint protocol.
	membership []t.NodeID

	// Application snapshot data associated with this checkpoint.
	appSnapshot []byte

	// Set of nodes from which any Checkpoint message has been received.
	// This is necessary for ignoring all but the first message a node sends, regardless of the snapshot hash.
	confirmations map[t.NodeID]struct{}
}

// newCheckpointTracker allocates and returns a new instance of a checkpointTracker associated with sequence number sn.
func newCheckpointTracker(sn t.SeqNr) *checkpointTracker {
	return &checkpointTracker{
		seqNr:         sn,
		confirmations: make(map[t.NodeID]struct{}),
		// the epoch and membership fields will be set later by iss.startCheckpoint
		// the appSnapshot field will be set by ProcessAppSnapshot
	}
}

// getCheckpointTracker looks up a checkpoint tracker associated with the given sequence number sn.
// If no such checkpoint exists, getCheckpointTracker creates a new one (and adds it to the ISS protocol state).
// Returns a pointer to the checkpoint tracker associated with sn.
func (iss *ISS) getCheckpointTracker(sn t.SeqNr) *checkpointTracker {

	// If no checkpoint tracker with sequence number sn exists, create a new one.
	if _, ok := iss.checkpoints[sn]; !ok {
		iss.checkpoints[sn] = newCheckpointTracker(sn)
	}

	// Look up and return checkpoint tracker.
	return iss.checkpoints[sn]
}

// Start initiates the checkpoint protocol among nodes in membership.
// The checkpoint to be produced encompasses all currently delivered sequence numbers.
// If Start is called during epoch transition,
// it must be called with the new epoch number, but the old epoch's membership.
func (ct *checkpointTracker) Start(epoch t.EpochNr, membership []t.NodeID) *events.EventList {

	// Set the checkpoint's epoch.
	ct.epoch = epoch

	// Save the membership this instance of the checkpoint protocol will use.
	// This is required in case where the membership changes before the checkpoint sub-protocol finishes.
	// That is also why the content of the Membership slice needs to be copied.
	ct.membership = make([]t.NodeID, len(membership), len(membership))
	copy(ct.membership, membership)

	// Request a snapshot of the application state.
	// TODO: also get a snapshot of the shared state
	return (&events.EventList{}).PushBack(events.AppSnapshotRequest(ct.seqNr))
}

func (ct *checkpointTracker) ProcessAppSnapshot(snapshot []byte) *events.EventList {

	// Save received snapshot
	// TODO: Compute and save the hash of the snapshot as well.
	ct.appSnapshot = snapshot

	// Write Checkpoint to WAL
	walEvent := events.WALAppend(PersistCheckpointEvent(ct.seqNr, ct.appSnapshot), t.WALRetIndex(ct.epoch))

	// Send a checkpoint message to all nodes after persisting checkpoint to the WAL.
	// TODO: Add hash of the snapshot
	// TODO: Add signature.
	// TODO: Implement checkpoint message retransmission.
	walEvent.FollowUp(events.SendMessage(CheckpointMessage(ct.epoch, ct.seqNr), ct.membership))

	// If the app snapshot was the last thing missing for the checkpoint to become stable,
	// also produce the necessary events.
	if ct.stable() {
		walEvent.FollowUps(ct.announceStable().Slice())
	}

	// Return resulting WALEvent (with the SendMessage event appended).
	return (&events.EventList{}).PushBack(walEvent)
}

func (ct *checkpointTracker) applyMessage(chkpMsg *isspb.Checkpoint, source t.NodeID) *events.EventList {

	// If checkpoint is already stable, ignore message.
	if ct.stable() {
		return &events.EventList{}
	}

	// TODO: Check signature of the sender.

	// TODO: Distinguish messages by snapshot hash,
	//       separately keeping the set of nodes from which a Checkpoint message has been received.

	// Ignore duplicate messages (regardless of snapshot hash).
	if _, ok := ct.confirmations[source]; ok {
		return &events.EventList{}
	}

	// TODO: Only accept messages from nodes in membership.
	//       This might be more tricky than it seems, especially when the membership is not yet initialized.

	// Note the reception of a Checkpoint message from node `source`.
	// TODO: take the snapshot hash into account. Separate data structures will be needed for that.
	ct.confirmations[source] = struct{}{}

	// If, after having applied this message, the checkpoint became stable, produce the necessary events.
	if ct.stable() {
		return ct.announceStable()
	} else {
		return &events.EventList{}
	}
}

func (ct *checkpointTracker) stable() bool {
	return ct.appSnapshot != nil && len(ct.confirmations) >= strongQuorum(len(ct.membership))
}

func (ct *checkpointTracker) announceStable() *events.EventList {
	// Create a stable checkpoint object.
	stableCheckpoint := &isspb.StableCheckpoint{
		Epoch: ct.epoch.Pb(),
		Sn:    ct.seqNr.Pb(),
	}

	// First persist the checkpoint in the WAL, then announce it to the protocol.
	persistEvent := events.WALAppend(PersistStableCheckpointEvent(stableCheckpoint), t.WALRetIndex(ct.epoch))
	persistEvent.FollowUp(StableCheckpointEvent(stableCheckpoint))
	return (&events.EventList{}).PushBack(persistEvent)
}
