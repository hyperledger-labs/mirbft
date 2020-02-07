/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/golang/protobuf/proto"
)

// epochConfig is the information required by the various
// state machines whose state is scoped to an epoch
type epochConfig struct {
	// number is the epoch number this config applies to
	number uint64

	initialSequence uint64

	// plannedExpiration is when this epoch ends, if it ends gracefully
	plannedExpiration uint64

	networkConfig *pb.NetworkConfig

	// buckets is a map from bucket ID to leader ID
	buckets map[BucketID]NodeID
}

// intersectionQuorum is the number of nodes required to agree
// such that any two sets intersected will each contain some same
// correct node.  This is ceil((n+f+1)/2), which is equivalent to
// (n+f+2)/2 under truncating integer math.
func (ec *epochConfig) intersectionQuorum() int {
	return (len(ec.networkConfig.Nodes) + int(ec.networkConfig.F) + 2) / 2
}

// someCorrectQuorum is the number of nodes such that at least one of them is correct
func (ec *epochConfig) someCorrectQuorum() int {
	return int(ec.networkConfig.F) + 1
}

func (ec *epochConfig) seqToBucket(seqNo uint64) BucketID {
	return BucketID((seqNo - uint64(ec.initialSequence)) % uint64(len(ec.buckets)))
}

func (ec *epochConfig) seqToColumn(seqNo uint64) uint64 {
	return (seqNo-uint64(ec.initialSequence))/uint64(len(ec.buckets)) + 1
}

func (ec *epochConfig) seqToBucketColumn(seqNo uint64) (BucketID, uint64) {
	return ec.seqToBucket(seqNo), ec.seqToColumn(seqNo)
}

func (ec *epochConfig) colBucketToSeq(column uint64, bucket BucketID) uint64 {
	return ec.initialSequence + (column-1)*uint64(len(ec.buckets)) + uint64(bucket)
}

func (ec *epochConfig) logWidth() int {
	return 2 * int(ec.networkConfig.CheckpointInterval)
}

type epochState int

const (
	prepending epochState = iota
	pending
	echoing
	readying
	active
	done
)

type epoch struct {
	myConfig *Config

	// config contains the static components of the epoch
	config *epochConfig

	ticks uint64

	myNewEpoch *pb.NewEpoch

	proposer *proposer

	state epochState

	// stateTicks tracks the number of ticks that have occurred
	// while in the current state.  Whenever the state transitions,
	// this should be reset to 0
	stateTicks uint64

	echos map[NodeID]*pb.EpochConfig

	readies map[NodeID]*pb.EpochConfig

	changes map[NodeID]*epochChange

	suspicions map[NodeID]struct{}

	isLeader bool

	sequences []*sequence

	lowestUncommitted int

	checkpointWindows []*checkpointWindow

	baseCheckpoint *pb.Checkpoint
}

// newEpoch creates a new epoch.  It uses the supplied initial checkpointWindows until
// new checkpoint windows are created using the given epochConfig.  The initialCheckpoint
// windows may be empty, of length 1, or length 2.
func newEpoch(baseCheckpoint *pb.Checkpoint, config *epochConfig, myConfig *Config) *epoch {

	proposer := newProposer(config, myConfig)
	proposer.maxAssignable = baseCheckpoint.SeqNo * uint64(len(config.buckets))

	var checkpointWindows []*checkpointWindow

	firstEnd := baseCheckpoint.SeqNo + uint64(config.networkConfig.CheckpointInterval)/uint64(len(config.buckets))
	if config.plannedExpiration >= firstEnd {
		proposer.maxAssignable = firstEnd * uint64(len(config.buckets))
		checkpointWindows = append(checkpointWindows, newCheckpointWindow(baseCheckpoint.SeqNo+1, firstEnd, config, myConfig))
	}

	secondEnd := baseCheckpoint.SeqNo + 2*uint64(config.networkConfig.CheckpointInterval)/uint64(len(config.buckets))
	if config.plannedExpiration >= secondEnd {
		checkpointWindows = append(checkpointWindows, newCheckpointWindow(firstEnd+1, secondEnd, config, myConfig))
	}

	sequences := make([]*sequence, config.logWidth())
	for i := range sequences {
		sequences[i] = newSequence(config, myConfig, &Entry{
			Epoch: config.number,
			SeqNo: baseCheckpoint.SeqNo + 1 + uint64(i),
		})
	}

	return &epoch{
		baseCheckpoint:    baseCheckpoint,
		myConfig:          myConfig,
		config:            config,
		echos:             map[NodeID]*pb.EpochConfig{},
		readies:           map[NodeID]*pb.EpochConfig{},
		suspicions:        map[NodeID]struct{}{},
		changes:           map[NodeID]*epochChange{},
		checkpointWindows: checkpointWindows,
		proposer:          proposer,
		isLeader:          config.number%uint64(len(config.networkConfig.Nodes)) == myConfig.ID,
		sequences:         sequences,
	}
}

// Summary of Bracha reliable broadcast from:
//   https://dcl.epfl.ch/site/_media/education/sdc_byzconsensus.pdf
//
// upon r-broadcast(m): // only Ps
// send message (SEND, m) to all
//
// upon receiving a message (SEND, m) from Ps:
// send message (ECHO, m) to all
//
// upon receiving ceil((n+t+1)/2)
// e messages(ECHO, m) and not having sent a READY message:
// send message (READY, m) to all
//
// upon receiving t+1 messages(READY, m) and not having sent a READY message:
// send message (READY, m) to all
// upon receiving 2t + 1 messages (READY, m):
//
// r-deliver(m)

func (e *epoch) applyNewEpochMsg(msg *pb.NewEpoch) *Actions {
	if e.state > pending {
		// TODO log oddity? maybe ensure not possible via nodemsgs
		return &Actions{}
	}

	epochChanges := map[NodeID]*epochChange{}
	for _, remoteEpochChange := range msg.EpochChanges {
		if _, ok := epochChanges[NodeID(remoteEpochChange.NodeId)]; ok {
			// TODO, malformed, log oddity
			return &Actions{}
		}

		helper, err := newEpochChange(remoteEpochChange.EpochChange)
		if err != nil {
			// TODO, log
			return &Actions{}
		}

		epochChanges[NodeID(remoteEpochChange.NodeId)] = helper
	}

	// XXX need to validate the signatures on the epoch changes

	newEpochConfig := constructNewEpochConfig(e.config, epochChanges)

	if !proto.Equal(newEpochConfig, msg.Config) {
		// TODO byzantine, log oddity
		return &Actions{}
	}

	e.state = echoing

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_NewEpochEcho{
					NewEpochEcho: &pb.NewEpochEcho{
						Config: msg.Config,
					},
				},
			},
		},
	}
}

func (e *epoch) applyNewEpochEchoMsg(source NodeID, msg *pb.NewEpochEcho) *Actions {
	if _, ok := e.echos[source]; ok {
		// TODO, if different, byzantine, oddities
		return &Actions{}
	}

	e.echos[source] = msg.Config

	if len(e.echos) < e.config.intersectionQuorum() {
		return &Actions{}
	}

	// XXX we need to verify that the configs actually match, but
	// since we have not computed a digest, this is potentially expensive
	// so deferring the implementation.

	if e.state > echoing {
		return &Actions{}
	}

	e.state = readying

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_NewEpochReady{
					NewEpochReady: &pb.NewEpochReady{
						Config: msg.Config,
					},
				},
			},
		},
	}
}

func (e *epoch) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	if _, ok := e.readies[source]; ok {
		// TODO, if different, byzantine, oddities
		return &Actions{}
	}

	e.readies[source] = msg.Config

	if e.state > readying {
		// We've already accepted the epoch config, move along
		return &Actions{}
	}

	// XXX we need to verify that the configs actually match, but
	// since we have not computed a digest, this is potentially expensive
	// so deferring the implementation.

	if len(e.readies) < e.config.someCorrectQuorum() {
		return &Actions{}
	}

	if e.state < readying {
		e.state = readying

		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpochReady{
						NewEpochReady: &pb.NewEpochReady{
							Config: msg.Config,
						},
					},
				},
			},
		}
	}

	if len(e.readies) >= e.config.intersectionQuorum() {
		e.state = active
	}

	return &Actions{}
}

func (e *epoch) checkpointWindowForSeqNo(seqNo uint64) *checkpointWindow {
	seqNo = e.config.seqToColumn(seqNo)
	if e.config.plannedExpiration < seqNo {
		return nil
	}

	if e.checkpointWindows[0].start > seqNo {
		return nil
	}

	offset := seqNo - e.checkpointWindows[0].start
	index := offset / (uint64(e.config.networkConfig.CheckpointInterval) / uint64(len(e.config.buckets)))

	if int(index) >= len(e.checkpointWindows) {
		return nil
	}
	return e.checkpointWindows[index]
}

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch [][]byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	offset := int(seqNo-e.baseCheckpoint.SeqNo*uint64(len(e.config.buckets))) - 1
	return e.sequences[offset].applyPreprepareMsg(batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	offset := int(seqNo-e.baseCheckpoint.SeqNo*uint64(len(e.config.buckets))) - 1
	return e.sequences[offset].applyPrepareMsg(source, digest)
}

func (e *epoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	offset := int(seqNo-e.baseCheckpoint.SeqNo*uint64(len(e.config.buckets))) - 1
	actions := e.sequences[offset].applyCommitMsg(source, digest)

	if len(actions.Commit) > 0 && offset == e.lowestUncommitted {
		actions.Append(e.advanceUncommitted())
	}

	return actions
}

func (e *epoch) advanceUncommitted() *Actions {
	actions := &Actions{}

	for e.lowestUncommitted < len(e.sequences) {
		if e.sequences[e.lowestUncommitted].state != Committed {
			break
		}

		if (e.lowestUncommitted+1)%int(e.config.networkConfig.CheckpointInterval) == 0 {
			actions.Checkpoint = append(actions.Checkpoint, e.sequences[e.lowestUncommitted].entry.SeqNo)
		}

		e.lowestUncommitted++
	}

	return actions
}

func (e *epoch) applyCheckpointMsg(source NodeID, seqNo uint64, value []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	cw := e.checkpointWindowForSeqNo(seqNo)
	actions := cw.applyCheckpointMsg(source, value)

	lastCW := e.checkpointWindows[len(e.checkpointWindows)-1]

	if lastCW.epochConfig.plannedExpiration == lastCW.end {
		// This epoch is about to end gracefully, don't allocate new windows
		// so no need to go into allocation or garbage collection logic.
		return &Actions{}
	}

	secondToLastCW := e.checkpointWindows[len(e.checkpointWindows)-2]
	ci := int(e.config.networkConfig.CheckpointInterval)

	if secondToLastCW.garbageCollectible {
		e.proposer.maxAssignable = lastCW.end * uint64(len(e.config.buckets))
		for i := 0; i < ci; i++ {
			entry := &Entry{
				SeqNo: lastCW.end*uint64(len(e.config.buckets)) + uint64(i) + 1,
				Epoch: e.config.number,
			}
			e.sequences = append(e.sequences, newSequence(e.config, e.myConfig, entry))
		}
		e.checkpointWindows = append(
			e.checkpointWindows,
			newCheckpointWindow(
				lastCW.end+1,
				lastCW.end+uint64(e.config.networkConfig.CheckpointInterval)/uint64(len(e.config.buckets)),
				e.config,
				e.myConfig,
			),
		)
	}

	actions.Append(e.proposer.drainQueue())
	for len(e.checkpointWindows) > 2 && (e.checkpointWindows[0].obsolete || e.checkpointWindows[1].garbageCollectible) {
		e.baseCheckpoint = &pb.Checkpoint{
			SeqNo: uint64(e.checkpointWindows[0].end),
			Value: e.checkpointWindows[0].myValue,
		}
		e.checkpointWindows = e.checkpointWindows[1:]
		e.sequences = e.sequences[ci:]
		e.lowestUncommitted -= ci
	}

	// XXX super-hacky, fix
	return actions
}

func (e *epoch) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
	if e.state == done {
		return &Actions{}
	}

	bucketID := BucketID(preprocessResult.Cup % uint64(len(e.config.buckets)))
	nodeID := e.config.buckets[bucketID]
	if nodeID == NodeID(e.myConfig.ID) {
		return e.proposer.propose(preprocessResult.Proposal.Data)
	}

	if preprocessResult.Proposal.Source == e.myConfig.ID {
		// I originated this proposal, but someone else leads this bucket,
		// forward the message to them
		return &Actions{
			Unicast: []Unicast{
				{
					Target: uint64(nodeID),
					Msg: &pb.Msg{
						Type: &pb.Msg_Forward{
							Forward: &pb.Forward{
								Epoch:  e.config.number,
								Bucket: uint64(bucketID),
								Data:   preprocessResult.Proposal.Data,
							},
						},
					},
				},
			},
		}
	}

	// Someone forwarded me this proposal, but I'm not responsible for it's bucket
	// TODO, log oddity? Assign it to the wrong bucket? Forward it again?
	return &Actions{}
}

func (e *epoch) applyDigestResult(seqNo uint64, digest []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	offset := int(seqNo-e.baseCheckpoint.SeqNo*uint64(len(e.config.buckets))) - 1
	actions := e.sequences[offset].applyDigestResult(digest)

	bucket := e.config.seqToBucket(seqNo)
	if e.config.buckets[bucket] == NodeID(e.myConfig.ID) {
		// If we are the leader, no need to validate
		_ = e.sequences[offset].applyValidateResult(true)
		return e.sequences[offset].applyPrepareMsg(NodeID(e.myConfig.ID), digest)
	}

	return actions
}

func (e *epoch) applyValidateResult(seqNo uint64, valid bool) *Actions {
	if e.state == done {
		return &Actions{}
	}

	offset := int(seqNo-e.baseCheckpoint.SeqNo*uint64(len(e.config.buckets))) - 1
	actions := e.sequences[offset].applyValidateResult(valid)
	bucket := e.config.seqToBucket(seqNo)
	if e.config.buckets[bucket] != NodeID(e.myConfig.ID) {
		actions.Append(e.sequences[offset].applyPrepareMsg(e.config.buckets[bucket], e.sequences[offset].digest))
	}
	return actions
}

func (e *epoch) applyCheckpointResult(seqNo uint64, value []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	cw := e.checkpointWindowForSeqNo(seqNo)
	if cw == nil {
		panic(fmt.Sprintf("received an unexpected checkpoint result for seqno=%d", seqNo))
	}

	return e.hackyMangle(0, cw.applyCheckpointResult(value))
}

func (e *epoch) hackyMangle(bucket BucketID, actions *Actions) *Actions {
	for _, msg := range actions.Broadcast {
		switch innerMsg := msg.Type.(type) {
		case *pb.Msg_Checkpoint:
			innerMsg.Checkpoint.SeqNo = e.config.colBucketToSeq(innerMsg.Checkpoint.SeqNo, BucketID(len(e.config.buckets)-1))
		}
	}

	return actions
}

func (e *epoch) applyEpochChangeMsg(source NodeID, msg *pb.EpochChange) {
	if ec, ok := e.changes[source]; ok {
		if !proto.Equal(ec.underlying, msg) {
			// TODO log oddity
		}
		return
	}

	epochChange, err := newEpochChange(msg)
	if err != nil {
		// TODO log oddity
		return
	}

	e.changes[source] = epochChange
}

func (e *epoch) applySuspectMsg(source NodeID, msg *pb.Suspect) {
	e.suspicions[source] = struct{}{}
}

func (e *epoch) tick() *Actions {
	e.stateTicks++

	actions := &Actions{}

	if e.state != done {
		// This is done first, as this tick may transition
		// the state to done.
		actions.Append(e.tickNotDone())
	}

	switch e.state {
	case prepending:
		actions.Append(e.tickPrepending())
	case pending:
		actions.Append(e.tickPending())
	case active:
		actions.Append(e.tickActive())
	default: // case done:
	}

	return actions
}

func (e *epoch) repeatEpochChangeBroadcast() *Actions {
	myEpochChange, ok := e.changes[NodeID(e.myConfig.ID)]
	if !ok {
		panic("TODO, handle me? Are we guaranteed to have sent an epoch change, I hope so")
	}

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: myEpochChange.underlying,
				},
			},
		},
	}
}

func (e *epoch) tickPrepending() *Actions {
	newEpoch := e.constructNewEpoch() // TODO, recomputing over and over again isn't useful unless we've gotten new epoch change messages in the meantime, should we somehow store the last one we computed?

	if newEpoch == nil {
		if e.stateTicks%uint64(e.myConfig.NewEpochTimeoutTicks/2) == 0 {
			return e.repeatEpochChangeBroadcast()
		}

		return &Actions{}
	}

	e.ticks = 0
	e.state = pending
	e.myNewEpoch = newEpoch

	if e.isLeader {
		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpoch{
						NewEpoch: e.myNewEpoch,
					},
				},
			},
		}
	}

	return &Actions{}
}

func (e *epoch) tickPending() *Actions {
	pendingTicks := e.stateTicks % uint64(e.myConfig.NewEpochTimeoutTicks)
	if e.isLeader {
		// resend the new-view if others perhaps missed it
		if pendingTicks%2 == 0 {
			return &Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_NewEpoch{
							NewEpoch: e.myNewEpoch,
						},
					},
				},
			}
		}
	} else {
		if pendingTicks == 0 {
			// TODO, new-view timeout
		}
		if pendingTicks%2 == 0 {
			return e.repeatEpochChangeBroadcast()
		}
	}
	return &Actions{}
}

func (e *epoch) tickNotDone() *Actions {
	if len(e.suspicions) < e.config.intersectionQuorum() {
		return &Actions{}
	}

	e.state = done

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: e.constructEpochChange(),
				},
			},
		},
	}

}

func (e *epoch) tickActive() *Actions {
	actions := &Actions{}
	if e.myConfig.HeartbeatTicks != 0 && e.ticks%uint64(e.myConfig.HeartbeatTicks) == 0 {
		actions.Append(e.proposer.noopAdvance())
	}

	for _, cw := range e.checkpointWindows {
		actions.Append(cw.tick())
	}

	return actions
}

func (e *epoch) constructEpochChange() *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: e.config.number + 1,
	}

	if len(e.checkpointWindows) == 0 ||
		e.checkpointWindows[0].myValue == nil ||
		!e.checkpointWindows[0].garbageCollectible {

		// We have no stable checkpoint windows which have not been
		// garbage collected, so use the most recently garbage collected one

		epochChange.Checkpoints = []*pb.Checkpoint{e.baseCheckpoint}
	}

	for _, cw := range e.checkpointWindows {
		if cw.myValue == nil {
			// Checkpoints necessarily generated in order, no further checkpoints are ready
			break
		}
		epochChange.Checkpoints = append(epochChange.Checkpoints, &pb.Checkpoint{
			SeqNo: uint64(cw.end),
			Value: cw.myValue,
		})

		for _, bucket := range cw.buckets {
			for seqNo, seq := range bucket.sequences {
				if seq.state < Validated {
					continue
				}

				entry := &pb.EpochChange_SetEntry{
					Epoch:  e.config.number,
					SeqNo:  seqNo,
					Digest: seq.digest,
				}

				epochChange.QSet = append(epochChange.QSet, entry)

				if seq.state < Prepared {
					continue
				}

				epochChange.PSet = append(epochChange.PSet, entry)
			}
		}
	}

	// XXX include the Qset from previous view-changes if it has not been garbage collected

	return epochChange
}

func (e *epoch) constructNewEpoch() *pb.NewEpoch {
	config := constructNewEpochConfig(e.config, e.changes)
	if config == nil {
		return nil
	}

	remoteChanges := make([]*pb.NewEpoch_RemoteEpochChange, 0, len(e.changes))
	for nodeID, change := range e.changes {
		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId:      uint64(nodeID),
			EpochChange: change.underlying,
		})
	}

	return &pb.NewEpoch{
		Config:       config,
		EpochChanges: remoteChanges,
	}
}

func (e *epoch) status() []*BucketStatus {
	buckets := make([]*BucketStatus, len(e.config.buckets))
	for i := range buckets {
		bucket := &BucketStatus{
			ID:        uint64(i),
			Leader:    e.config.buckets[BucketID(i)] == NodeID(e.myConfig.ID),
			Sequences: make([]SequenceState, len(e.sequences)),
		}

		for j, seq := range e.sequences {
			bucket.Sequences[j] = seq.state
		}

		buckets[i] = bucket
	}

	return buckets
}
