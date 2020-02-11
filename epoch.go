/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"
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
func intersectionQuorum(nc *pb.NetworkConfig) int {
	return (len(nc.Nodes) + int(nc.F) + 2) / 2
}

// someCorrectQuorum is the number of nodes such that at least one of them is correct
func someCorrectQuorum(nc *pb.NetworkConfig) int {
	return int(nc.F) + 1
}

func (ec *epochConfig) intersectionQuorum() int {
	return intersectionQuorum(ec.networkConfig)
}

/*
func (ec *epochConfig) someCorrectQuorum() int {
	return someCorrectQuorum(ec.networkConfig)
}
*/

func (ec *epochConfig) seqToBucket(seqNo uint64) BucketID {
	return BucketID((seqNo - uint64(ec.initialSequence)) % uint64(len(ec.buckets)))
}

func (ec *epochConfig) seqToColumn(seqNo uint64) uint64 {
	return (seqNo-uint64(ec.initialSequence))/uint64(len(ec.buckets)) + 1
}

/*
func (ec *epochConfig) seqToBucketColumn(seqNo uint64) (BucketID, uint64) {
	return ec.seqToBucket(seqNo), ec.seqToColumn(seqNo)
}

func (ec *epochConfig) colBucketToSeq(column uint64, bucket BucketID) uint64 {
	return ec.initialSequence + (column-1)*uint64(len(ec.buckets)) + uint64(bucket)
}
*/

func (ec *epochConfig) logWidth() int {
	return 2 * int(ec.networkConfig.CheckpointInterval)
}

type epoch struct {
	// config contains the static components of the epoch
	config   *epochConfig
	myConfig *Config

	ticks uint64

	proposer *proposer

	sequences []*sequence

	lowestUncommitted int

	lastCommittedAtTick uint64
	ticksSinceProgress  int

	checkpoints       []*checkpoint
	checkpointTracker *checkpointTracker

	baseCheckpoint *pb.Checkpoint
}

// newEpoch creates a new epoch.  It uses the supplied initial checkpoints until
// new checkpoint windows are created using the given epochConfig.  The initialCheckpoint
// windows may be empty, of length 1, or length 2.
func newEpoch(newEpochConfig *pb.EpochConfig, checkpointTracker *checkpointTracker, networkConfig *pb.NetworkConfig, myConfig *Config) *epoch {

	config := &epochConfig{
		number:            newEpochConfig.Number,
		initialSequence:   newEpochConfig.StartingCheckpoint.SeqNo + 1,
		plannedExpiration: 100000000000, // XXX derive this from message
		networkConfig:     networkConfig,
		buckets:           map[BucketID]NodeID{},
	}

	i := BucketID(0)
	for _, nodeID := range newEpochConfig.Leaders {
		config.buckets[i] = NodeID(nodeID)
		i++
	}

	proposer := newProposer(config, myConfig)
	proposer.maxAssignable = newEpochConfig.StartingCheckpoint.SeqNo

	checkpoints := make([]*checkpoint, 0, 3)

	firstEnd := newEpochConfig.StartingCheckpoint.SeqNo + uint64(config.networkConfig.CheckpointInterval)
	if config.plannedExpiration >= firstEnd {
		proposer.maxAssignable = firstEnd
		checkpoints = append(checkpoints, checkpointTracker.checkpoint(firstEnd))
	}

	secondEnd := firstEnd + uint64(config.networkConfig.CheckpointInterval)
	if config.plannedExpiration >= secondEnd {
		checkpoints = append(checkpoints, checkpointTracker.checkpoint(secondEnd))
	}

	sequences := make([]*sequence, config.logWidth())
	for i := range sequences {
		sequences[i] = newSequence(config, myConfig, &Entry{
			Epoch: config.number,
			SeqNo: newEpochConfig.StartingCheckpoint.SeqNo + 1 + uint64(i),
		})
	}

	for i, digest := range newEpochConfig.FinalPreprepares {
		sequences[i].state = Validated
		sequences[i].digest = digest
		// XXX we need to get the old entry if it exists
		panic("we need persistence and or state transfer to handle this path")
	}

	return &epoch{
		baseCheckpoint:    newEpochConfig.StartingCheckpoint,
		myConfig:          myConfig,
		config:            config,
		checkpointTracker: checkpointTracker,
		checkpoints:       checkpoints,
		proposer:          proposer,
		sequences:         sequences,
	}
}

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch [][]byte) *Actions {
	baseCheckpoint := e.baseCheckpoint
	offset := int(seqNo-baseCheckpoint.SeqNo) - 1
	return e.sequences[offset].applyPreprepareMsg(batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
	return e.sequences[offset].applyPrepareMsg(source, digest)
}

func (e *epoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
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

func (e *epoch) moveWatermarks() *Actions {
	lastCW := e.checkpoints[len(e.checkpoints)-1]

	if e.config.plannedExpiration == lastCW.end {
		// This epoch is about to end gracefully, don't allocate new windows
		// so no need to go into allocation or garbage collection logic.
		return &Actions{}
	}

	secondToLastCW := e.checkpoints[len(e.checkpoints)-2]
	ci := int(e.config.networkConfig.CheckpointInterval)

	if secondToLastCW.stable {
		e.proposer.maxAssignable = lastCW.end
		for i := 0; i < ci; i++ {
			entry := &Entry{
				SeqNo: lastCW.end + uint64(i) + 1,
				Epoch: e.config.number,
			}
			e.sequences = append(e.sequences, newSequence(e.config, e.myConfig, entry))
		}
		e.checkpoints = append(e.checkpoints, e.checkpointTracker.checkpoint(lastCW.end+uint64(ci)))
	}

	for len(e.checkpoints) > 2 && (e.checkpoints[0].obsolete || e.checkpoints[1].stable) {
		e.baseCheckpoint = &pb.Checkpoint{
			SeqNo: uint64(e.checkpoints[0].end),
			Value: e.checkpoints[0].myValue,
		}
		e.checkpointTracker.release(e.checkpoints[0])
		e.checkpoints = e.checkpoints[1:]
		e.sequences = e.sequences[ci:]
		e.lowestUncommitted -= ci
	}

	return e.proposer.drainQueue()
}

func (e *epoch) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
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
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
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
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
	actions := e.sequences[offset].applyValidateResult(valid)
	bucket := e.config.seqToBucket(seqNo)
	if e.config.buckets[bucket] != NodeID(e.myConfig.ID) {
		actions.Append(e.sequences[offset].applyPrepareMsg(e.config.buckets[bucket], e.sequences[offset].digest))
	}
	return actions
}

func (e *epoch) tick() *Actions {
	actions := &Actions{} // TODO, only heartbeat if no progress
	if e.myConfig.HeartbeatTicks != 0 && e.ticks%uint64(e.myConfig.HeartbeatTicks) == 0 {
		actions.Append(e.proposer.noopAdvance())
	}

	if e.sequences[e.lowestUncommitted].entry.SeqNo == e.lastCommittedAtTick+1 {
		e.ticksSinceProgress++
		if e.ticksSinceProgress > e.myConfig.SuspectTicks {
			return &Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Suspect{
							Suspect: &pb.Suspect{
								Epoch: e.config.number,
							},
						},
					},
				},
			}
		}
		return &Actions{}
	} else {
		e.ticksSinceProgress = 0
	}

	return actions
}

func (e *epoch) constructEpochChange(newEpoch uint64) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: e.config.number + 1,
	}

	if len(e.checkpoints) == 0 ||
		e.checkpoints[0].myValue == nil ||
		!e.checkpoints[0].stable {

		// We have no stable checkpoint windows which have not been
		// garbage collected, so use the most recently garbage collected one

		epochChange.Checkpoints = []*pb.Checkpoint{e.baseCheckpoint}
	}

	for _, cw := range e.checkpoints {
		if cw.myValue == nil {
			// Checkpoints necessarily generated in order, no further checkpoints are ready
			break
		}
		epochChange.Checkpoints = append(epochChange.Checkpoints, &pb.Checkpoint{
			SeqNo: uint64(cw.end),
			Value: cw.myValue,
		})

		for _, seq := range e.sequences {
			if seq.state < Validated {
				continue
			}

			entry := &pb.EpochChange_SetEntry{
				Epoch:  e.config.number,
				SeqNo:  seq.entry.SeqNo,
				Digest: seq.digest,
			}

			epochChange.QSet = append(epochChange.QSet, entry)

			if seq.state < Prepared {
				continue
			}

			epochChange.PSet = append(epochChange.PSet, entry)
		}
	}

	// XXX include the Qset from previous view-changes if it has not been garbage collected

	return epochChange
}

func (e *epoch) status() []*BucketStatus {
	buckets := make([]*BucketStatus, len(e.config.buckets))
	for i := range buckets {
		bucket := &BucketStatus{
			ID:        uint64(i),
			Leader:    e.config.buckets[BucketID(i)] == NodeID(e.myConfig.ID),
			Sequences: make([]SequenceState, 0, len(e.sequences)/len(buckets)),
		}

		for j := i; j < len(e.sequences); j = j + len(buckets) {
			bucket.Sequences = append(bucket.Sequences, e.sequences[j].state)
		}

		buckets[i] = bucket
	}

	return buckets
}
