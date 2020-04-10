/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/pkg/errors"
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

	// leaders is the set of nodes which will act as leaders in this epoch
	leaders []uint64

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

	proposer      *proposer
	persisted     *persisted
	clientWindows *clientWindows

	sequences []*sequence

	ending            bool // set when this epoch about to end gracefully
	lowestUncommitted int
	lowestUnallocated []int // index by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  int

	checkpoints       []*checkpoint
	checkpointTracker *checkpointTracker

	baseCheckpoint *pb.Checkpoint
}

type initialEpochState struct {
	baseCheckpoint       *pb.Checkpoint
	initializedSequences []*initializedSequence
}

type initializedSequence struct {
	state  SequenceState // May only be one of Uninitialized, Preprepared, Prepared, Committed
	digest []byte
	batch  []*clientRequest
}

// newEpoch creates a new epoch.  It uses the supplied initial checkpoints until
// new checkpoint windows are created using the given epochConfig.  The initialCheckpoint
// windows may be empty, of length 1, or length 2.
func newEpoch(persisted *persisted, newEpochConfig *pb.EpochConfig, checkpointTracker *checkpointTracker, clientWindows *clientWindows, networkConfig *pb.NetworkConfig, myConfig *Config) *epoch {

	config := &epochConfig{
		number:            newEpochConfig.Number,
		initialSequence:   newEpochConfig.StartingCheckpoint.SeqNo + 1,
		plannedExpiration: newEpochConfig.StartingCheckpoint.SeqNo + networkConfig.MaxEpochLength,
		networkConfig:     networkConfig,
		buckets:           map[BucketID]NodeID{},
		leaders:           newEpochConfig.Leaders,
	}

	leaders := map[uint64]struct{}{}
	for _, leader := range newEpochConfig.Leaders {
		leaders[leader] = struct{}{}
	}

	overflowIndex := 0 // TODO, this should probably start after the last assigned node
	for i := 0; i < int(networkConfig.NumberOfBuckets); i++ {
		bucketID := BucketID(i)
		leader := networkConfig.Nodes[(uint64(i)+newEpochConfig.Number)%uint64(len(networkConfig.Nodes))]
		if _, ok := leaders[leader]; !ok {
			config.buckets[bucketID] = NodeID(newEpochConfig.Leaders[overflowIndex%len(newEpochConfig.Leaders)])
			overflowIndex++
		} else {
			config.buckets[bucketID] = NodeID(leader)
		}
	}

	checkpoints := make([]*checkpoint, 0, 3)

	firstEnd := newEpochConfig.StartingCheckpoint.SeqNo + uint64(config.networkConfig.CheckpointInterval)
	if config.plannedExpiration >= firstEnd {
		checkpoints = append(checkpoints, checkpointTracker.checkpoint(firstEnd))
	}

	secondEnd := firstEnd + uint64(config.networkConfig.CheckpointInterval)
	if config.plannedExpiration >= secondEnd {
		checkpoints = append(checkpoints, checkpointTracker.checkpoint(secondEnd))
	}

	lowestUnallocated := make([]int, len(config.buckets))
	for i := range lowestUnallocated {
		lowestUnallocated[i] = i + config.logWidth() // The first seq for the bucket beyond our watermarks
	}

	sequences := make([]*sequence, config.logWidth())
	for i := range sequences {
		seqNo := newEpochConfig.StartingCheckpoint.SeqNo + 1 + uint64(i)
		bucket := config.seqToBucket(seqNo)
		owner := config.buckets[bucket]

		sequences[i] = newSequence(owner, config.number, seqNo, persisted, networkConfig, myConfig)
		qEntry, ok := persisted.qSet[seqNo][newEpochConfig.Number]
		if !ok {
			if i < lowestUnallocated[bucket] {
				lowestUnallocated[bucket] = i
			}
			continue
		}

		sequences[i].qEntry = qEntry
		sequences[i].digest = qEntry.Digest
		sequences[i].state = Preprepared

		pEntry, ok := persisted.pSet[seqNo]
		if !ok || pEntry.Epoch != newEpochConfig.Number {
			continue
		}

		sequences[i].state = Prepared

		if seqNo > persisted.lastCommitted {
			continue
		}

		sequences[i].state = Committed
	}

	lowestUncommitted := len(sequences)
	for i, seq := range sequences {
		if seq.state == Committed {
			continue
		}
		lowestUncommitted = i
		break
	}

	proposer := newProposer(myConfig, clientWindows, config.buckets)
	proposer.stepAllClientWindows()

	return &epoch{
		baseCheckpoint:    newEpochConfig.StartingCheckpoint,
		myConfig:          myConfig,
		config:            config,
		checkpointTracker: checkpointTracker,
		checkpoints:       checkpoints,
		clientWindows:     clientWindows,
		persisted:         persisted,
		proposer:          proposer,
		sequences:         sequences,
		lowestUnallocated: lowestUnallocated,
		lowestUncommitted: lowestUncommitted,
	}
}

func (e *epoch) getSequence(seqNo uint64) (*sequence, int, error) {
	if seqNo < e.lowWatermark() || seqNo > e.highWatermark() {
		return nil, 0, errors.Errorf("requested seq no (%d) is out of range [%d - %d]",
			seqNo, e.lowWatermark(), e.highWatermark())
	}
	offset := int(seqNo - e.lowWatermark())
	return e.sequences[offset], offset, nil
}

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch []*clientRequest) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	bucketID := e.config.seqToBucket(seqNo)

	if source == NodeID(e.myConfig.ID) {
		// Apply our own preprepares as a prepare
		return seq.applyPrepareMsg(source, seq.digest)
	}

	defer func() {
		e.lowestUnallocated[int(bucketID)] += len(e.config.buckets)
	}()

	if offset != e.lowestUnallocated[int(bucketID)] {
		panic(fmt.Sprintf("dev test, this really shouldn't happen: offset=%d e.lowestUnallocated=%d\n", offset, e.lowestUnallocated[int(bucketID)]))
	}

	for _, request := range batch {
		if len(request.agreements) < intersectionQuorum(e.config.networkConfig) {
			return seq.allocateInvalid(batch)
		}
	}

	return seq.allocate(batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}
	return seq.applyPrepareMsg(source, digest)
}

func (e *epoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	seq.applyCommitMsg(source, digest)
	if seq.state != Committed || offset != e.lowestUncommitted {
		return &Actions{}
	}

	actions := &Actions{}

	for e.lowestUncommitted < len(e.sequences) {
		if e.sequences[e.lowestUncommitted].state != Committed {
			break
		}

		actions.Commits = append(actions.Commits, &Commit{
			QEntry:     e.sequences[e.lowestUncommitted].qEntry,
			Checkpoint: e.sequences[e.lowestUncommitted].seqNo%uint64(e.config.networkConfig.CheckpointInterval) == 0,
		})
		for _, reqForward := range e.sequences[e.lowestUncommitted].qEntry.Requests {
			cw, ok := e.clientWindows.clientWindow(reqForward.Request.ClientId)
			if !ok {
				panic("we never should have committed this without the client available")
			}
			cw.request(reqForward.Request.ReqNo).committed = &seqNo
		}

		e.persisted.setLastCommitted(e.sequences[e.lowestUncommitted].seqNo)
		e.lowestUncommitted++
	}

	return actions
}

func (e *epoch) moveWatermarks() *Actions {
	lastCW := e.checkpoints[len(e.checkpoints)-1]

	secondToLastCW := e.checkpoints[len(e.checkpoints)-2]
	ci := int(e.config.networkConfig.CheckpointInterval)

	// If this epoch is ending, don't allocate new sequences
	if lastCW.end == e.config.plannedExpiration {
		e.ending = true
	}

	if secondToLastCW.stable && !e.ending {
		for i := 0; i < ci; i++ {
			seqNo := lastCW.end + uint64(i) + 1
			epoch := e.config.number
			owner := e.config.buckets[e.config.seqToBucket(seqNo)]
			e.sequences = append(e.sequences, newSequence(owner, epoch, seqNo, e.persisted, e.config.networkConfig, e.myConfig))
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
		for i := range e.lowestUnallocated {
			e.lowestUnallocated[i] -= ci
		}
	}

	return e.drainProposer()
}

func (e *epoch) drainProposer() *Actions {
	actions := &Actions{}

	for bucketID, ownerID := range e.config.buckets {
		if ownerID != NodeID(e.myConfig.ID) {
			continue
		}

		for e.proposer.hasPending(bucketID) {
			i := e.lowestUnallocated[int(bucketID)]
			if i >= len(e.sequences) {
				break
			}
			seq := e.sequences[i]

			if len(e.sequences)-i <= int(e.config.networkConfig.CheckpointInterval) && !e.ending {
				// let the network move watermarks before filling up the last checkpoint
				// interval
				break
			}

			if ownerID == NodeID(e.myConfig.ID) && e.proposer.hasPending(bucketID) {
				proposals := e.proposer.next(bucketID)
				actions.Append(seq.allocate(proposals))
				e.lowestUnallocated[int(bucketID)] += len(e.config.buckets)
			}
		}
	}

	return actions
}

func (e *epoch) applyProcessResult(seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	return seq.applyProcessResult(digest)
}

func (e *epoch) tick() *Actions {
	if e.lowestUncommitted < len(e.sequences) && e.sequences[e.lowestUncommitted].seqNo != e.lastCommittedAtTick+1 {
		e.ticksSinceProgress = 0
		e.lastCommittedAtTick = e.sequences[e.lowestUncommitted].seqNo - 1
		return &Actions{}
	}

	e.ticksSinceProgress++
	actions := &Actions{}

	if e.ticksSinceProgress > e.myConfig.SuspectTicks {
		actions.Append(&Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_Suspect{
						Suspect: &pb.Suspect{
							Epoch: e.config.number,
						},
					},
				},
			},
		})
	}

	if e.myConfig.HeartbeatTicks == 0 || e.ticksSinceProgress%e.myConfig.HeartbeatTicks != 0 {
		return actions
	}

	for bucketID, index := range e.lowestUnallocated {
		if index >= len(e.sequences) {
			continue
		}

		if e.config.buckets[BucketID(bucketID)] != NodeID(e.myConfig.ID) {
			continue
		}

		if len(e.sequences)-index <= int(e.config.networkConfig.CheckpointInterval) && !e.ending {
			continue
		}

		if e.proposer.hasOutstanding(BucketID(bucketID)) {
			proposals := e.proposer.next(BucketID(bucketID))
			actions.Append(e.sequences[index].allocate(proposals))
		} else {
			actions.Append(e.sequences[index].allocate(nil))
		}

		e.lowestUnallocated[int(bucketID)] += len(e.config.buckets)
	}

	return actions
}

func (e *epoch) lowWatermark() uint64 {
	return e.sequences[0].seqNo
}

func (e *epoch) highWatermark() uint64 {
	return e.sequences[len(e.sequences)-1].seqNo
}

func (e *epoch) constructEpochChange(newEpoch uint64) *pb.EpochChange {
	epochChange := &pb.EpochChange{
		NewEpoch: newEpoch,
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
	}

	for _, seq := range e.sequences {
		if seq.state < Preprepared {
			continue
		}

		if seq.seqNo < epochChange.Checkpoints[0].SeqNo {
			// We retain more sequences than the latest stable checkpoint
			// so don't send those
			continue
		}

		entry := &pb.EpochChange_SetEntry{
			Epoch:  seq.epoch,
			SeqNo:  seq.seqNo,
			Digest: seq.digest,
		}

		epochChange.QSet = append(epochChange.QSet, entry)

		if seq.state < Prepared {
			continue
		}

		epochChange.PSet = append(epochChange.PSet, entry)

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
