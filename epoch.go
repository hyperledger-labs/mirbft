/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"

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

/*
func (ec *epochConfig) intersectionQuorum() int {
	return intersectionQuorum(ec.networkConfig)
}

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
	lowestUnallocated []int // index by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  int

	checkpoints       []*checkpoint
	checkpointTracker *checkpointTracker

	baseCheckpoint *pb.Checkpoint
}

// newEpoch creates a new epoch.  It uses the supplied initial checkpoints until
// new checkpoint windows are created using the given epochConfig.  The initialCheckpoint
// windows may be empty, of length 1, or length 2.
func newEpoch(newEpochConfig *pb.EpochConfig, checkpointTracker *checkpointTracker, clientWindows *clientWindows, lastEpoch *epoch, networkConfig *pb.NetworkConfig, myConfig *Config) *epoch {

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

	sequences := make([]*sequence, config.logWidth())
	for i := range sequences {
		seqNo := newEpochConfig.StartingCheckpoint.SeqNo + 1 + uint64(i)
		bucket := config.seqToBucket(seqNo)
		owner := config.buckets[bucket]

		sequences[i] = newSequence(owner, config.number, seqNo, networkConfig, myConfig)
	}

	if lastEpoch != nil {
		j := 0
	outer:
		for _, oldSeq := range lastEpoch.sequences {
			for ; j < len(newEpochConfig.FinalPreprepares); j++ {
				newSeq := sequences[j]
				if newSeq.seqNo < oldSeq.seqNo {
					continue
				}

				if newSeq.seqNo > oldSeq.seqNo {
					break
				}

				newSeq.state = Prepared
				newSeq.digest = newEpochConfig.FinalPreprepares[j]

				if newSeq.digest == nil {
					newSeq.qEntry = &pb.QEntry{
						Epoch: newSeq.epoch,
						SeqNo: newSeq.seqNo,
					}
				} else if bytes.Equal(oldSeq.digest, newSeq.digest) {
					newSeq.qEntry = &pb.QEntry{
						Epoch:    newSeq.epoch,
						SeqNo:    newSeq.seqNo,
						Digest:   newSeq.digest,
						Requests: oldSeq.qEntry.Requests,
					}
					newSeq.batch = oldSeq.batch

					if oldSeq.state == Committed {
						for _, batchEntry := range newSeq.batch {
							batchEntry.state = Committed
						}
						newSeq.state = Committed
					}

					continue outer
				} else {
					panic(fmt.Sprintf("we need persistence and or state transfer to handle this path, epoch=%d seqno=%d digest=%x bucket=%d", newSeq.epoch, newSeq.seqNo, newSeq.digest, config.seqToBucket(newSeq.seqNo)))
				}
				break
			}

			if oldSeq.batch != nil && oldSeq.owner == NodeID(myConfig.ID) {
				for _, request := range oldSeq.batch {
					clientWindow, ok := clientWindows.clientWindow(request.requestData.ClientId)
					if !ok {
						panic(fmt.Sprintf("epoch tried to start with request from unknown client"))

					}

					reqNo := request.requestData.ReqNo

					if reqNo < clientWindow.lowWatermark {
						// This request already committed somewhere
						continue
					}

					if reqNo > clientWindow.highWatermark {
						panic("we should not be processing reqnos which are above the watermarks for this checkpoint")
					}

					newRequest := clientWindow.request(reqNo)
					if request != newRequest {
						// TODO don't lose data that we once allocated, but got dropped
						panic(fmt.Sprintf("about to abandon a request %d %x", reqNo, request.digest))
					}
				}
			}
		}

		for ; j < len(newEpochConfig.FinalPreprepares); j++ {
			newSeq := sequences[j]
			newSeq.state = Prepared
			newSeq.digest = newEpochConfig.FinalPreprepares[j]
			if newSeq.digest == nil {
				newSeq.qEntry = &pb.QEntry{
					Epoch: newSeq.epoch,
					SeqNo: newSeq.seqNo,
				}
			} else {
				panic(fmt.Sprintf("we need persistence and or state transfer to handle this path, epoch=%d seqno=%d digest=%v bucket=%d", newSeq.epoch, newSeq.seqNo, newSeq.digest, config.seqToBucket(newSeq.seqNo)))
			}
		}
	}

	proposer := newProposer(myConfig, clientWindows, config.buckets)
	proposer.stepAllRequestWindows()

	return &epoch{
		baseCheckpoint:    newEpochConfig.StartingCheckpoint,
		myConfig:          myConfig,
		config:            config,
		checkpointTracker: checkpointTracker,
		checkpoints:       checkpoints,
		proposer:          proposer,
		sequences:         sequences,
		lowestUnallocated: make([]int, len(config.buckets)),
	}
}

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch []*request) *Actions {
	baseCheckpoint := e.baseCheckpoint
	offset := int(seqNo-baseCheckpoint.SeqNo) - 1
	seq := e.sequences[offset]
	if source == NodeID(e.myConfig.ID) {
		// Apply our own preprepares as a prepare
		return seq.applyPrepareMsg(source, seq.digest)
	}

	for _, request := range batch {
		if request.state == Invalid {
			return seq.allocateInvalid(batch)
		}
	}

	return seq.allocate(batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
	return e.sequences[offset].applyPrepareMsg(source, digest)
}

func (e *epoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
	actions := e.sequences[offset].applyCommitMsg(source, digest)

	if len(actions.Commits) > 0 && offset == e.lowestUncommitted {
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
		for i := 0; i < ci; i++ {
			seqNo := lastCW.end + uint64(i) + 1
			epoch := e.config.number
			owner := e.config.buckets[e.config.seqToBucket(seqNo)]
			e.sequences = append(e.sequences, newSequence(owner, epoch, seqNo, e.config.networkConfig, e.myConfig))
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

func (e *epoch) advanceLowestUnallocated() {
	// TODO, this could definitely be made more efficient

	for bucketID, index := range e.lowestUnallocated {
		if index < 0 {
			// If we haven't advanced since the watermarks moved, we might
			// not have skipped the sequences we didn't own and now be negative.
			e.lowestUnallocated[bucketID] = 0
		}
		for i := e.lowestUnallocated[bucketID]; i < len(e.sequences); i++ {
			seq := e.sequences[i]
			e.lowestUnallocated[bucketID] = i
			if seq.state != Uninitialized || e.config.seqToBucket(seq.seqNo) != BucketID(bucketID) {
				continue
			}
			break
		}
	}
}

func (e *epoch) drainProposer() *Actions {
	actions := &Actions{}

	for bucketID, ownerID := range e.config.buckets {
		if ownerID != NodeID(e.myConfig.ID) {
			continue
		}

		for e.proposer.hasPending(bucketID) {
			e.advanceLowestUnallocated()

			i := e.lowestUnallocated[int(bucketID)]
			if i > len(e.sequences) {
				break
			}
			seq := e.sequences[i]

			if len(e.sequences)-i <= int(e.config.networkConfig.CheckpointInterval) {
				// let the network move watermarks before filling up the last checkpoint
				// interval
				break
			}

			if ownerID == NodeID(e.myConfig.ID) && e.proposer.hasPending(bucketID) {
				proposals := e.proposer.next(bucketID)
				actions.Append(seq.allocate(proposals))
			}
		}
	}

	return actions
}

func (e *epoch) applyProcessResult(seqNo uint64, digest []byte) *Actions {
	offset := int(seqNo-e.baseCheckpoint.SeqNo) - 1
	seq := e.sequences[offset]
	actions := seq.applyProcessResult(digest)
	if seq.owner != NodeID(e.myConfig.ID) {
		actions.Append(seq.applyPrepareMsg(seq.owner, digest))
	}
	return actions
}

func (e *epoch) tick() *Actions {
	actions := &Actions{} // TODO, only heartbeat if no progress
	if e.myConfig.HeartbeatTicks != 0 && e.ticks%uint64(e.myConfig.HeartbeatTicks) == 0 {
		e.advanceLowestUnallocated()
		for bucketID, index := range e.lowestUnallocated {
			if e.config.buckets[BucketID(bucketID)] != NodeID(e.myConfig.ID) {
				continue
			}

			if len(e.sequences)-index <= int(e.config.networkConfig.CheckpointInterval) {
				continue
			}

			if e.proposer.hasOutstanding(BucketID(bucketID)) {
				proposals := e.proposer.next(BucketID(bucketID))
				actions.Append(e.sequences[index].allocate(proposals))
			} else {
				actions.Append(e.sequences[index].allocate(nil))
			}
		}
	}

	if e.lowestUncommitted >= len(e.sequences) || e.sequences[e.lowestUncommitted].seqNo == e.lastCommittedAtTick+1 {
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
		return actions
	} else {
		e.ticksSinceProgress = 0
		e.lastCommittedAtTick = e.sequences[e.lowestUncommitted].seqNo - 1
	}

	return actions
}

/*
func (e *epoch) lowWatermark() uint64 {
	return e.sequences[0].seqNo
}
*/

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
		if seq.state < Prepared {
			continue
		}

		entry := &pb.EpochChange_SetEntry{
			Epoch:  seq.epoch,
			SeqNo:  seq.seqNo,
			Digest: seq.digest,
		}

		epochChange.QSet = append(epochChange.QSet, entry)

		if seq.state < Preprepared {
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
