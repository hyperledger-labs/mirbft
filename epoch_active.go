/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
)

type activeEpoch struct {
	epochConfig   *pb.EpochConfig
	networkConfig *pb.NetworkState_Config
	myConfig      *pb.StateEvent_InitialParameters
	logger        Logger

	outstandingReqs *allOutstandingReqs
	proposer        *proposer
	persisted       *persisted
	commitState     *commitState

	buckets   map[bucketID]nodeID
	sequences [][]*sequence

	lowestUncommitted uint64   // seqNo
	lowestUnallocated []uint64 // seqNo indexed by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  uint32
}

func newActiveEpoch(epochConfig *pb.EpochConfig, persisted *persisted, commitState *commitState, clientTracker *clientTracker, myConfig *pb.StateEvent_InitialParameters, logger Logger) *activeEpoch {
	var startingEntry *logEntry
	var maxCheckpoint *pb.CEntry

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_EpochChange:
			startingEntry = head.next
		case *pb.Persistent_CEntry:
			startingEntry = head.next
			maxCheckpoint = d.CEntry
		}
	}

	networkConfig := maxCheckpoint.NetworkState.Config

	outstandingReqs := newOutstandingReqs(clientTracker, maxCheckpoint.NetworkState)

	buckets := map[bucketID]nodeID{}

	leaders := map[uint64]struct{}{}
	for _, leader := range epochConfig.Leaders {
		leaders[leader] = struct{}{}
	}

	overflowIndex := 0 // TODO, this should probably start after the last assigned node
	for i := 0; i < int(networkConfig.NumberOfBuckets); i++ {
		bucketID := bucketID(i)
		leader := networkConfig.Nodes[(uint64(i)+epochConfig.Number)%uint64(len(networkConfig.Nodes))]
		if _, ok := leaders[leader]; !ok {
			buckets[bucketID] = nodeID(epochConfig.Leaders[overflowIndex%len(epochConfig.Leaders)])
			overflowIndex++
		} else {
			buckets[bucketID] = nodeID(leader)
		}
	}

	lowestUnallocated := make([]uint64, len(buckets))
	for i := range lowestUnallocated {
		firstSeqNo := maxCheckpoint.SeqNo + uint64(i+1)
		lowestUnallocated[int(seqToBucket(firstSeqNo, networkConfig))] = firstSeqNo
	}

	sequences := make([][]*sequence, 0, 3)
	ci := int(networkConfig.CheckpointInterval)
	for seqNo := maxCheckpoint.SeqNo + 1; seqNo <= commitState.stopAtSeqNo; seqNo++ {
		bucket := seqToBucket(seqNo, networkConfig)
		owner := buckets[bucket]
		i := int(seqNo - maxCheckpoint.SeqNo - 1)
		ciIndex := i / ci
		if ciIndex == len(sequences) {
			sequences = append(sequences, make([]*sequence, ci))
		}
		ciOffset := i % ci
		sequences[ciIndex][ciOffset] = newSequence(owner, epochConfig.Number, seqNo, persisted, networkConfig, myConfig, logger)
	}

	for logEntry := startingEntry; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persistent_QEntry:
			ciIndex := (int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1) / ci
			ciOffset := (int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1) % ci
			if ciIndex < 0 {
				// The epoch change selected an earlier checkpoint as its base
				// but we have already committed this entry, skipping.
				continue
			}
			if ciIndex >= len(sequences) {
				panic(fmt.Sprintf("should never be possible, QEntry seqNo=%d but started from checkpoint %d with log width of %d", d.QEntry.SeqNo, maxCheckpoint.SeqNo, len(sequences)))
			}
			bucket := seqToBucket(d.QEntry.SeqNo, networkConfig)
			err := outstandingReqs.applyBatch(bucket, d.QEntry.Requests)
			if err != nil {
				panic(fmt.Sprintf("need to handle holes: %s", err))
			}

			lowestUnallocated[bucket] = d.QEntry.SeqNo + uint64(len(buckets))
			sequences[ciIndex][ciOffset].qEntry = d.QEntry
			sequences[ciIndex][ciOffset].digest = d.QEntry.Digest
			sequences[ciIndex][ciOffset].state = sequencePreprepared
		case *pb.Persistent_PEntry:
			ciIndex := (int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1) / ci
			ciOffset := (int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1) % ci
			if ciIndex < 0 {
				// The epoch change selected an earlier checkpoint as its base
				// but we have already committed this entry, skipping.
				continue
			}
			if ciIndex >= len(sequences) {
				panic(fmt.Sprintf("should never be possible, QEntry seqNo=%d but started from checkpoint %d with log width of %d", d.PEntry.SeqNo, maxCheckpoint.SeqNo, len(sequences)))
			}

			// XXX are we sure this is the right logic? In general,
			// during epoch change we commit all the sequences during the change,
			// and on crash, we start from a checkpoint.  Alternatively
			// lastCommit is the last action we sent for a commit, not what
			// we actually know to be committed.
			if commitState.lastCommit >= d.PEntry.SeqNo {
				sequences[ciIndex][ciOffset].state = sequenceCommitted
			} else {
				sequences[ciIndex][ciOffset].state = sequencePrepared
			}
		}
	}

	lowestUncommitted := commitState.lastCommit + 1

	proposer := newProposer(
		maxCheckpoint.SeqNo,
		uint64(networkConfig.CheckpointInterval),
		myConfig,
		clientTracker,
		buckets,
	)

	return &activeEpoch{
		buckets:           buckets,
		myConfig:          myConfig,
		epochConfig:       epochConfig,
		networkConfig:     networkConfig,
		persisted:         persisted,
		commitState:       commitState,
		proposer:          proposer,
		sequences:         sequences,
		lowestUnallocated: lowestUnallocated,
		lowestUncommitted: lowestUncommitted,
		outstandingReqs:   outstandingReqs,
		logger:            logger,
	}
}

func (e *activeEpoch) seqToBucket(seqNo uint64) bucketID {
	return seqToBucket(seqNo, e.networkConfig)
}

func (e *activeEpoch) sequence(seqNo uint64) *sequence {
	ci := int(e.networkConfig.CheckpointInterval)
	ciIndex := int(seqNo-e.lowWatermark()) / ci
	ciOffset := int(seqNo-e.lowWatermark()) % ci
	if ciIndex > len(e.sequences) {
		panic(fmt.Sprintf("dev error: low=%d high=%d seqno=%d ciIndex=%d ciOffset=%d len(sequences)=%d", e.lowWatermark(), e.highWatermark(), seqNo, ciIndex, ciOffset, len(e.sequences)))
	}
	return e.sequences[ciIndex][ciOffset]
}

func (e *activeEpoch) inWatermarks(seqNo uint64) bool {
	return seqNo >= e.lowWatermark() && seqNo <= e.highWatermark()
}

func (e *activeEpoch) applyPreprepareMsg(source nodeID, seqNo uint64, batch []*pb.RequestAck) *Actions {
	if !e.inWatermarks(seqNo) {
		return &Actions{}
	}

	seq := e.sequence(seqNo)

	if seq.owner == nodeID(e.myConfig.Id) {
		// We already performed the unallocated movement when we allocated the seq
		return seq.applyPrepareMsg(source, seq.digest)
	}

	bucketID := e.seqToBucket(seqNo)

	if seqNo != e.lowestUnallocated[int(bucketID)] {
		panic(fmt.Sprintf("dev test, this really shouldn't happen: seqNo=%d e.lowestUnallocated=%d\n", seqNo, e.lowestUnallocated[int(bucketID)]))
	}

	e.lowestUnallocated[int(bucketID)] += uint64(len(e.buckets))

	// Note, this allocates the sequence inside, as we need to track
	// outstanding requests before transitioning the sequence to preprepared
	actions, err := e.outstandingReqs.applyAcks(bucketID, seq, batch)
	if err != nil {
		panic(fmt.Sprintf("handle me, we need to stop the bucket and suspect: %s", err))
	}

	return actions
}

func (e *activeEpoch) applyPrepareMsg(source nodeID, seqNo uint64, digest []byte) *Actions {
	if !e.inWatermarks(seqNo) {
		return &Actions{}
	}

	seq := e.sequence(seqNo)

	return seq.applyPrepareMsg(source, digest)
}

func (e *activeEpoch) applyCommitMsg(source nodeID, seqNo uint64, digest []byte) *Actions {
	if !e.inWatermarks(seqNo) {
		return &Actions{}
	}

	seq := e.sequence(seqNo)

	seq.applyCommitMsg(source, digest)
	if seq.state != sequenceCommitted || seqNo != e.lowestUncommitted {
		return &Actions{}
	}

	actions := &Actions{}

	for e.lowestUncommitted <= e.highWatermark() {
		seq := e.sequence(e.lowestUncommitted)
		if seq.state != sequenceCommitted {
			break
		}

		e.commitState.commit(seq.qEntry)
		e.lowestUncommitted++
	}

	return actions
}

func (e *activeEpoch) moveLowWatermark(seqNo uint64) (*Actions, bool) {
	if seqNo == e.epochConfig.PlannedExpiration {
		return &Actions{}, false
	}

	ci := int(e.networkConfig.CheckpointInterval)
	ciIndex := int(seqNo-e.lowWatermark()) / ci
	e.sequences = e.sequences[ciIndex+1:]

	return e.advance(), true
}

func (e *activeEpoch) advance() *Actions {
	actions := &Actions{}

	if e.highWatermark() > e.epochConfig.PlannedExpiration || e.highWatermark() > e.commitState.stopAtSeqNo {
		panic("dev sanity check")
	}

	if e.highWatermark() < e.epochConfig.PlannedExpiration &&

		e.highWatermark() < e.commitState.stopAtSeqNo &&
		len(e.sequences) <= 3 {
		ci := int(e.networkConfig.CheckpointInterval)
		newSequences := make([]*sequence, ci)
		for i := range newSequences {
			seqNo := e.highWatermark() + 1 + uint64(i)
			fmt.Printf("  JKY: allocating new equences for seqNo %d\n", seqNo)
			epoch := e.epochConfig.Number
			owner := e.buckets[e.seqToBucket(seqNo)]
			newSequences[i] = newSequence(owner, epoch, seqNo, e.persisted, e.networkConfig, e.myConfig, e.logger)
		}
		e.sequences = append(e.sequences, newSequences)
		fmt.Printf("JKY: draining proposer, adding new sequences\n")
	} else {
		fmt.Printf("    JKY not adding new sequences highWatermark=%d plannedExpiration=%d len(e.sequences)=%d stopAtSeqNo=%d\n", e.highWatermark(), e.epochConfig.PlannedExpiration, len(e.sequences), e.commitState.stopAtSeqNo)
	}

	for bucketID, ownerID := range e.buckets {
		if ownerID != nodeID(e.myConfig.Id) {
			continue
		}

		fmt.Printf("JKY: draining proposer, for my bucket %d\n", bucketID)
		prb := e.proposer.proposalBucket(bucketID)

		for {
			seqNo := e.lowestUnallocated[int(bucketID)]
			if seqNo > e.highWatermark() {
				break
			}

			if !prb.hasPending(seqNo) {
				break
			}

			fmt.Printf("JKY: allocating seqNo %d\n", seqNo)
			seq := e.sequence(seqNo)

			actions.concat(seq.allocateAsOwner(prb.next()))

			e.lowestUnallocated[int(bucketID)] += uint64(len(e.buckets))
		}
	}

	return actions
}

func (e *activeEpoch) applyBatchHashResult(seqNo uint64, digest []byte) *Actions {
	if !e.inWatermarks(seqNo) {
		// TODO, log?
		return &Actions{}
	}

	seq := e.sequence(seqNo)

	return seq.applyBatchHashResult(digest)
}

func (e *activeEpoch) tick() *Actions {
	if e.lastCommittedAtTick < e.commitState.lastCommit {
		e.lastCommittedAtTick = e.commitState.lastCommit
		e.ticksSinceProgress = 0
		return &Actions{}
	}

	e.ticksSinceProgress++
	actions := &Actions{}

	if e.ticksSinceProgress > e.myConfig.SuspectTicks {
		suspect := &pb.Suspect{
			Epoch: e.epochConfig.Number,
		}
		actions.send(e.networkConfig.Nodes, &pb.Msg{
			Type: &pb.Msg_Suspect{
				Suspect: suspect,
			},
		})
		actions.concat(e.persisted.addSuspect(suspect))
	}

	if e.myConfig.HeartbeatTicks == 0 || e.ticksSinceProgress%e.myConfig.HeartbeatTicks != 0 {
		return actions
	}

	for bid, unallocatedSeqNo := range e.lowestUnallocated {
		if unallocatedSeqNo > e.highWatermark() {
			continue
		}

		if e.buckets[bucketID(bid)] != nodeID(e.myConfig.Id) {
			continue
		}

		seq := e.sequence(unallocatedSeqNo)

		prb := e.proposer.proposalBucket(bucketID(bid))

		var clientReqs []*clientRequest

		if prb.hasOutstanding(unallocatedSeqNo) {
			clientReqs = prb.next()
		}

		actions.concat(seq.allocateAsOwner(clientReqs))

		e.lowestUnallocated[bid] += uint64(len(e.buckets))
	}

	return actions
}

func (e *activeEpoch) lowWatermark() uint64 {
	return e.sequences[0][0].seqNo
}

func (e *activeEpoch) highWatermark() uint64 {
	interval := e.sequences[len(e.sequences)-1]
	if interval[len(interval)-1] == nil {
		panic(fmt.Sprintf("sequence in %v is nil", interval))
	}
	return interval[len(interval)-1].seqNo
}

func (e *activeEpoch) status() []*status.Bucket {
	buckets := make([]*status.Bucket, len(e.buckets))
	for i := range buckets {
		buckets[i] = &status.Bucket{
			ID:        uint64(i),
			Leader:    e.buckets[bucketID(i)] == nodeID(e.myConfig.Id),
			Sequences: make([]status.SequenceState, len(e.sequences)*len(e.sequences[0])/len(buckets)),
		}
	}

	for seqNo := e.lowWatermark(); seqNo <= e.highWatermark(); seqNo++ {
		seq := e.sequence(seqNo)
		bucket := int(seqToBucket(seqNo, e.networkConfig))
		index := int(seqNo-e.lowWatermark()) / len(e.buckets)
		buckets[bucket].Sequences[index] = status.SequenceState(seq.state)
	}

	return buckets
}
