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

type activeEpoch struct {
	epochConfig   *pb.EpochConfig
	networkConfig *pb.NetworkState_Config
	myConfig      *pb.StateEvent_InitialParameters
	logger        Logger

	outstandingReqs *allOutstandingReqs
	proposer        *proposer
	persisted       *persisted

	buckets   map[BucketID]NodeID
	sequences []*sequence

	ending            bool // set when this epoch about to end gracefully
	lowestUncommitted int
	lowestUnallocated []int // index by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  uint32
}

func newActiveEpoch(persisted *persisted, clientWindows *clientWindows, myConfig *pb.StateEvent_InitialParameters, logger Logger) *activeEpoch {
	var startingEntry *logEntry
	var maxCheckpoint *pb.CEntry
	var epochConfig *pb.EpochConfig

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_NewEpochStart:
			epochConfig = d.NewEpochStart
		case *pb.Persistent_EpochChange:
			startingEntry = head.next
		case *pb.Persistent_CEntry:
			startingEntry = head.next
			maxCheckpoint = d.CEntry
			epochConfig = d.CEntry.EpochConfig
		}
	}

	networkConfig := maxCheckpoint.NetworkState.Config

	outstandingReqs := newOutstandingReqs(clientWindows, maxCheckpoint.NetworkState)

	buckets := map[BucketID]NodeID{}

	leaders := map[uint64]struct{}{}
	for _, leader := range epochConfig.Leaders {
		leaders[leader] = struct{}{}
	}

	overflowIndex := 0 // TODO, this should probably start after the last assigned node
	for i := 0; i < int(networkConfig.NumberOfBuckets); i++ {
		bucketID := BucketID(i)
		leader := networkConfig.Nodes[(uint64(i)+epochConfig.Number)%uint64(len(networkConfig.Nodes))]
		if _, ok := leaders[leader]; !ok {
			buckets[bucketID] = NodeID(epochConfig.Leaders[overflowIndex%len(epochConfig.Leaders)])
			overflowIndex++
		} else {
			buckets[bucketID] = NodeID(leader)
		}
	}

	lowestUnallocated := make([]int, len(buckets))
	for i := range lowestUnallocated {
		lowestUnallocated[int(seqToBucket(maxCheckpoint.SeqNo+uint64(i+1), epochConfig, networkConfig))] = i
	}

	sequences := make([]*sequence, logWidth(networkConfig))
	for i := range sequences {
		seqNo := maxCheckpoint.SeqNo + uint64(i+1)
		bucket := seqToBucket(seqNo, epochConfig, networkConfig)
		owner := buckets[bucket]
		sequences[i] = newSequence(owner, epochConfig.Number, seqNo, persisted, networkConfig, myConfig, logger)
	}

	for logEntry := startingEntry; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persistent_QEntry:
			offset := int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1
			if offset < 0 || offset >= len(sequences) {
				panic("should never be possible") // TODO, improve
			}
			bucket := seqToBucket(d.QEntry.SeqNo, epochConfig, networkConfig)
			err := outstandingReqs.applyBatch(bucket, d.QEntry.Requests)
			if err != nil {
				panic(fmt.Sprintf("need to handle holes: %s", err))
			}

			lowestUnallocated[bucket] = offset + len(buckets)
			sequences[offset].qEntry = d.QEntry
			sequences[offset].digest = d.QEntry.Digest
			sequences[offset].state = Preprepared
		case *pb.Persistent_PEntry:
			offset := int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1
			if offset < 0 || offset >= len(sequences) {
				panic("should never be possible") // TODO, improve
			}

			if persisted.lastCommitted >= d.PEntry.SeqNo {
				sequences[offset].state = Committed
			} else {
				sequences[offset].state = Prepared
			}
		}
	}

	lowestUncommitted := int(persisted.lastCommitted - maxCheckpoint.SeqNo)

	proposer := newProposer(myConfig, clientWindows, buckets)

	return &activeEpoch{
		buckets:           buckets,
		myConfig:          myConfig,
		epochConfig:       epochConfig,
		networkConfig:     networkConfig,
		persisted:         persisted,
		proposer:          proposer,
		sequences:         sequences,
		lowestUnallocated: lowestUnallocated,
		lowestUncommitted: lowestUncommitted,
		outstandingReqs:   outstandingReqs,
		logger:            logger,
	}
}

func (e *activeEpoch) seqToBucket(seqNo uint64) BucketID {
	return seqToBucket(seqNo, e.epochConfig, e.networkConfig)
}

func (e *activeEpoch) getSequence(seqNo uint64) (*sequence, int, error) {
	if seqNo < e.lowWatermark() || seqNo > e.highWatermark() {
		return nil, 0, errors.Errorf("requested seq no (%d) is out of range [%d - %d]",
			seqNo, e.lowWatermark(), e.highWatermark())
	}
	offset := int(seqNo - e.lowWatermark())
	if offset > len(e.sequences) {
		panic(fmt.Sprintf("dev error: low=%d high=%d seqno=%d offset=%d len(sequences)=%d", e.lowWatermark(), e.highWatermark(), seqNo, offset, len(e.sequences)))
	}
	return e.sequences[offset], offset, nil
}

func (e *activeEpoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch []*pb.RequestAck) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.logger.Error(err.Error())
		return &Actions{}
	}

	if seq.owner == NodeID(e.myConfig.Id) {
		// We already performed the unallocated movement when we allocated the seq
		return seq.applyPrepareMsg(source, seq.digest)
	}

	bucketID := e.seqToBucket(seqNo)

	if offset != e.lowestUnallocated[int(bucketID)] {
		panic(fmt.Sprintf("dev test, this really shouldn't happen: offset=%d e.lowestUnallocated=%d\n", offset, e.lowestUnallocated[int(bucketID)]))
	}

	e.lowestUnallocated[int(bucketID)] += len(e.buckets)

	// Note, this allocates the sequence inside, as we need to track
	// outstanding requests before transitioning the sequence to preprepared
	actions, err := e.outstandingReqs.applyAcks(bucketID, seq, batch)
	if err != nil {
		panic(fmt.Sprintf("handle me, we need to stop the bucket and suspect: %s", err))
	}

	return actions
}

func (e *activeEpoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.logger.Error(err.Error())
		return &Actions{}
	}
	return seq.applyPrepareMsg(source, digest)
}

func (e *activeEpoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.logger.Error(err.Error())
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
			QEntry:      e.sequences[e.lowestUncommitted].qEntry,
			EpochConfig: e.epochConfig,
		})

		e.lowestUncommitted++
	}

	return actions
}

func (e *activeEpoch) moveWatermarks(seqNo uint64) *Actions {
	for _, seq := range e.sequences {
		if seq.seqNo > seqNo {
			break
		}

		// XXX this is really a pretty inefficient
		// we can and should do better
		e.sequences = e.sequences[1:]
		for i := range e.lowestUnallocated {
			e.lowestUnallocated[i]--
		}
		e.lowestUncommitted--

		newSeqNo := e.sequences[len(e.sequences)-1].seqNo + 1
		epoch := e.epochConfig.Number
		owner := e.buckets[e.seqToBucket(newSeqNo)]
		e.sequences = append(e.sequences, newSequence(owner, epoch, newSeqNo, e.persisted, e.networkConfig, e.myConfig, e.logger))
		if newSeqNo == e.epochConfig.PlannedExpiration {
			e.ending = true
			break
		}
	}

	return e.drainProposer()
}

func (e *activeEpoch) drainProposer() *Actions {
	actions := &Actions{}

	for bucketID, ownerID := range e.buckets {
		if ownerID != NodeID(e.myConfig.Id) {
			continue
		}

		prb := e.proposer.proposalBucket(bucketID)

		for prb.hasPending() {
			i := e.lowestUnallocated[int(bucketID)]
			if i >= len(e.sequences) {
				break
			}
			seq := e.sequences[i]

			if len(e.sequences)-i <= int(e.networkConfig.CheckpointInterval) && !e.ending {
				// let the network move watermarks before filling up the last checkpoint
				// interval
				break
			}

			actions.concat(seq.allocateAsOwner(prb.next()))

			e.lowestUnallocated[int(bucketID)] += len(e.buckets)
		}
	}

	return actions
}

func (e *activeEpoch) applyBatchHashResult(seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.logger.Error(err.Error())
		return &Actions{}
	}

	return seq.applyBatchHashResult(digest)
}

func (e *activeEpoch) tick() *Actions {
	if e.lastCommittedAtTick < e.persisted.lastCommitted {
		e.lastCommittedAtTick = e.persisted.lastCommitted
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

	for bucketID, index := range e.lowestUnallocated {
		if index >= len(e.sequences) {
			continue
		}

		if e.buckets[BucketID(bucketID)] != NodeID(e.myConfig.Id) {
			continue
		}

		if len(e.sequences)-index <= int(e.networkConfig.CheckpointInterval) && !e.ending {
			continue
		}

		seq := e.sequences[index]

		prb := e.proposer.proposalBucket(BucketID(bucketID))

		var clientReqs []*clientRequest

		if prb.hasOutstanding() {
			clientReqs = prb.next()
		}

		actions.concat(seq.allocateAsOwner(clientReqs))

		e.lowestUnallocated[int(bucketID)] += len(e.buckets)
	}

	return actions
}

func (e *activeEpoch) lowWatermark() uint64 {
	return e.sequences[0].seqNo
}

func (e *activeEpoch) highWatermark() uint64 {
	return e.sequences[len(e.sequences)-1].seqNo
}

func (e *activeEpoch) status() []*BucketStatus {
	buckets := make([]*BucketStatus, len(e.buckets))
	for i := range buckets {
		bucket := &BucketStatus{
			ID:        uint64(i),
			Leader:    e.buckets[BucketID(i)] == NodeID(e.myConfig.Id),
			Sequences: make([]SequenceState, 0, len(e.sequences)/len(buckets)),
		}

		for j := i; j < len(e.sequences); j = j + len(buckets) {
			bucket.Sequences = append(bucket.Sequences, e.sequences[j].state)
		}

		buckets[i] = bucket
	}

	return buckets
}
