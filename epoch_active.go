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

type preprepareBuffer struct {
	nextSeqNo uint64
	buffer    *msgBuffer
}

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

	preprepareBuffers []*preprepareBuffer // indexed by bucket
	otherBuffers      map[nodeID]*msgBuffer
	lowestUncommitted uint64   // seqNo
	lowestUnallocated []uint64 // seqNo indexed by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  uint32
}

func newActiveEpoch(epochConfig *pb.EpochConfig, persisted *persisted, commitState *commitState, clientTracker *clientTracker, myConfig *pb.StateEvent_InitialParameters, logger Logger) (*activeEpoch, *Actions) {
	var startingEntry *logEntry
	var maxCheckpoint *pb.CEntry

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_ECEntry:
			startingEntry = head.next
		case *pb.Persistent_CEntry:
			startingEntry = head.next
			maxCheckpoint = d.CEntry
		}
	}

	networkConfig := maxCheckpoint.NetworkState.Config

	// fmt.Printf("JKY: maxCheckpoint is %d\n", maxCheckpoint.SeqNo)

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

	actions := &Actions{}

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
		if ciOffset == 0 {
			actions.concat(persisted.addNEntry(&pb.NEntry{
				SeqNo:       seqNo,
				EpochConfig: epochConfig,
			}))
		}
		sequences[ciIndex][ciOffset] = newSequence(owner, epochConfig.Number, seqNo, persisted, networkConfig, myConfig, logger)
	}

	for logEntry := startingEntry; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persistent_QEntry:
			if d.QEntry.SeqNo <= maxCheckpoint.SeqNo {
				// The epoch change selected an earlier checkpoint as its base
				// but we have already committed this entry, skipping.
				continue
			}
			ciIndex := (int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1) / ci
			ciOffset := (int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1) % ci
			if ciIndex >= len(sequences) {
				panic(fmt.Sprintf("should never be possible, QEntry seqNo=%d but started from checkpoint %d with log width of %d", d.QEntry.SeqNo, maxCheckpoint.SeqNo, len(sequences)))
			}
			bucket := seqToBucket(d.QEntry.SeqNo, networkConfig)
			if commitState.lastCommit >= d.QEntry.SeqNo {
				// If we already committed this, attempting to find
				// the requests in the outstandingReqs will fail
				err := outstandingReqs.applyBatch(bucket, d.QEntry.Requests)
				if err != nil {
					panic(fmt.Sprintf("need to handle holes: %s", err))
				}
			}
			lowestUnallocated[bucket] = d.QEntry.SeqNo + uint64(len(buckets))
			sequences[ciIndex][ciOffset].qEntry = d.QEntry
			sequences[ciIndex][ciOffset].digest = d.QEntry.Digest
			sequences[ciIndex][ciOffset].state = sequencePreprepared
		case *pb.Persistent_PEntry:
			if d.PEntry.SeqNo <= maxCheckpoint.SeqNo {
				// The epoch change selected an earlier checkpoint as its base
				// but we have already committed this entry, skipping.
				continue
			}
			ciIndex := (int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1) / ci
			ciOffset := (int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1) % ci
			if ciIndex >= len(sequences) {
				panic(fmt.Sprintf("should never be possible, PEntry seqNo=%d but started from checkpoint %d with log width of %d", d.PEntry.SeqNo, maxCheckpoint.SeqNo, len(sequences)))
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

	preprepareBuffers := make([]*preprepareBuffer, len(lowestUnallocated))
	for i, lu := range lowestUnallocated {
		preprepareBuffers[i] = &preprepareBuffer{
			nextSeqNo: lu,
			buffer:    newMsgBuffer(myConfig, logger),
		}
	}

	otherBuffers := map[nodeID]*msgBuffer{}
	for _, node := range networkConfig.Nodes {
		otherBuffers[nodeID(node)] = newMsgBuffer(myConfig, logger)
	}

	return &activeEpoch{
		buckets:           buckets,
		myConfig:          myConfig,
		epochConfig:       epochConfig,
		networkConfig:     networkConfig,
		persisted:         persisted,
		commitState:       commitState,
		proposer:          proposer,
		sequences:         sequences,
		preprepareBuffers: preprepareBuffers,
		otherBuffers:      otherBuffers,
		lowestUnallocated: lowestUnallocated,
		lowestUncommitted: lowestUncommitted,
		outstandingReqs:   outstandingReqs,
		logger:            logger,
	}, actions
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

func (ae *activeEpoch) filter(source nodeID, msg *pb.Msg) applyable {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_Preprepare:
		seqNo := innerMsg.Preprepare.SeqNo

		bucketID := ae.seqToBucket(seqNo)
		owner := ae.buckets[bucketID]
		if owner != source {
			return invalid
		}

		if seqNo > ae.epochConfig.PlannedExpiration {
			return invalid
		}

		if seqNo > ae.highWatermark() {
			return future
		}

		if seqNo < ae.lowWatermark() {
			return past
		}

		nextPreprepare := ae.preprepareBuffers[int(bucketID)].nextSeqNo
		switch {
		case seqNo < nextPreprepare:
			return past
		case seqNo > nextPreprepare:
			return future
		default:
			return current
		}
	case *pb.Msg_Prepare:
		seqNo := innerMsg.Prepare.SeqNo

		bucketID := ae.seqToBucket(seqNo)
		owner := ae.buckets[bucketID]
		if owner == source {
			return invalid
		}

		if seqNo > ae.epochConfig.PlannedExpiration {
			return invalid
		}

		switch {
		case seqNo < ae.lowWatermark():
			return past
		case seqNo > ae.highWatermark():
			return future
		default:
			return current
		}
	case *pb.Msg_Commit:
		seqNo := innerMsg.Commit.SeqNo

		if seqNo > ae.epochConfig.PlannedExpiration {
			return invalid
		}

		switch {
		case seqNo < ae.lowWatermark():
			return past
		case seqNo > ae.highWatermark():
			return future
		default:
			return current
		}
	default:
		panic(fmt.Sprintf("unexpected msg type: %T", msg.Type))
	}
}

func (ae *activeEpoch) apply(source nodeID, msg *pb.Msg) *Actions {
	actions := &Actions{}

	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_Preprepare:
		bucket := ae.seqToBucket(innerMsg.Preprepare.SeqNo)
		preprepareBuffer := ae.preprepareBuffers[bucket]
		nextMsg := msg
		for nextMsg != nil {
			ppMsg := nextMsg.Type.(*pb.Msg_Preprepare).Preprepare
			actions.concat(ae.applyPreprepareMsg(source, ppMsg.SeqNo, ppMsg.Batch))
			preprepareBuffer.nextSeqNo += uint64(len(ae.buckets))
			nextMsg = preprepareBuffer.buffer.next(func(nextMsg *pb.Msg) applyable {
				return ae.filter(source, nextMsg)
			})
		}
	case *pb.Msg_Prepare:
		msg := innerMsg.Prepare
		actions.concat(ae.applyPrepareMsg(source, msg.SeqNo, msg.Digest))
	case *pb.Msg_Commit:
		msg := innerMsg.Commit
		actions.concat(ae.applyCommitMsg(source, msg.SeqNo, msg.Digest))
	default:
		panic(fmt.Sprintf("unexpected msg type: %T", msg.Type))
	}

	return actions
}

func (ae *activeEpoch) step(source nodeID, msg *pb.Msg) *Actions {
	switch ae.filter(source, msg) {
	case past:
	case future:
		switch innerMsg := msg.Type.(type) {
		case *pb.Msg_Preprepare:
			bucket := ae.seqToBucket(innerMsg.Preprepare.SeqNo)
			ae.preprepareBuffers[int(bucket)].buffer.store(msg)
		default:
			ae.otherBuffers[source].store(msg)
		}
	case invalid:
		// TODO Log?
	default: // current
		return ae.apply(source, msg)
	}
	return &Actions{}
}

func (e *activeEpoch) inWatermarks(seqNo uint64) bool {
	return seqNo >= e.lowWatermark() && seqNo <= e.highWatermark()
}

func (e *activeEpoch) applyPreprepareMsg(source nodeID, seqNo uint64, batch []*pb.RequestAck) *Actions {
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
		panic(fmt.Sprintf("handle me, seq_no=%d we need to stop the bucket and suspect: %s", seqNo, err))
	}

	return actions
}

func (e *activeEpoch) applyPrepareMsg(source nodeID, seqNo uint64, digest []byte) *Actions {
	seq := e.sequence(seqNo)

	return seq.applyPrepareMsg(source, digest)
}

func (e *activeEpoch) applyCommitMsg(source nodeID, seqNo uint64, digest []byte) *Actions {
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
		return &Actions{}, true
	}

	if seqNo == e.commitState.stopAtSeqNo {
		return &Actions{}, true
	}

	ci := int(e.networkConfig.CheckpointInterval)
	ciIndex := int(seqNo-e.lowWatermark()) / ci
	e.sequences = e.sequences[ciIndex+1:]

	return e.advance(), false
}

func (e *activeEpoch) drainBuffers() *Actions {
	actions := &Actions{}

	for i := 0; i < len(e.buckets); i++ {
		preprepareBuffer := e.preprepareBuffers[bucketID(i)]
		source := e.buckets[bucketID(i)]
		nextMsg := preprepareBuffer.buffer.next(func(msg *pb.Msg) applyable {
			return e.filter(source, msg)
		})
		if nextMsg == nil {
			continue
		}

		actions.concat(e.apply(source, nextMsg))
		// Note, below, we loop until nextMsg is nil,
		// but apply actually loops for us in the preprepare case
		// the difference being that non-preprepare messages
		// only change applayble state when watermarks move,
		// preprepare messages change applyable state after the
		// previous preprepare applies.
	}

	for _, id := range e.networkConfig.Nodes {
		buffer := e.otherBuffers[nodeID(id)]
		for {
			nextMsg := buffer.next(func(msg *pb.Msg) applyable {
				return e.filter(nodeID(id), msg)
			}) // TODO, this is painfully inefficient, have an iterator on the buffer
			if nextMsg == nil {
				break
			}
			actions.concat(e.apply(nodeID(id), nextMsg))
		}
	}

	return actions
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
		actions.concat(e.persisted.addNEntry(&pb.NEntry{
			SeqNo:       e.highWatermark() + 1,
			EpochConfig: e.epochConfig,
		}))
		for i := range newSequences {
			seqNo := e.highWatermark() + 1 + uint64(i)
			// fmt.Printf("  JKY: allocating new equences for seqNo %d\n", seqNo)
			epoch := e.epochConfig.Number
			owner := e.buckets[e.seqToBucket(seqNo)]
			newSequences[i] = newSequence(owner, epoch, seqNo, e.persisted, e.networkConfig, e.myConfig, e.logger)
		}
		e.sequences = append(e.sequences, newSequences)
		// fmt.Printf("JKY: draining proposer, adding new sequences\n")

		actions.concat(e.drainBuffers())
	}

	e.proposer.advance(e.lowestUncommitted)

	for bucketID, ownerID := range e.buckets {
		if ownerID != nodeID(e.myConfig.Id) {
			continue
		}

		// fmt.Printf("JKY: draining proposer, for my bucket %d\n", bucketID)
		prb := e.proposer.proposalBucket(bucketID)

		for {
			seqNo := e.lowestUnallocated[int(bucketID)]
			if seqNo > e.highWatermark() {
				break
			}

			if !prb.hasPending(seqNo) {
				// fmt.Printf("  JKY: my bucket has no pending requests for seqNo %d\n", seqNo)
				break
			}

			// fmt.Printf("JKY: allocating seqNo %d\n", seqNo)
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
