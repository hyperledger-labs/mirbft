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

func newActiveEpoch(epochConfig *pb.EpochConfig, persisted *persisted, nodeBuffers *nodeBuffers, commitState *commitState, clientTracker *clientTracker, myConfig *pb.StateEvent_InitialParameters, logger Logger) *activeEpoch {
	networkConfig := commitState.activeState.Config
	startingSeqNo := commitState.highestCommit + 1

	// fmt.Printf("JKY: starting new epoch seq is %d\n", startingSeqNo)

	outstandingReqs := newOutstandingReqs(clientTracker, commitState.activeState)

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
		firstSeqNo := startingSeqNo + uint64(i+1)
		lowestUnallocated[int(seqToBucket(firstSeqNo, networkConfig))] = firstSeqNo
	}

	lowestUncommitted := commitState.highestCommit + 1
	// fmt.Printf("JKY: setting lowestUncommitted to %d\n", lowestUncommitted)

	proposer := newProposer(
		startingSeqNo,
		uint64(networkConfig.CheckpointInterval),
		myConfig,
		clientTracker,
		buckets,
	)

	preprepareBuffers := make([]*preprepareBuffer, len(lowestUnallocated))
	for i, lu := range lowestUnallocated {
		preprepareBuffers[i] = &preprepareBuffer{
			nextSeqNo: lu,
			buffer: newMsgBuffer(
				fmt.Sprintf("epoch-%d-preprepare", epochConfig.Number),
				nodeBuffers.nodeBuffer(buckets[bucketID(i)]),
			),
		}
	}

	otherBuffers := map[nodeID]*msgBuffer{}
	for _, node := range networkConfig.Nodes {
		otherBuffers[nodeID(node)] = newMsgBuffer(
			fmt.Sprintf("epoch-%d-other", epochConfig.Number),
			nodeBuffers.nodeBuffer(nodeID(node)),
		)
	}

	return &activeEpoch{
		buckets:           buckets,
		myConfig:          myConfig,
		epochConfig:       epochConfig,
		networkConfig:     networkConfig,
		persisted:         persisted,
		commitState:       commitState,
		proposer:          proposer,
		sequences:         make([][]*sequence, 0, 3),
		preprepareBuffers: preprepareBuffers,
		otherBuffers:      otherBuffers,
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
	if ciIndex > len(e.sequences) || ciIndex < 0 || ciOffset < 0 {
		panic(fmt.Sprintf("dev error: low=%d high=%d seqno=%d ciIndex=%d ciOffset=%d len(sequences)=%d", e.lowWatermark(), e.highWatermark(), seqNo, ciIndex, ciOffset, len(e.sequences)))
	}

	sequence := e.sequences[ciIndex][ciOffset]
	if sequence.seqNo != seqNo {
		panic(fmt.Sprintf("dev sanity test -- tried to get seqNo=%d, but ended up with seqNo=%d", seqNo, sequence.seqNo))
	}

	return sequence
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
			nextMsg = preprepareBuffer.buffer.next(ae.filter)
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

	actions := e.advance()

	for seqNo > e.lowWatermark() {
		// fmt.Printf(" JKY: moving low watermark to %d, lowWatermark=%d highWatermark=%d\n", seqNo, e.lowWatermark(), e.highWatermark())

		e.sequences = e.sequences[1:]

	}

	return actions, false
}

func (e *activeEpoch) drainBuffers() *Actions {
	actions := &Actions{}

	for i := 0; i < len(e.buckets); i++ {
		preprepareBuffer := e.preprepareBuffers[bucketID(i)]
		source := e.buckets[bucketID(i)]
		nextMsg := preprepareBuffer.buffer.next(e.filter)
		if nextMsg == nil {
			continue
		}

		actions.concat(e.apply(source, nextMsg))
		// Note, below, we iterate over all msgs
		// but apply actually loops for us in the preprepare case
		// the difference being that non-preprepare messages
		// only change applayble state when watermarks move,
		// preprepare messages change applyable state after the
		// previous preprepare applies.
	}

	for _, id := range e.networkConfig.Nodes {
		e.otherBuffers[nodeID(id)].iterate(e.filter, func(id nodeID, msg *pb.Msg) {
			actions.concat(e.apply(id, msg))
		})
	}

	return actions
}

func (e *activeEpoch) advance() *Actions {
	actions := &Actions{}

	if e.highWatermark() > e.epochConfig.PlannedExpiration || e.highWatermark() > e.commitState.stopAtSeqNo {
		panic(fmt.Sprintf("dev sanity check -- highWatermark=%d plannedExpiration=%d stopAtSeqNo=%d", e.highWatermark(), e.epochConfig.PlannedExpiration, e.commitState.stopAtSeqNo))
	}

	ci := int(e.networkConfig.CheckpointInterval)

	for e.highWatermark() < e.epochConfig.PlannedExpiration &&
		e.highWatermark() < e.commitState.stopAtSeqNo {
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
	}

	actions.concat(e.drainBuffers())

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
			// fmt.Printf("   JKY: allocation moved %d to %d\n", seqNo, e.lowestUnallocated[int(bucketID)])
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
	if e.lastCommittedAtTick < e.commitState.highestCommit {
		e.lastCommittedAtTick = e.commitState.highestCommit
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
		// fmt.Printf("   JKY: tick moved %d to %d\n", seq.seqNo, e.lowestUnallocated[bid])
	}

	return actions
}

func (e *activeEpoch) lowWatermark() uint64 {
	return e.sequences[0][0].seqNo
}

func (e *activeEpoch) highWatermark() uint64 {
	if len(e.sequences) == 0 {
		return e.commitState.lowWatermark
	}

	interval := e.sequences[len(e.sequences)-1]
	if interval[len(interval)-1] == nil {
		panic(fmt.Sprintf("sequence in %v is nil", interval))
	}
	return interval[len(interval)-1].seqNo
}

func (e *activeEpoch) status() []*status.Bucket {
	if len(e.sequences) == 0 {
		return []*status.Bucket{}
	}

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
