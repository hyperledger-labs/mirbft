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

type epoch struct {
	epochConfig   *pb.EpochConfig
	networkConfig *pb.NetworkState_Config
	myConfig      *Config

	outstandingReqs *allOutstandingReqs
	proposer        *proposer
	persisted       *persisted

	buckets   map[BucketID]NodeID
	sequences []*sequence

	ending                 bool // set when this epoch about to end gracefully
	lowestUncommitted      int
	lowestUnallocated      []int // index by bucket
	lowestOwnedUnallocated []int // index by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  int
}

func newEpoch(persisted *persisted, clientWindows *clientWindows, myConfig *Config) *epoch {
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
	lowestOwnedUnallocated := make([]int, len(buckets))
	for i := range lowestUnallocated {
		lowestUnallocated[int(seqToBucket(maxCheckpoint.SeqNo+uint64(i+1), epochConfig, networkConfig))] = i
		lowestOwnedUnallocated[int(seqToBucket(maxCheckpoint.SeqNo+uint64(i+1), epochConfig, networkConfig))] = i
	}

	sequences := make([]*sequence, logWidth(networkConfig))
	for i := range sequences {
		seqNo := maxCheckpoint.SeqNo + uint64(i+1)
		bucket := seqToBucket(seqNo, epochConfig, networkConfig)
		owner := buckets[bucket]
		sequences[i] = newSequence(owner, epochConfig.Number, seqNo, persisted, networkConfig, myConfig)
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
			lowestOwnedUnallocated[bucket] = offset + len(buckets)
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

	return &epoch{
		buckets:                buckets,
		myConfig:               myConfig,
		epochConfig:            epochConfig,
		networkConfig:          networkConfig,
		persisted:              persisted,
		proposer:               proposer,
		sequences:              sequences,
		lowestUnallocated:      lowestUnallocated,
		lowestOwnedUnallocated: lowestOwnedUnallocated,
		lowestUncommitted:      lowestUncommitted,
		outstandingReqs:        outstandingReqs,
	}
}

func (e *epoch) getSequence(seqNo uint64) (*sequence, int, error) {
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

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch []*pb.RequestAck) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	bucketID := e.seqToBucket(seqNo)

	if offset != e.lowestUnallocated[int(bucketID)] {
		panic(fmt.Sprintf("dev test, this really shouldn't happen: offset=%d e.lowestUnallocated=%d\n", offset, e.lowestUnallocated[int(bucketID)]))
	}

	e.lowestUnallocated[int(bucketID)] += len(e.buckets)

	// Note, this allocates the sequence inside, as we need to track
	// outstanidng requests before transitioning the sequence to preprepared
	actions, err := e.outstandingReqs.applyAcks(bucketID, seq, batch)
	if err != nil {
		panic(fmt.Sprintf("handle me, we need to stop the bucket and suspect: %s", err))
	}

	return actions
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
			QEntry:      e.sequences[e.lowestUncommitted].qEntry,
			EpochConfig: e.epochConfig,
		})

		e.lowestUncommitted++
	}

	return actions
}

func (e *epoch) moveWatermarks(seqNo uint64) *Actions {
	ci := int(e.networkConfig.CheckpointInterval)
	if seqNo+1 < e.lowWatermark()+uint64(ci) {
		return &Actions{}
	} else if seqNo+1 > e.lowWatermark()+uint64(ci) {
		panic(fmt.Sprintf("dev sanity test: expected seqNo=%d, got seqNo=%d", e.lowWatermark()+uint64(ci), seqNo+1))
	}

	e.sequences = e.sequences[ci:]
	e.lowestUncommitted -= ci
	for i := range e.lowestUnallocated {
		e.lowestUnallocated[i] -= ci
		e.lowestOwnedUnallocated[i] -= ci
	}

	if seqNo+3*uint64(ci)-1 == e.epochConfig.PlannedExpiration {
		e.ending = true
		return &Actions{}
	}

	for i := 0; i < ci; i++ {
		seqNo := seqNo + uint64(2*ci+i+1)
		epoch := e.epochConfig.Number
		owner := e.buckets[e.seqToBucket(seqNo)]
		e.sequences = append(e.sequences, newSequence(owner, epoch, seqNo, e.persisted, e.networkConfig, e.myConfig))
	}

	if e.highWatermark()-e.lowWatermark() != uint64(ci)*3-1 {
		panic(fmt.Sprintf("dev sanity test: low=%d high=%d width=%d", e.lowWatermark(), e.highWatermark(), ci*3))
	}

	return e.drainProposer()
}

func (e *epoch) drainProposer() *Actions {
	actions := &Actions{}

	for bucketID, ownerID := range e.buckets {
		if ownerID != NodeID(e.myConfig.ID) {
			continue
		}

		prb := e.proposer.proposalBucket(bucketID)

		for prb.hasPending() {
			i := e.lowestOwnedUnallocated[int(bucketID)]
			if i >= len(e.sequences) {
				break
			}
			seq := e.sequences[i]

			if len(e.sequences)-i <= int(e.networkConfig.CheckpointInterval) && !e.ending {
				// let the network move watermarks before filling up the last checkpoint
				// interval
				break
			}

			// TODO, roll this back into the proposer?
			proposals := prb.next()
			requestAcks := make([]*pb.RequestAck, len(proposals))
			forwardRequests := make([]*pb.ForwardRequest, len(proposals))
			for i, proposal := range proposals {
				requestAcks[i] = &pb.RequestAck{
					ClientId: proposal.data.ClientId,
					ReqNo:    proposal.data.ReqNo,
					Digest:   proposal.digest,
				}

				forwardRequests[i] = &pb.ForwardRequest{
					Request: proposal.data,
					Digest:  proposal.digest,
				}

				actions.Broadcast = append(actions.Broadcast, &pb.Msg{
					Type: &pb.Msg_ForwardRequest{
						ForwardRequest: forwardRequests[i],
					},
				})
			}

			actions.Broadcast = append(actions.Broadcast, &pb.Msg{
				Type: &pb.Msg_Preprepare{
					Preprepare: &pb.Preprepare{
						SeqNo: seq.seqNo,
						Epoch: seq.epoch,
						Batch: requestAcks,
					},
				},
			})

			// XXX Missing QEntry

			e.lowestOwnedUnallocated[int(bucketID)] += len(e.buckets)
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
	if e.lastCommittedAtTick < e.persisted.lastCommitted {
		e.lastCommittedAtTick = e.persisted.lastCommitted
		e.ticksSinceProgress = 0
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
							Epoch: e.epochConfig.Number,
						},
					},
				},
			},
		})
	}

	if e.myConfig.HeartbeatTicks == 0 || e.ticksSinceProgress%e.myConfig.HeartbeatTicks != 0 {
		return actions
	}

	for bucketID, index := range e.lowestOwnedUnallocated {
		if index >= len(e.sequences) {
			continue
		}

		if e.buckets[BucketID(bucketID)] != NodeID(e.myConfig.ID) {
			continue
		}

		if len(e.sequences)-index <= int(e.networkConfig.CheckpointInterval) && !e.ending {
			continue
		}

		prb := e.proposer.proposalBucket(BucketID(bucketID))

		var batch []*pb.RequestAck

		if prb.hasOutstanding() {
			// TODO, roll this back into the proposer?
			proposals := prb.next()
			batch = make([]*pb.RequestAck, len(proposals))
			for i, proposal := range proposals {
				batch[i] = &pb.RequestAck{
					ClientId: proposal.data.ClientId,
					ReqNo:    proposal.data.ReqNo,
					Digest:   proposal.digest,
				}

				actions.Broadcast = append(actions.Broadcast, &pb.Msg{
					Type: &pb.Msg_ForwardRequest{
						ForwardRequest: &pb.ForwardRequest{
							Request: proposal.data,
							Digest:  proposal.digest,
						},
					},
				})
			}
		}

		actions.Broadcast = append(actions.Broadcast, &pb.Msg{
			Type: &pb.Msg_Preprepare{
				Preprepare: &pb.Preprepare{
					SeqNo: e.sequences[index].seqNo,
					Epoch: e.sequences[index].epoch,
					Batch: batch,
				},
			},
		})

		// XXX Missing QEntry

		e.lowestOwnedUnallocated[int(bucketID)] += len(e.buckets)
	}

	return actions
}

func (e *epoch) lowWatermark() uint64 {
	return e.sequences[0].seqNo
}

func (e *epoch) highWatermark() uint64 {
	return e.sequences[len(e.sequences)-1].seqNo
}

func (e *epoch) status() []*BucketStatus {
	buckets := make([]*BucketStatus, len(e.buckets))
	for i := range buckets {
		bucket := &BucketStatus{
			ID:        uint64(i),
			Leader:    e.buckets[BucketID(i)] == NodeID(e.myConfig.ID),
			Sequences: make([]SequenceState, 0, len(e.sequences)/len(buckets)),
		}

		for j := i; j < len(e.sequences); j = j + len(buckets) {
			bucket.Sequences = append(bucket.Sequences, e.sequences[j].state)
		}

		buckets[i] = bucket
	}

	return buckets
}
