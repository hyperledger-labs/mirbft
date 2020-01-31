/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import pb "github.com/IBM/mirbft/mirbftpb"

// epochConfig is the information required by the various
// state machines whose state is scoped to an epoch
type epochConfig struct {
	// myConfig is the configuration specific to this node
	myConfig *Config

	// number is the epoch number this config applies to
	number uint64

	// plannedExpiration is when this epoch ends, if it ends gracefully
	plannedExpiration SeqNo

	// F is the total number of faults tolerated by the network
	f int

	// CheckpointInterval is the number of sequence numbers to commit before broadcasting a checkpoint
	checkpointInterval SeqNo

	// nodes is all the node ids in the network
	nodes []NodeID

	// buckets is a map from bucket ID to leader ID
	buckets map[BucketID]NodeID
}

type epochState int

const (
	prepending epochState = iota
	pending
	active
	done
)

type epoch struct {
	// config contains the static components of the epoch
	config *epochConfig

	ticks uint64

	proposer *proposer

	state epochState

	newEpochPending bool

	inactiveTicks int

	changes map[NodeID]*pb.EpochChange

	suspicions map[NodeID]struct{}

	checkpointWindows []*checkpointWindow
}

func (e *epoch) checkpointWindowForSeqNo(seqNo SeqNo) *checkpointWindow {
	if e.config.plannedExpiration < seqNo {
		return nil
	}

	if e.checkpointWindows[0].start > seqNo {
		return nil
	}

	offset := seqNo - SeqNo(e.checkpointWindows[0].start)
	index := offset / SeqNo(e.config.checkpointInterval)
	if int(index) >= len(e.checkpointWindows) {
		return nil
	}
	return e.checkpointWindows[index]
}

func (e *epoch) applyPreprepareMsg(source NodeID, msg *pb.Preprepare) *Actions {
	if e.state == done {
		return &Actions{}
	}

	return e.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyPreprepareMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, msg *pb.Prepare) *Actions {
	if e.state == done {
		return &Actions{}
	}

	return e.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyPrepareMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
}

func (e *epoch) applyCommitMsg(source NodeID, msg *pb.Commit) *Actions {
	if e.state == done {
		return &Actions{}
	}

	return e.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyCommitMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
}

func (e *epoch) applyCheckpointMsg(source NodeID, seqNo SeqNo, value []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	lastCW := e.checkpointWindows[len(e.checkpointWindows)-1]

	if lastCW.epochConfig.plannedExpiration == lastCW.end {
		// This epoch is about to end gracefully, don't allocate new windows
		// so no need to go into allocation or garbage collection logic.
		return &Actions{}
	}

	cw := e.checkpointWindowForSeqNo(seqNo)

	secondToLastCW := e.checkpointWindows[len(e.checkpointWindows)-2]
	actions := cw.applyCheckpointMsg(source, value)

	if secondToLastCW.garbageCollectible {
		e.proposer.maxAssignable = lastCW.end
		e.checkpointWindows = append(
			e.checkpointWindows,
			newCheckpointWindow(
				lastCW.end+1,
				lastCW.end+e.config.checkpointInterval,
				e.config,
			),
		)
	}

	actions.Append(e.proposer.drainQueue())
	for len(e.checkpointWindows) > 2 && (e.checkpointWindows[0].obsolete || e.checkpointWindows[1].garbageCollectible) {
		e.checkpointWindows = e.checkpointWindows[1:]
	}

	return actions
}

func (e *epoch) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
	if e.state == done {
		return &Actions{}
	}

	bucketID := BucketID(preprocessResult.Cup % uint64(len(e.config.buckets)))
	nodeID := e.config.buckets[bucketID]
	if nodeID == NodeID(e.config.myConfig.ID) {
		return e.proposer.propose(preprocessResult.Proposal.Data)
	}

	if preprocessResult.Proposal.Source == e.config.myConfig.ID {
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

func (e *epoch) applyDigestResult(seqNo SeqNo, bucketID BucketID, digest []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	return e.checkpointWindowForSeqNo(seqNo).applyDigestResult(seqNo, bucketID, digest)
}

func (e *epoch) applyValidateResult(seqNo SeqNo, bucketID BucketID, valid bool) *Actions {
	if e.state == done {
		return &Actions{}
	}

	return e.checkpointWindowForSeqNo(seqNo).applyValidateResult(seqNo, bucketID, valid)
}

func (e *epoch) applyCheckpointResult(seqNo SeqNo, value []byte) *Actions {
	if e.state == done {
		return &Actions{}
	}

	cw := e.checkpointWindowForSeqNo(seqNo)
	if cw == nil {
		panic("received an unexpected checkpoint result")
	}
	return cw.applyCheckpointResult(value)
}

func (e *epoch) tick() *Actions {
	e.ticks++

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

func (e *epoch) tickPrepending() *Actions {
	newEpoch := e.constructNewEpoch() // TODO, recomputing over and over again isn't useful unless we've gotten new epoch change messages in the meantime, should we somehow store the last one we computed?

	if newEpoch == nil {
		return &Actions{}
	}

	e.newEpochPending = true

	if e.config.number%uint64(len(e.config.nodes)) == e.config.myConfig.ID {
		return &Actions{
			Broadcast: []*pb.Msg{
				// TODO return new epoch here
			},
		}
	}

	return &Actions{}
}

func (e *epoch) tickPending() *Actions {
	e.inactiveTicks++
	// TODO new view timeout
	return &Actions{}
}

func (e *epoch) tickNotDone() *Actions {
	// Wait for 2f+1 suspicions before initiating an epoch change,
	if len(e.suspicions) < 2*e.config.f+1 {
		return &Actions{}
	}

	e.state = done

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					// TODO, actually construct the message
					EpochChange: &pb.EpochChange{},
				},
			},
		},
	}

}

func (e *epoch) tickActive() *Actions {
	actions := &Actions{}
	if e.config.myConfig.HeartbeatTicks != 0 && e.ticks%uint64(e.config.myConfig.HeartbeatTicks) == 0 {
		actions.Append(e.proposer.noopAdvance())
	}

	for _, cw := range e.checkpointWindows {
		actions.Append(cw.tick())
	}
	return actions
}

func (e *epoch) constructEpochChange() *pb.EpochChange {
	epochChange := &pb.EpochChange{}
	for _, cw := range e.checkpointWindows {
		if cw.myValue == nil {
			// Checkpoints necessarily generated in order, no further checkpoints are ready
			break
		}
		epochChange.Checkpoints = append(epochChange.Checkpoints, &pb.EpochChange_Checkpoint{
			SeqNo: uint64(cw.end),
			Value: cw.myValue,
		})
	}
	return epochChange
}

func (e *epoch) constructNewEpoch() *pb.Msg {
	// XXX this should probably return a *pb.NewEpoch (which doesn't currently exist)
	return nil
}
