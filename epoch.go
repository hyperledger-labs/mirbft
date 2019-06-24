/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

// epochConfig is the information required by the various
// state machines whose state is scoped to an epoch
type epochConfig struct {
	// myConfig is the configuration specific to this node
	myConfig *Config

	// oddities stores counts of suspicious acitivities and logs them.
	oddities *oddities

	// number is the epoch number this config applies to
	number uint64

	// highWatermark is the current maximum seqno that may be processed
	highWatermark SeqNo

	// lowWatermark is the current minimum seqno for which messages are valid
	lowWatermark SeqNo

	// F is the total number of faults tolerated by the network
	f int

	// CheckpointInterval is the number of sequence numbers to commit before broadcasting a checkpoint
	checkpointInterval SeqNo

	// nodes is all the node ids in the network
	nodes []NodeID

	// buckets is a map from bucket ID to leader ID
	buckets map[BucketID]NodeID
}

type epoch struct {
	epochConfig *epochConfig

	nodeMsgs map[NodeID]*nodeMsgs

	buckets map[BucketID]*bucket

	proposer *proposer

	checkpointWindows map[SeqNo]*checkpointWindow
}

func newEpoch(config *epochConfig) *epoch {
	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range config.nodes {
		nodeMsgs[id] = newNodeMsgs(id, config)
	}

	buckets := map[BucketID]*bucket{}
	for bucketID := range config.buckets {
		buckets[bucketID] = newBucket(config, bucketID)
	}

	checkpointWindows := map[SeqNo]*checkpointWindow{}
	for seqNo := config.lowWatermark + config.checkpointInterval; seqNo <= config.highWatermark; seqNo += config.checkpointInterval {
		checkpointWindows[seqNo] = newCheckpointWindow(seqNo, config)
	}

	return &epoch{
		epochConfig:       config,
		nodeMsgs:          nodeMsgs,
		buckets:           buckets,
		checkpointWindows: checkpointWindows,
		proposer:          newProposer(config),
	}
}

func (e *epoch) validateMsg(
	source NodeID,
	seqNo SeqNo,
	bucket BucketID,
	msgType string,
	inspect func(node *nodeMsgs) applyable,
	apply func(node *nodeMsgs) *Actions,
) *Actions {
	if bucket > BucketID(len(e.epochConfig.buckets)) {
		e.epochConfig.oddities.badBucket(e.epochConfig, msgType, source, seqNo, bucket)
		return &Actions{}
	}

	if seqNo < e.epochConfig.lowWatermark {
		e.epochConfig.oddities.belowWatermarks(e.epochConfig, msgType, source, seqNo, bucket)
		return &Actions{}
	}

	if seqNo > e.epochConfig.highWatermark {
		e.epochConfig.oddities.aboveWatermarks(e.epochConfig, msgType, source, seqNo, bucket)
		return &Actions{}
	}

	node, ok := e.nodeMsgs[source]
	if !ok {
		e.epochConfig.myConfig.Logger.Panic("unknown node")
		// TODO perhaps handle this a bit more gracefully? We should never get a message for a node
		// not defined in this epoch, but at the time of this writing, it's not clear whether
		// a subtle bug could cause this or whether this is an obvious panic situation
	}

	switch inspect(node) {
	case Past:
		e.epochConfig.oddities.AlreadyProcessed(e.epochConfig, msgType, source, seqNo, bucket)
	case Future:
		e.epochConfig.myConfig.Logger.Debug("deferring apply as it's from the future", zap.Uint64("NodeID", uint64(source)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		// TODO handle this with some sort of 'unprocessed' cache, but ignoring for now
	case Current:
		e.epochConfig.myConfig.Logger.Debug("applying", zap.Uint64("NodeID", uint64(source)), zap.Uint64("bucket", uint64(bucket)), zap.Uint64("SeqNo", uint64(seqNo)))
		return apply(node)
	default: // Invalid
		e.epochConfig.oddities.InvalidMessage(e.epochConfig, msgType, source, seqNo, bucket)
	}
	return &Actions{}
}

func (e *epoch) process(preprocessResult PreprocessResult) *Actions {
	bucketID := BucketID(preprocessResult.Cup % uint64(len(e.epochConfig.buckets)))
	nodeID := e.epochConfig.buckets[bucketID]
	if nodeID == NodeID(e.epochConfig.myConfig.ID) {
		return e.proposer.propose(preprocessResult.Proposal.Data)
	}

	if preprocessResult.Proposal.Source == e.epochConfig.myConfig.ID {
		// I originated this proposal, but someone else leads this bucket,
		// forward the message to them
		return &Actions{
			Unicast: []Unicast{
				{
					Target: uint64(nodeID),
					Msg: &pb.Msg{
						Type: &pb.Msg_Forward{
							Forward: &pb.Forward{
								Epoch:  e.epochConfig.number,
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

func (e *epoch) Preprepare(source NodeID, seqNo SeqNo, bucket BucketID, batch [][]byte) *Actions {
	return e.validateMsg(
		source, seqNo, bucket, "Preprepare",
		func(node *nodeMsgs) applyable { return node.inspectPreprepare(seqNo, bucket) },
		func(node *nodeMsgs) *Actions {
			actions := &Actions{}
			newLargest := node.applyPreprepare(seqNo, bucket)
			if newLargest && node.largestPreprepare >= e.proposer.nextAssigned+e.epochConfig.checkpointInterval {
				// XXX this is really a kind of heuristic check, to make sure
				// that if the network is advancing without us, possibly because
				// of unbalanced buckets, that we keep up, it's worth formalizing.
				nodesFurtherThanMe := 0
				for _, node := range e.nodeMsgs {
					if node.leadsSomeBucket && node.largestPreprepare >= e.proposer.nextAssigned {
						nodesFurtherThanMe++
					}
				}

				if nodesFurtherThanMe > e.epochConfig.f {
					actions.Append(e.proposer.noopAdvance())
				}
			}
			actions.Append(e.buckets[bucket].applyPreprepare(seqNo, batch))
			return actions
		},
	)
}

func (e *epoch) Prepare(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return e.validateMsg(
		source, seqNo, bucket, "Prepare",
		func(node *nodeMsgs) applyable { return node.inspectPrepare(seqNo, bucket) },
		func(node *nodeMsgs) *Actions {
			node.applyPrepare(seqNo, bucket)
			return e.buckets[bucket].applyPrepare(source, seqNo, digest)
		},
	)
}

func (e *epoch) Commit(source NodeID, seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return e.validateMsg(
		source, seqNo, bucket, "Commit",
		func(node *nodeMsgs) applyable { return node.inspectCommit(seqNo, bucket) },
		func(node *nodeMsgs) *Actions {
			node.applyCommit(seqNo, bucket)
			actions := e.buckets[bucket].applyCommit(source, seqNo, digest)
			if len(actions.Commit) > 0 {
				// XXX this is a moderately hacky way to determine if this commit msg triggered
				// a commit, is there a better way?
				if checkpointWindow, ok := e.checkpointWindows[seqNo]; ok {
					actions.Append(checkpointWindow.Committed(bucket))
				}
			}
			return actions
		},
	)
}

func (e *epoch) Checkpoint(source NodeID, seqNo SeqNo, value, attestation []byte) *Actions {
	return e.validateMsg(
		source, seqNo, 0, "Checkpoint", // XXX using bucket '0' for checkpoints is a bit of a hack, as it has no bucket
		func(node *nodeMsgs) applyable { return node.inspectCheckpoint(seqNo) },
		func(node *nodeMsgs) *Actions {
			node.applyCheckpoint(seqNo)
			cw := e.checkpointWindows[seqNo]
			actions := cw.applyCheckpointMsg(source, value, attestation)
			if cw.garbageCollectible {
				checkpointWindows := []*checkpointWindow{}
				for seqNo := e.epochConfig.lowWatermark + e.epochConfig.checkpointInterval; seqNo <= e.epochConfig.highWatermark; seqNo += e.epochConfig.checkpointInterval {
					checkpointWindow := e.checkpointWindows[seqNo]
					if !checkpointWindow.garbageCollectible {
						break
					}
					checkpointWindows = append(checkpointWindows, checkpointWindow)
					// XXX, the constant '4' garbage checkpoints is tied to the constant '5' free checkpoints in
					// bucket.go and assumes the network is configured for 10 total checkpoints, but not enforced.
					// Also, if there are at least 2 checkpoints, and the first one is obsolete (meaning all
					// nodes have acknowledged it, not simply a quorum), garbage collect it.
					if len(checkpointWindows) > 4 || (len(checkpointWindows) > 2 && checkpointWindows[0].obsolete) {
						newLowWatermark := e.epochConfig.lowWatermark + e.epochConfig.checkpointInterval
						newHighWatermark := e.epochConfig.highWatermark + e.epochConfig.checkpointInterval
						actions.Append(e.moveWatermarks(newLowWatermark, newHighWatermark))
						checkpointWindows = checkpointWindows[1:]
					}
				}
			}
			return actions
		},
	)
}

func (e *epoch) moveWatermarks(low, high SeqNo) *Actions {
	originalLowWatermark := e.epochConfig.lowWatermark
	originalHighWatermark := e.epochConfig.highWatermark
	e.epochConfig.lowWatermark = low
	e.epochConfig.highWatermark = high

	for _, bucket := range e.buckets {
		bucket.moveWatermarks()
	}

	for _, node := range e.nodeMsgs {
		node.moveWatermarks()
	}

	for seqNo := originalLowWatermark; seqNo < low && seqNo <= originalHighWatermark; seqNo += e.epochConfig.checkpointInterval {
		delete(e.checkpointWindows, seqNo)
	}

	for seqNo := low; seqNo <= high; seqNo += e.epochConfig.checkpointInterval {
		if seqNo < originalHighWatermark {
			continue
		}
		e.checkpointWindows[seqNo] = newCheckpointWindow(seqNo, e.epochConfig)
	}

	return e.proposer.drainQueue()
}

func (e *epoch) checkpointResult(seqNo SeqNo, value, attestation []byte) *Actions {
	checkpointWindow, ok := e.checkpointWindows[seqNo]
	if !ok {
		panic("received an unexpected checkpoint result")
	}
	return checkpointWindow.applyCheckpointResult(value, attestation)
}

func (e *epoch) digest(seqNo SeqNo, bucket BucketID, digest []byte) *Actions {
	return e.validateMsg(
		NodeID(e.epochConfig.myConfig.ID), seqNo, bucket, "Digest",
		func(node *nodeMsgs) applyable { return Current },
		func(node *nodeMsgs) *Actions {
			return e.buckets[bucket].applyDigestResult(seqNo, digest)
		},
	)
}

func (e *epoch) validate(seqNo SeqNo, bucket BucketID, valid bool) *Actions {
	return e.validateMsg(
		NodeID(e.epochConfig.myConfig.ID), seqNo, bucket, "Validate",
		func(node *nodeMsgs) applyable { return Current },
		func(node *nodeMsgs) *Actions {
			return e.buckets[bucket].applyValidateResult(seqNo, valid)
		},
	)
}

func (e *epoch) Tick() *Actions {
	return e.proposer.noopAdvance()
}
