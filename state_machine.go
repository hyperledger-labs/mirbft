/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"
	"math"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type stateMachine struct {
	myConfig     *Config
	nodeMsgs     map[NodeID]*nodeMsgs
	currentEpoch *epoch

	checkpointWindows map[SeqNo]*checkpointWindow

	//checkpointWs should really be named checkpointWindows, but
	// trying to go incrementally and non-invasively.
	checkpointWs []*checkpointWindow
}

func newStateMachine(config *epochConfig) *stateMachine {
	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range config.nodes {
		nodeMsgs[id] = newNodeMsgs(id, config)
	}

	checkpointWindows := map[SeqNo]*checkpointWindow{}
	checkpointWs := []*checkpointWindow{}
	for seqNo := config.lowWatermark + config.checkpointInterval; seqNo <= config.highWatermark; seqNo += config.checkpointInterval {
		cw := newCheckpointWindow(seqNo, config)
		checkpointWindows[seqNo] = cw
		checkpointWs = append(checkpointWs, cw)
	}

	return &stateMachine{
		myConfig:          config.myConfig,
		currentEpoch:      newEpoch(config),
		nodeMsgs:          nodeMsgs,
		checkpointWindows: checkpointWindows,
		checkpointWs:      checkpointWs,
	}
}

func (sm *stateMachine) propose(data []byte) *Actions {
	return &Actions{
		Preprocess: []Proposal{
			{
				Source: sm.myConfig.ID,
				Data:   data,
			},
		},
	}
}

func (sm *stateMachine) step(source NodeID, outerMsg *pb.Msg) *Actions {
	nodeMsgs, ok := sm.nodeMsgs[source]
	if !ok {
		sm.myConfig.Logger.Panic("received a message from a node ID that does not exist", zap.Int("source", int(source)))
	}
	msgs := nodeMsgs.processMsg(outerMsg)

	for _, msg := range msgs {
		switch innerMsg := msg.Type.(type) {
		case *pb.Msg_Preprepare:
			msg := innerMsg.Preprepare
			// TODO check for nil and log oddity
			return sm.currentEpoch.preprepare(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Batch)
		case *pb.Msg_Prepare:
			msg := innerMsg.Prepare
			// TODO check for nil and log oddity
			return sm.currentEpoch.prepare(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
		case *pb.Msg_Commit:
			msg := innerMsg.Commit
			// TODO check for nil and log oddity
			actions := sm.currentEpoch.commit(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
			if len(actions.Commit) > 0 {
				// XXX this is a moderately hacky way to determine if this commit msg triggered
				// a commit, is there a better way?
				if checkpointWindow, ok := sm.checkpointWindows[SeqNo(msg.SeqNo)]; ok {
					actions.Append(checkpointWindow.Committed(BucketID(msg.Bucket)))
				}
			}
			return actions
		case *pb.Msg_Checkpoint:
			msg := innerMsg.Checkpoint
			// TODO check for nil and log oddity
			return sm.checkpointMsg(source, SeqNo(msg.SeqNo), msg.Value, msg.Attestation)
		case *pb.Msg_Forward:
			msg := innerMsg.Forward
			// TODO check for nil and log oddity
			// TODO should we have a separate validate step here?  How do we prevent
			// forwarded messages with bad data from poisoning our batch?
			return &Actions{
				Preprocess: []Proposal{
					{
						Source: uint64(source),
						Data:   msg.Data,
					},
				},
			}
		default:
			// TODO mark oddity
			return &Actions{}
		}
	}

	return &Actions{}
}

func (sm *stateMachine) checkpointMsg(source NodeID, seqNo SeqNo, value, attestation []byte) *Actions {
	e := sm.currentEpoch

	cw := sm.checkpointWindows[seqNo]

	actions := cw.applyCheckpointMsg(source, value, attestation)
	if !cw.garbageCollectible {
		return actions
	}

	garbageCollectible := []*checkpointWindow{}
	for _, cw := range sm.checkpointWs {
		if !cw.garbageCollectible {
			break
		}
		garbageCollectible = append(garbageCollectible, cw)
	}

	for {
		// XXX, the constant '4' garbage checkpoints is tied to the constant '5' free checkpoints in
		// bucket.go and assumes the network is configured for 10 total checkpoints, but not enforced.
		// Also, if there are at least 2 checkpoints, and the first one is obsolete (meaning all
		// nodes have acknowledged it, not simply a quorum), garbage collect it.
		if len(garbageCollectible) > 4 || (len(garbageCollectible) > 2 && garbageCollectible[0].obsolete) {
			newLowWatermark := e.epochConfig.lowWatermark + e.epochConfig.checkpointInterval
			newHighWatermark := e.epochConfig.highWatermark + e.epochConfig.checkpointInterval
			actions.Append(sm.moveWatermarks(newLowWatermark, newHighWatermark))
			garbageCollectible = garbageCollectible[1:]
			continue
		}

		break
	}

	seqnos := []uint64{}
	for _, gc := range garbageCollectible {
		seqnos = append(seqnos, uint64(gc.number))
	}

	return actions
}

func (sm *stateMachine) moveWatermarks(low, high SeqNo) *Actions {
	e := sm.currentEpoch

	originalLowWatermark := e.epochConfig.lowWatermark
	originalHighWatermark := e.epochConfig.highWatermark
	e.epochConfig.lowWatermark = low
	e.epochConfig.highWatermark = high

	for seqNo := originalLowWatermark; seqNo < low && seqNo <= originalHighWatermark; seqNo += e.epochConfig.checkpointInterval {
		delete(sm.checkpointWindows, seqNo)
	}

	for len(sm.checkpointWs) > 0 {
		cw := sm.checkpointWs[0]
		if cw.number < low {
			sm.checkpointWs = sm.checkpointWs[1:]
			continue
		}
		break
	}

	for seqNo := low; seqNo <= high; seqNo += e.epochConfig.checkpointInterval {
		if seqNo <= originalHighWatermark {
			continue
		}
		cw := newCheckpointWindow(seqNo, e.epochConfig)
		sm.checkpointWindows[seqNo] = cw
		sm.checkpointWs = append(sm.checkpointWs, cw)

	}

	for _, node := range sm.nodeMsgs {
		node.moveWatermarks()
	}

	return e.moveWatermarks()
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}
	for i, preprocessResult := range results.Preprocesses {
		sm.myConfig.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.currentEpoch.process(preprocessResult))
	}

	for i, digestResult := range results.Digests {
		sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
		actions.Append(sm.currentEpoch.digest(SeqNo(digestResult.Entry.SeqNo), BucketID(digestResult.Entry.BucketID), digestResult.Digest))
	}

	for i, validateResult := range results.Validations {
		sm.myConfig.Logger.Debug("applying validate result", zap.Int("index", i))
		actions.Append(sm.currentEpoch.validate(SeqNo(validateResult.Entry.SeqNo), BucketID(validateResult.Entry.BucketID), validateResult.Valid))
	}

	for i, checkpointResult := range results.Checkpoints {
		sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.checkpointResult(SeqNo(checkpointResult.SeqNo), checkpointResult.Value, checkpointResult.Attestation))
	}

	return actions
}

func (sm *stateMachine) checkpointResult(seqNo SeqNo, value, attestation []byte) *Actions {
	checkpointWindow, ok := sm.checkpointWindows[seqNo]
	if !ok {
		panic("received an unexpected checkpoint result")
	}
	return checkpointWindow.applyCheckpointResult(value, attestation)
}

func (sm *stateMachine) tick() *Actions {
	return sm.currentEpoch.tick()
}

func (sm *stateMachine) status() *Status {
	epochConfig := sm.currentEpoch.epochConfig

	nodes := make([]*NodeStatus, len(sm.currentEpoch.epochConfig.nodes))
	for i, nodeID := range epochConfig.nodes {
		nodes[i] = sm.nodeMsgs[nodeID].status()
	}

	buckets := make([]*BucketStatus, len(sm.currentEpoch.buckets))
	for i := BucketID(0); i < BucketID(len(sm.currentEpoch.buckets)); i++ {
		buckets[int(i)] = sm.currentEpoch.buckets[i].status()
	}

	checkpoints := []*CheckpointStatus{}
	for seqNo := epochConfig.lowWatermark + epochConfig.checkpointInterval; seqNo <= epochConfig.highWatermark; seqNo += epochConfig.checkpointInterval {
		checkpoints = append(checkpoints, sm.checkpointWindows[seqNo].status())
	}

	return &Status{
		LowWatermark:  epochConfig.lowWatermark,
		HighWatermark: epochConfig.highWatermark,
		EpochNumber:   epochConfig.number,
		Nodes:         nodes,
		Buckets:       buckets,
		Checkpoints:   checkpoints,
	}
}

type Status struct {
	LowWatermark  SeqNo
	HighWatermark SeqNo
	EpochNumber   uint64
	Nodes         []*NodeStatus
	Buckets       []*BucketStatus
	Checkpoints   []*CheckpointStatus
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("LowWatermark=%d, HighWatermark=%d, Epoch=%d\n\n", s.LowWatermark, s.HighWatermark, s.EpochNumber))

	hRule := func() {
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString("--")
		}
	}

	for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
		magnitude := SeqNo(math.Pow10(i - 1))
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString(fmt.Sprintf(" %d", seqNo/magnitude%10))
		}
		buffer.WriteString("\n")
	}

	for _, nodeStatus := range s.Nodes {

		fmt.Printf("%+v\n\n\n", nodeStatus)

		hRule()
		buffer.WriteString(fmt.Sprintf("- === Node %d === \n", nodeStatus.ID))
		for bucket, bucketStatus := range nodeStatus.BucketStatuses {
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
				if seqNo == SeqNo(bucketStatus.LastCheckpoint) {
					buffer.WriteString("|X")
					continue
				}

				if seqNo == SeqNo(bucketStatus.LastCommit) {
					buffer.WriteString("|C")
					continue
				}

				if seqNo == SeqNo(bucketStatus.LastPrepare) {
					if bucketStatus.IsLeader {
						buffer.WriteString("|Q")
					} else {
						buffer.WriteString("|P")
					}
					continue
				}
				buffer.WriteString("| ")
			}

			if bucketStatus.IsLeader {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d (Leader)\n", bucket))
			} else {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucket))
			}
		}
	}

	hRule()
	buffer.WriteString("- === Buckets ===\n")

	for _, bucketStatus := range s.Buckets {
		for _, state := range bucketStatus.Sequences {
			switch state {
			case Uninitialized:
				buffer.WriteString("| ")
			case Preprepared:
				buffer.WriteString("|Q")
			case Digested:
				buffer.WriteString("|D")
			case InvalidBatch:
				buffer.WriteString("|I")
			case Validated:
				buffer.WriteString("|V")
			case Prepared:
				buffer.WriteString("|P")
			case Committed:
				buffer.WriteString("|C")
			}
		}
		if bucketStatus.Leader {
			buffer.WriteString(fmt.Sprintf("| Bucket=%d (LocalLeader)\n", bucketStatus.ID))
		} else {
			buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucketStatus.ID))
		}
	}

	hRule()
	buffer.WriteString("- === Checkpoints ===\n")
	i := 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		checkpoint := s.Checkpoints[i]
		if seqNo == SeqNo(checkpoint.SeqNo) {
			buffer.WriteString(fmt.Sprintf("|%d", checkpoint.PendingCommits))
			i++
			continue
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Pending Commits\n")
	i = 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		checkpoint := s.Checkpoints[i]
		if seqNo == SeqNo(s.Checkpoints[i].SeqNo) {
			switch {
			case checkpoint.NetQuorum && !checkpoint.LocalAgreement:
				buffer.WriteString("|N")
			case checkpoint.NetQuorum && checkpoint.LocalAgreement:
				buffer.WriteString("|G")
			default:
				buffer.WriteString("|P")
			}
			i++
			continue
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Status\n")

	hRule()
	buffer.WriteString("-\n")

	return buffer.String()
}
