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
	myConfig *Config
	nodeMsgs map[NodeID]*nodeMsgs

	activeEpoch *epoch
	epochs      []*epoch

	latestSuspicion   map[NodeID]*pb.Suspect
	latestEpochChange map[NodeID]*pb.EpochChange
}

func newStateMachine(config *epochConfig) *stateMachine {
	oddities := &oddities{
		logger: config.myConfig.Logger,
	}
	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range config.nodes {
		nodeMsgs[id] = newNodeMsgs(id, config, oddities)
	}

	activeEpoch := newEpoch(0, []byte("TODO, get from state"), config, []*checkpointWindow{})

	return &stateMachine{
		myConfig:          config.myConfig,
		activeEpoch:       activeEpoch,
		epochs:            []*epoch{activeEpoch},
		nodeMsgs:          nodeMsgs,
		latestSuspicion:   map[NodeID]*pb.Suspect{},
		latestEpochChange: map[NodeID]*pb.EpochChange{},
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

	nodeMsgs.ingest(outerMsg)

	actions := &Actions{}

	for {
		msg := nodeMsgs.next()
		if msg == nil {
			break
		}

		switch innerMsg := msg.Type.(type) {
		case *pb.Msg_Preprepare:
			actions.Append(sm.activeEpoch.applyPreprepareMsg(source, innerMsg.Preprepare))
		case *pb.Msg_Prepare:
			actions.Append(sm.activeEpoch.applyPrepareMsg(source, innerMsg.Prepare))
		case *pb.Msg_Commit:
			actions.Append(sm.activeEpoch.applyCommitMsg(source, innerMsg.Commit))
		case *pb.Msg_Checkpoint:
			msg := innerMsg.Checkpoint
			actions.Append(sm.checkpointMsg(source, SeqNo(msg.SeqNo), msg.Value))
		case *pb.Msg_Forward:
			msg := innerMsg.Forward
			// TODO should we have a separate validate step here?  How do we prevent
			// forwarded messages with bad data from poisoning our batch?
			actions.Append(&Actions{
				Preprocess: []Proposal{
					{
						Source: uint64(source),
						Data:   msg.Data,
					},
				},
			})
		case *pb.Msg_Suspect:
			sm.suspectMsg(source, innerMsg.Suspect)
		case *pb.Msg_EpochChange:
			sm.epochChangeMsg(source, innerMsg.EpochChange)
		case *pb.Msg_NewEpoch:
			// TODO, handle this
		default:
			// This should be unreachable, as the nodeMsgs filters based on type as well
			panic("unexpected bad message type, should have been detected earlier")
		}
	}

	return actions
}

func (sm *stateMachine) suspectMsg(source NodeID, msg *pb.Suspect) {
	sm.latestSuspicion[source] = msg

	for _, ew := range sm.epochs {
		if ew.config.number == msg.Epoch {
			ew.suspicions[source] = struct{}{}
			return
		}
	}
}

func (sm *stateMachine) epochChangeMsg(source NodeID, msg *pb.EpochChange) {
	sm.latestEpochChange[source] = msg
}

func (sm *stateMachine) checkpointMsg(source NodeID, seqNo SeqNo, value []byte) *Actions {
	actions := &Actions{}
	for _, e := range sm.epochs {
		actions.Append(e.applyCheckpointMsg(source, seqNo, value))
	}
	return actions
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}
	for _, preprocessResult := range results.Preprocesses {
		// sm.myConfig.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.applyPreprocessResult(preprocessResult))
	}

	for _, digestResult := range results.Digests {
		for _, e := range sm.epochs {
			if e.config.number != digestResult.Entry.Epoch {
				continue
			}
			// sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
			seqNo := digestResult.Entry.SeqNo
			actions.Append(sm.activeEpoch.applyDigestResult(SeqNo(seqNo), BucketID(digestResult.Entry.BucketID), digestResult.Digest))
			break
		}
	}

	for _, validateResult := range results.Validations {
		for _, e := range sm.epochs {
			if e.config.number != validateResult.Entry.Epoch {
				continue
			}
			// sm.myConfig.Logger.Debug("applying validate result", zap.Int("index", i))
			seqNo := validateResult.Entry.SeqNo
			actions.Append(sm.activeEpoch.applyValidateResult(SeqNo(seqNo), BucketID(validateResult.Entry.BucketID), validateResult.Valid))
			break
		}
	}

	for _, checkpointResult := range results.Checkpoints {
		// sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.applyCheckpointResult(SeqNo(checkpointResult.SeqNo), checkpointResult.Value))
	}

	return actions
}

func (sm *stateMachine) applyCheckpointResult(seqNo SeqNo, value []byte) *Actions {
	actions := &Actions{}
	for _, e := range sm.epochs {
		actions.Append(e.applyCheckpointResult(seqNo, value))
	}
	return actions
}

func (sm *stateMachine) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
	for _, e := range sm.epochs {
		if e.state == done {
			continue
		}

		// TODO we should probably have some sophisticated logic here for graceful epoch
		// rotations.  We should prefer finalizing the old epoch and populating the new.
		// We should also handle the case that we are out of space in the current active
		// epoch.
		return e.applyPreprocessResult(preprocessResult)
	}

	panic("we should always have at least one epoch that's active or pending")
}

func (sm *stateMachine) tick() *Actions {
	actions := &Actions{}

	for _, e := range sm.epochs {
		actions.Append(e.tick())
	}

	return actions
}

func (sm *stateMachine) status() *Status {
	epochConfig := sm.epochs[0].config

	nodes := make([]*NodeStatus, len(epochConfig.nodes))
	for i, nodeID := range epochConfig.nodes {
		nodes[i] = sm.nodeMsgs[nodeID].status()
	}

	var buckets []*BucketStatus
	checkpoints := []*CheckpointStatus{}

	for _, cw := range sm.activeEpoch.checkpointWindows {
		checkpoints = append(checkpoints, cw.status())
		if buckets == nil {
			buckets = make([]*BucketStatus, len(cw.buckets))
			for i := BucketID(0); i < BucketID(len(sm.activeEpoch.checkpointWindows[0].buckets)); i++ {
				buckets[int(i)] = cw.buckets[i].status()
			}
		} else {
			for i := BucketID(0); i < BucketID(len(sm.activeEpoch.checkpointWindows[0].buckets)); i++ {
				buckets[int(i)].Sequences = append(buckets[int(i)].Sequences, cw.buckets[i].status().Sequences...)
			}
		}
	}

	lowWatermark := sm.activeEpoch.checkpointWindows[0].start
	highWatermark := sm.activeEpoch.checkpointWindows[len(sm.activeEpoch.checkpointWindows)-1].end

	return &Status{
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochNumber:   epochConfig.number,

		Buckets:     buckets,
		Checkpoints: checkpoints,
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

	if s.LowWatermark == s.HighWatermark {
		buffer.WriteString("Empty Watermarks\n")
		return buffer.String()
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
		if len(s.Checkpoints) > i {
			checkpoint := s.Checkpoints[i]
			if seqNo == SeqNo(checkpoint.SeqNo) {
				buffer.WriteString(fmt.Sprintf("|%d", checkpoint.PendingCommits))
				i++
				continue
			}
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Pending Commits\n")
	i = 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		if len(s.Checkpoints) > i {
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
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Status\n")

	hRule()
	buffer.WriteString("-\n")

	return buffer.String()
}
