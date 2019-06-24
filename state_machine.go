/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type StateMachine struct {
	Config       *Config
	CurrentEpoch *Epoch
}

func (sm *StateMachine) Propose(data []byte) *Actions {
	return &Actions{
		Preprocess: []Proposal{
			{
				Source: sm.Config.ID,
				Data:   data,
			},
		},
	}
}

func (sm *StateMachine) Step(source NodeID, outerMsg *pb.Msg) *Actions {
	switch innerMsg := outerMsg.Type.(type) {
	case *pb.Msg_Preprepare:
		msg := innerMsg.Preprepare
		// TODO check for nil and log oddity
		return sm.CurrentEpoch.Preprepare(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Batch)
	case *pb.Msg_Prepare:
		msg := innerMsg.Prepare
		// TODO check for nil and log oddity
		return sm.CurrentEpoch.Prepare(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
	case *pb.Msg_Commit:
		msg := innerMsg.Commit
		// TODO check for nil and log oddity
		return sm.CurrentEpoch.Commit(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest)
	case *pb.Msg_Checkpoint:
		msg := innerMsg.Checkpoint
		// TODO check for nil and log oddity
		return sm.CurrentEpoch.Checkpoint(source, SeqNo(msg.SeqNo), msg.Value, msg.Attestation)
	case *pb.Msg_Forward:
		msg := innerMsg.Forward
		// TODO check for nil and log oddity
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

func (sm *StateMachine) ProcessResults(results ActionResults) *Actions {
	actions := &Actions{}
	for i, preprocessResult := range results.Preprocesses {
		sm.Config.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.CurrentEpoch.Process(preprocessResult))
	}

	for i, digestResult := range results.Digests {
		sm.Config.Logger.Debug("applying digest result", zap.Int("index", i))
		actions.Append(sm.CurrentEpoch.Digest(SeqNo(digestResult.Entry.SeqNo), BucketID(digestResult.Entry.BucketID), digestResult.Digest))
	}

	for i, validateResult := range results.Validations {
		sm.Config.Logger.Debug("applying validate result", zap.Int("index", i))
		actions.Append(sm.CurrentEpoch.Validate(SeqNo(validateResult.Entry.SeqNo), BucketID(validateResult.Entry.BucketID), validateResult.Valid))
	}

	for i, checkpointResult := range results.Checkpoints {
		sm.Config.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.CurrentEpoch.CheckpointResult(SeqNo(checkpointResult.SeqNo), checkpointResult.Value, checkpointResult.Attestation))
	}

	return actions
}

func (sm *StateMachine) Tick() *Actions {
	return sm.CurrentEpoch.Tick()
}

func (sm *StateMachine) Status() *Status {
	epochConfig := sm.CurrentEpoch.EpochConfig

	nodes := make([]*NodeStatus, len(sm.CurrentEpoch.EpochConfig.Nodes))
	for i, nodeID := range epochConfig.Nodes {
		nodes[i] = sm.CurrentEpoch.NodeMsgs[nodeID].Status()
	}

	buckets := make([]*BucketStatus, len(sm.CurrentEpoch.Buckets))
	for i := BucketID(0); i < BucketID(len(sm.CurrentEpoch.Buckets)); i++ {
		buckets[int(i)] = sm.CurrentEpoch.Buckets[i].Status()
	}

	checkpoints := []*CheckpointStatus{}
	for seqNo := epochConfig.LowWatermark + epochConfig.CheckpointInterval; seqNo <= epochConfig.HighWatermark; seqNo += epochConfig.CheckpointInterval {
		checkpoints = append(checkpoints, sm.CurrentEpoch.CheckpointWindows[seqNo].Status())
	}

	return &Status{
		LowWatermark:  epochConfig.LowWatermark,
		HighWatermark: epochConfig.HighWatermark,
		EpochNumber:   epochConfig.Number,
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

func (s *Status) JSON() string {
	result, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(result)
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
			buffer.WriteString(fmt.Sprintf("| Bucket=%d (LocalLeader, Pending=%d)\n", bucketStatus.ID, bucketStatus.BatchesPending))
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
