/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"bytes"
	"fmt"
	"math"

	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type StateMachine struct {
	Config       *consumer.Config
	CurrentEpoch *Epoch
}

func (sm *StateMachine) Propose(data []byte) *consumer.Actions {
	return &consumer.Actions{
		Preprocess: []consumer.Proposal{
			{
				Source: sm.Config.ID,
				Data:   data,
			},
		},
	}
}

func (sm *StateMachine) Step(source NodeID, outerMsg *pb.Msg) *consumer.Actions {
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
		return &consumer.Actions{
			Preprocess: []consumer.Proposal{
				{
					Source: uint64(source),
					Data:   msg.Data,
				},
			},
		}
	default:
		// TODO mark oddity
		return &consumer.Actions{}
	}
}

func (sm *StateMachine) ProcessResults(results consumer.ActionResults) *consumer.Actions {
	actions := &consumer.Actions{}
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

func (sm *StateMachine) Status() *Status {
	nodes := map[NodeID]*NodeStatus{}
	for nodeID, node := range sm.CurrentEpoch.Nodes {
		nodes[nodeID] = node.Status()
	}

	buckets := map[BucketID]*BucketStatus{}
	for bucketID, bucket := range sm.CurrentEpoch.Buckets {
		buckets[bucketID] = bucket.Status()
	}

	checkpoints := map[SeqNo]*CheckpointStatus{}
	for seqNo, checkpointWindow := range sm.CurrentEpoch.CheckpointWindows {
		checkpoints[seqNo] = checkpointWindow.Status()
	}

	return &Status{
		LowWatermark:  sm.CurrentEpoch.EpochConfig.LowWatermark,
		HighWatermark: sm.CurrentEpoch.EpochConfig.HighWatermark,
		EpochNumber:   sm.CurrentEpoch.EpochConfig.Number,
		Nodes:         nodes,
		Buckets:       buckets,
		Checkpoints:   checkpoints,
	}
}

type Status struct {
	LowWatermark  SeqNo
	HighWatermark SeqNo
	EpochNumber   uint64
	Nodes         map[NodeID]*NodeStatus
	Buckets       map[BucketID]*BucketStatus
	Checkpoints   map[SeqNo]*CheckpointStatus
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("LowWatermark=%d, HighWatermark=%d, Epoch=%d\n\n", s.LowWatermark, s.HighWatermark, s.EpochNumber))

	for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
		magnitude := SeqNo(math.Pow10(i - 1))
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString(fmt.Sprintf(" %d", seqNo/magnitude%10))
		}
		buffer.WriteString("\n")
	}

	for node, nodeStatus := range s.Nodes {
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString("--")
		}

		buffer.WriteString(fmt.Sprintf("- === Node %d === \n", node))
		for bucket := BucketID(0); bucket < BucketID(len(s.Buckets)); bucket++ {
			messageStatus := nodeStatus.Messages[bucket]
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
				if seqNo == nodeStatus.LastCheckpoint {
					buffer.WriteString("|X")
					continue
				}

				if seqNo+1 == messageStatus.Commit {
					buffer.WriteString("|C")
					continue
				}

				if seqNo+1 == messageStatus.Prepare {
					if messageStatus.Leader {
						buffer.WriteString("|Q")
					} else {
						buffer.WriteString("|P")
					}
					continue
				}
				buffer.WriteString("| ")
			}

			buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucket))
		}
	}

	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		buffer.WriteString("--")
	}
	buffer.WriteString("- === Buckets ===\n")

	for bucketID := BucketID(0); bucketID < BucketID(len(s.Buckets)); bucketID++ {
		bucketStatus := s.Buckets[bucketID]
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			state := bucketStatus.Sequences[seqNo]
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
			buffer.WriteString(fmt.Sprintf("| Bucket=%d (Leader, Pending=%d)\n", bucketID, bucketStatus.BatchesPending))
		} else {
			buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucketID))
		}
	}

	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		buffer.WriteString("--")
	}
	buffer.WriteString("-\n")

	return buffer.String()
}
