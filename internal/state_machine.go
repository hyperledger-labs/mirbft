/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
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
