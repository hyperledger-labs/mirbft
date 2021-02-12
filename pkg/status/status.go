/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package status

import (
	"bytes"
	"fmt"
	"math"
)

type EpochTargetState int

const (
	// EpochPrepending indicates we have sent an epoch-change, but waiting for a quorum
	EpochPrepending = iota

	// EpochPending indicates that we have a quorum of epoch-change messages, waits on new-epoch
	EpochPending

	// EpochVerifying indicates we have received a new view message but it references epoch changes we cannot yet verify
	EpochVerifying

	// EpochFetching indicates we have received and verified a new epoch messages, and are waiting to get state
	EpochFetching

	// EpochEchoing indicates we have received and validated a new-epoch, waiting for a quorum of echos
	EpochEchoing

	// EpochReadying indicates we have received a quorum of echos, waiting a on qourum of readies
	EpochReadying

	// EpochReady indicates the new epoch is ready to begin
	EpochReady

	// EpochInProgress indicates the epoch is currently active
	EpochInProgress

	// EpochDone indicates this epoch has ended, either gracefully or because we sent an epoch change
	EpochDone
)

type SequenceState int

const (
	// SequenceUnitialized indicates no batch has been assigned to this sequence.
	SequenceUninitialized SequenceState = iota

	// SequenceAllocated indicates that a potentially valid batch has been assigned to this sequence.
	SequenceAllocated

	// SequencePendingRequests indicates that we are waiting for missing requests to arrive or be validated.
	SequencePendingRequests

	// SequenceReady indicates that we have all requests and are ready to proceed with the 3-phase commit.
	SequenceReady

	// SequencePreprepared indicates that we have sent a Prepare/Preprepare as follow/leader respectively.
	SequencePreprepared

	// SequencePreprepared indicates that we have sent a Commit message.
	SequencePrepared

	// SequenceCommitted indicates that we have a quorum of commit messages and the sequence is
	// eligible to commit.  Note though, that all prior sequences must commit prior to the consumer
	// seeing this commit event.
	SequenceCommitted
)

type StateMachine struct {
	NodeID        uint64           `json:"node_id"`
	LowWatermark  uint64           `json:"low_watermark"`
	HighWatermark uint64           `json:"high_watermark"`
	EpochTracker  *EpochTracker    `json:"epoch_tracker"`
	NodeBuffers   []*NodeBuffer    `json:"node_buffers"`
	Buckets       []*Bucket        `json:"buckets"`
	Checkpoints   []*Checkpoint    `json:"checkpoints"`
	ClientWindows []*ClientTracker `json:"client_tracker"`
}

type Bucket struct {
	ID        uint64          `json:"id"`
	Leader    bool            `json:"leader"`
	Sequences []SequenceState `json:"sequences"`
}

type Checkpoint struct {
	SeqNo         uint64 `json:"seq_no"`
	MaxAgreements int    `json:"max_agreements"`
	NetQuorum     bool   `json:"net_quorum"`
	LocalDecision bool   `json:"local_decision"`
}

type EpochTracker struct {
	State           EpochTargetState `json:"state"` // TODO, move into epoch target
	LastActiveEpoch uint64           `json:"last_active_epoch"`
	EpochTargets    []*EpochTarget   `json:"epoch_targets"`
}

type EpochTarget struct {
	Number       uint64         `json:"number"`
	EpochChanges []*EpochChange `json:"epoch_changes"`
	Echos        []uint64       `json:"echos"`
	Readies      []uint64       `json:"readies"`
	Suspicions   []uint64       `json:"suspicions"`
}

type EpochChange struct {
	Source uint64            `json:"source"`
	Msgs   []*EpochChangeMsg `json:"messages"`
}

type EpochChangeMsg struct {
	Digest []byte   `json:"digest"`
	Acks   []uint64 `json:"acks"`
}

type NodeBuffer struct {
	ID             uint64       `json:"id"`
	Buckets        []NodeBucket `json:"buckets"`
	LastCheckpoint uint64       `json:"last_checkpoint"`
}

type NodeBucket struct {
	BucketID    int    `json:"bucket_id"`
	IsLeader    bool   `json:"is_leader"`
	LastPrepare uint64 `json:"last_prepare"`
	LastCommit  uint64 `json:"last_commit"`
}

type ClientTracker struct {
	ClientID      uint64   `json:"client_id"`
	LowWatermark  uint64   `json:"low_watermark"`
	HighWatermark uint64   `json:"high_watermark"`
	Allocated     []uint64 `json:"allocated"`
}

func (s *StateMachine) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString("===========================================\n")
	buffer.WriteString(fmt.Sprintf("NodeID=%d, LowWatermark=%d, HighWatermark=%d, Epoch=%d\n", s.NodeID, s.LowWatermark, s.HighWatermark, s.EpochTracker.LastActiveEpoch))
	buffer.WriteString("===========================================\n\n")

	buffer.WriteString("=== Epoch Changer ===\n")
	buffer.WriteString(fmt.Sprintf("Change is in state: %d, last active epoch %d\n", s.EpochTracker.State, s.EpochTracker.LastActiveEpoch))
	for _, et := range s.EpochTracker.EpochTargets {
		buffer.WriteString(fmt.Sprintf("Target Epoch %d:\n", et.Number))
		buffer.WriteString("  EpochChanges:\n")
		for _, ec := range et.EpochChanges {
			for _, ecm := range ec.Msgs {
				buffer.WriteString(fmt.Sprintf("    Source=%d Digest=%.4x Acks=%v\n", ec.Source, ecm.Digest, ecm.Acks))
			}
		}
		buffer.WriteString(fmt.Sprintf("  Echos: %v\n", et.Echos))
		buffer.WriteString(fmt.Sprintf("  Readies: %v\n", et.Readies))
		buffer.WriteString(fmt.Sprintf("  Suspicions: %v\n", et.Suspicions))
	}
	buffer.WriteString("\n")
	buffer.WriteString("=====================\n")
	buffer.WriteString("\n")

	hRule := func() {
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
			buffer.WriteString("--")
		}
	}

	if s.LowWatermark == s.HighWatermark {
		buffer.WriteString("=== Empty Watermarks ===\n")
	} else {
		if s.HighWatermark-s.LowWatermark > 10000 {
			buffer.WriteString(fmt.Sprintf("=== Suspiciously wide watermarks [%d, %d] ===\n", s.LowWatermark, s.HighWatermark))
			return buffer.String()
		}

		for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
			magnitude := math.Pow10(i - 1)
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
				buffer.WriteString(fmt.Sprintf(" %d", seqNo/uint64(magnitude)%10))
			}
			buffer.WriteString("\n")
		}

		for _, nodeBuffer := range s.NodeBuffers {
			hRule()
			buffer.WriteString(fmt.Sprintf("- === Node %d === \n", nodeBuffer.ID))
			for bucket, bucketStatus := range nodeBuffer.Buckets {
				for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
					if seqNo == nodeBuffer.LastCheckpoint {
						buffer.WriteString("|X")
						continue
					}

					if seqNo == bucketStatus.LastCommit {
						buffer.WriteString("|C")
						continue
					}

					if seqNo == bucketStatus.LastPrepare {
						buffer.WriteString("|P")
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

		for _, bucketBuffer := range s.Buckets {
			for _, state := range bucketBuffer.Sequences {
				switch state {
				case SequenceUninitialized:
					buffer.WriteString("| ")
				case SequenceAllocated:
					buffer.WriteString("|A")
				case SequencePendingRequests:
					buffer.WriteString("|F")
				case SequenceReady:
					buffer.WriteString("|R")
				case SequencePreprepared:
					buffer.WriteString("|Q")
				case SequencePrepared:
					buffer.WriteString("|P")
				case SequenceCommitted:
					buffer.WriteString("|C")
				}
			}
			if bucketBuffer.Leader {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d (LocalLeader)\n", bucketBuffer.ID))
			} else {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucketBuffer.ID))
			}
		}

		hRule()
		buffer.WriteString("- === Checkpoints ===\n")
		i := 0
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
			if len(s.Checkpoints) > i {
				checkpoint := s.Checkpoints[i]
				if seqNo == checkpoint.SeqNo {
					buffer.WriteString(fmt.Sprintf("|%d", checkpoint.MaxAgreements))
					i++
					continue
				}
			}
			buffer.WriteString("| ")
		}
		buffer.WriteString("| Max Agreements\n")
		i = 0
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
			if len(s.Checkpoints) > i {
				checkpoint := s.Checkpoints[i]
				if seqNo == s.Checkpoints[i].SeqNo/uint64(len(s.Buckets)) {
					switch {
					case checkpoint.NetQuorum && !checkpoint.LocalDecision:
						buffer.WriteString("|N")
					case checkpoint.NetQuorum && checkpoint.LocalDecision:
						buffer.WriteString("|G")
					case !checkpoint.NetQuorum && checkpoint.LocalDecision:
						buffer.WriteString("|M")
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
	}

	hRule()
	buffer.WriteString("-\n")

	buffer.WriteString("\n\n Request Windows\n")
	hRule()
	for _, rws := range s.ClientWindows {
		buffer.WriteString(fmt.Sprintf("\nClient %x L/H %d/%d : %v\n", rws.ClientID, rws.LowWatermark, rws.HighWatermark, rws.Allocated))
		hRule()
	}

	return buffer.String()
}
