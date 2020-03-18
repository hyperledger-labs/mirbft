/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"
	"math"
)

type Status struct {
	NodeID         uint64                 `json:"node_id"`
	LowWatermark   uint64                 `json:"low_watermark"`
	HighWatermark  uint64                 `json:"high_watermark"`
	EpochChanger   *EpochChangerStatus    `json:"epoch_changer"`
	Nodes          []*NodeStatus          `json:"nodes"`
	Buckets        []*BucketStatus        `json:"buckets"`
	Checkpoints    []*CheckpointStatus    `json:"checkpoints"`
	RequestWindows []*RequestWindowStatus `json:"request_windows"`
}

type BucketStatus struct {
	ID        uint64          `json:"id"`
	Leader    bool            `json:"leader"`
	Sequences []SequenceState `json:"sequences"`
}

type CheckpointStatus struct {
	SeqNo         uint64 `json:"seq_no"`
	MaxAgreements int    `json:"max_agreements"`
	NetQuorum     bool   `json:"net_quorum"`
	LocalDecision bool   `json:"local_decision"`
}

type EpochChangerStatus struct {
	State           epochChangeState     `json:"state"`
	LastActiveEpoch uint64               `json:"last_active_epoch"`
	EpochTargets    []*EpochTargetStatus `json:"epoch_targets"`
}

type EpochTargetStatus struct {
	Number       uint64   `json:"number"`
	EpochChanges []uint64 `json:"epoch_changes"`
	Echos        []uint64 `json:"echos"`
	Readies      []uint64 `json:"readies"`
	Suspicions   []uint64 `json:"suspicions"`
}

type NodeStatus struct {
	ID             uint64             `json:"id"`
	BucketStatuses []NodeBucketStatus `json:"bucket_statuses"`
	LastCheckpoint uint64             `json:"last_checkpoint"`
}

type NodeBucketStatus struct {
	BucketID    int    `json:"bucket_id"`
	IsLeader    bool   `json:"is_leader"`
	LastPrepare uint64 `json:"last_prepare"`
	LastCommit  uint64 `json:"last_commit"`
}

type RequestWindowStatus struct {
	ClientID      []byte   `json:"client_id"`
	LowWatermark  uint64   `json:"low_watermark"`
	HighWatermark uint64   `json:"high_watermark"`
	Allocated     []uint64 `json:"allocated"`
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("===========================================\n"))
	buffer.WriteString(fmt.Sprintf("NodeID=%d, LowWatermark=%d, HighWatermark=%d, Epoch=%d\n", s.NodeID, s.LowWatermark, s.HighWatermark, s.EpochChanger.LastActiveEpoch))
	buffer.WriteString(fmt.Sprintf("===========================================\n\n"))

	buffer.WriteString("=== Epoch Changer ===\n")
	buffer.WriteString(fmt.Sprintf("Change is in state: %d, last active epoch %d\n", s.EpochChanger.State, s.EpochChanger.LastActiveEpoch))
	for _, et := range s.EpochChanger.EpochTargets {
		buffer.WriteString(fmt.Sprintf("Target Epoch %d:\n", et.Number))
		buffer.WriteString(fmt.Sprintf("  EpochChanges: %v\n", et.EpochChanges))
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

	for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
		magnitude := math.Pow10(i - 1)
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
			buffer.WriteString(fmt.Sprintf(" %d", seqNo/uint64(magnitude)%10))
		}
		buffer.WriteString("\n")
	}

	if s.LowWatermark == s.HighWatermark {
		buffer.WriteString("=== Empty Watermarks ===\n")
		return buffer.String()
	}

	for _, nodeStatus := range s.Nodes {
		hRule()
		buffer.WriteString(fmt.Sprintf("- === Node %d === \n", nodeStatus.ID))
		for bucket, bucketStatus := range nodeStatus.BucketStatuses {
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo += uint64(len(s.Buckets)) {
				if seqNo == nodeStatus.LastCheckpoint {
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

	for _, bucketStatus := range s.Buckets {
		buffer.WriteString("| ")
		for _, state := range bucketStatus.Sequences {
			switch state {
			case Uninitialized:
				buffer.WriteString("| ")
			case Allocated:
				buffer.WriteString("|A")
			case Invalid:
				buffer.WriteString("|I")
			case Preprepared:
				buffer.WriteString("|Q")
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

	hRule()
	buffer.WriteString("-\n")

	buffer.WriteString("\n\n Request Windows\n")
	hRule()
	for _, rws := range s.RequestWindows {
		buffer.WriteString(fmt.Sprintf("\nClient %x L/H %d/%d : %v\n", rws.ClientID, rws.LowWatermark, rws.HighWatermark, rws.Allocated))
		hRule()
	}

	return buffer.String()
}
