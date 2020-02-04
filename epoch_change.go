/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"

	pb "github.com/IBM/mirbft/mirbftpb"
	// "github.com/golang/protobuf/proto"

	"github.com/pkg/errors"
)

// TODO, these nested maps are a little tricky to read, might be better to make proper types

type epochChange struct {
	underlying   *pb.EpochChange
	lowWatermark uint64
	pSet         map[bucketSeqNo]*pb.EpochChange_SetEntry
	qSet         map[bucketSeqNo]map[uint64]*pb.EpochChange_SetEntry
}

type bucketSeqNo struct {
	bucket uint64
	seqNo  uint64
}

func newEpochChange(underlying *pb.EpochChange) (*epochChange, error) {
	if len(underlying.Checkpoints) == 0 {
		return nil, errors.Errorf("epoch change did not contain any checkpoints")
	}

	lowWatermark := underlying.Checkpoints[0].SeqNo
	checkpoints := map[uint64]*pb.Checkpoint{}

	for _, checkpoint := range underlying.Checkpoints {
		if lowWatermark > checkpoint.SeqNo {
			lowWatermark = checkpoint.SeqNo
		}

		if _, ok := checkpoints[checkpoint.SeqNo]; ok {
			return nil, errors.Errorf("epoch change contained duplicated seqnos for %d", checkpoint.SeqNo)
		}
	}

	// TODO, check pSet and qSet for 'too advanced' views.

	// TODO, check pSet and qSet for entries within log window relative to low watermark

	pSet := map[bucketSeqNo]*pb.EpochChange_SetEntry{}
	for _, entry := range underlying.PSet {
		bsn := bucketSeqNo{
			bucket: entry.Bucket,
			seqNo:  entry.SeqNo,
		}
		if _, ok := pSet[bsn]; ok {
			return nil, errors.Errorf("epoch change pSet contained duplicate seqnos %d for bucket %d", entry.SeqNo, entry.Bucket)
		}

		pSet[bsn] = entry
	}

	qSet := map[bucketSeqNo]map[uint64]*pb.EpochChange_SetEntry{}
	for _, entry := range underlying.QSet {
		bsn := bucketSeqNo{
			bucket: entry.Bucket,
			seqNo:  entry.SeqNo,
		}

		views, ok := qSet[bsn]
		if !ok {
			views = map[uint64]*pb.EpochChange_SetEntry{}
		}

		if _, ok := views[entry.Epoch]; ok {
			return nil, errors.Errorf("epoch change qSet contained duplicate entries for seqnos=%d bucket=%d epoch=%d", entry.SeqNo, entry.Bucket, entry.Epoch)
		}

		views[entry.Epoch] = entry
	}

	return &epochChange{
		underlying:   underlying,
		lowWatermark: lowWatermark,
		pSet:         pSet,
		qSet:         qSet,
	}, nil
}

func (ech *epochChange) pSetByBucketSeq(bucket, seqNo uint64) (*pb.EpochChange_SetEntry, bool) {
	entry, ok := ech.pSet[bucketSeqNo{
		bucket: bucket,
		seqNo:  seqNo,
	}]
	return entry, ok
}

func (ech *epochChange) qSetByBucketSeq(bucket, seqNo uint64) (map[uint64]*pb.EpochChange_SetEntry, bool) {
	entry, ok := ech.qSet[bucketSeqNo{
		bucket: bucket,
		seqNo:  seqNo,
	}]
	return entry, ok
}

func constructNewEpochConfig(config *epochConfig, epochChanges map[NodeID]*epochChange) *pb.EpochConfig {
	type checkpointKey struct {
		SeqNo uint64
		Value string
	}

	checkpoints := map[checkpointKey][]NodeID{}

	var newEpochNumber uint64 // TODO this is super-hacky

	for nodeID, epochChange := range epochChanges {
		newEpochNumber = epochChange.underlying.NewEpoch
		for _, checkpoint := range epochChange.underlying.Checkpoints {

			key := checkpointKey{
				SeqNo: checkpoint.SeqNo,
				Value: string(checkpoint.Value),
			}

			checkpoints[key] = append(checkpoints[key], nodeID)
		}
	}

	var maxCheckpoint *checkpointKey

	for key, supporters := range checkpoints {
		if len(supporters) < config.someCorrectQuorum() {
			continue
		}

		nodesWithLowerWatermark := 0
		for _, epochChange := range epochChanges {
			if epochChange.lowWatermark <= key.SeqNo {
				nodesWithLowerWatermark++
			}
		}

		if nodesWithLowerWatermark < config.intersectionQuorum() {
			continue
		}

		if maxCheckpoint == nil {
			maxCheckpoint = &key
			continue
		}

		if maxCheckpoint.SeqNo > key.SeqNo {
			continue
		}

		if maxCheckpoint.SeqNo == key.SeqNo {
			panic("two correct quorums have different checkpoints for same seqno")
		}

		maxCheckpoint = &key
	}

	if maxCheckpoint == nil {
		return nil
	}

	newEpochConfig := &pb.EpochConfig{
		Number: newEpochNumber,
		StartingCheckpoint: &pb.Checkpoint{
			SeqNo: maxCheckpoint.SeqNo,
			Value: []byte(maxCheckpoint.Value),
		},
		FinalPreprepares: make([]*pb.EpochConfig_Bucket, len(config.buckets)),
	}

	type entryKey struct {
		seqNo  uint64
		bucket uint64
	}

	for bucketID := range newEpochConfig.FinalPreprepares {
		digests := make([][]byte, 2*config.checkpointInterval)
		newEpochConfig.FinalPreprepares[bucketID] = &pb.EpochConfig_Bucket{
			Digests: digests,
		}

		for seqNoOffset := range digests {
			seqNo := uint64(seqNoOffset) + maxCheckpoint.SeqNo + 1

			for _, nodeID := range config.nodes {
				// Note, it looks like we're re-implementing `range epochChanges` here,
				// and we are, but doing so in a deterministic order.

				epochChange, ok := epochChanges[nodeID]
				if !ok {
					continue
				}

				entry, ok := epochChange.pSetByBucketSeq(uint64(bucketID), seqNo)
				if !ok {
					continue
				}

				a1Count := 0
				for _, iEpochChange := range epochChanges {
					if iEpochChange.lowWatermark >= seqNo {
						continue
					}

					iEntry, ok := iEpochChange.pSetByBucketSeq(uint64(bucketID), seqNo)
					if !ok || iEntry.Epoch < entry.Epoch {
						a1Count++
						continue
					}

					if iEntry.Epoch > entry.Epoch {
						continue
					}

					// Thus, iEntry.Epoch == entry.Epoch

					if bytes.Equal(entry.Digest, iEntry.Digest) {
						a1Count++
					}
				}

				if a1Count < config.intersectionQuorum() {
					continue
				}

				a2Count := 0
				for _, iEpochChange := range epochChanges {
					viewEntries, ok := iEpochChange.qSetByBucketSeq(uint64(bucketID), seqNo)
					if !ok {
						continue
					}

					for view, iEntry := range viewEntries {
						if view < entry.Epoch {
							continue
						}

						if !bytes.Equal(entry.Digest, iEntry.Digest) {
							continue
						}

						a2Count++
						break
					}
				}

				if a2Count < config.someCorrectQuorum() {
					continue
				}

				digests[seqNoOffset] = entry.Digest
				break
			}

			if digests[seqNoOffset] != nil {
				// Some entry from the pSet was selected for this bucketSeq
				continue
			}

			bCount := 0
			for _, epochChange := range epochChanges {
				if epochChange.lowWatermark >= seqNo {
					continue
				}

				if _, ok := epochChange.pSetByBucketSeq(uint64(bucketID), seqNo); !ok {
					bCount++
				}
			}

			if bCount < config.intersectionQuorum() {
				// We could not satisfy condition A, or B, we need to wait
				return nil
			}
		}
	}

	return newEpochConfig
}
