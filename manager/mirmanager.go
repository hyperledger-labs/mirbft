// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"sort"
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/statetransfer"
	"github.com/hyperledger-labs/mirbft/tracing"
	"github.com/hyperledger-labs/mirbft/util"
)

// Holds the state of the MirManager.
type MirManager struct {
	// The set of possible mir-leaders: mir-leaders are the first ids in the set of leaders of a segment.
	// The only the first leader of a segment is allowed to propose new batches.
	// The rest of segment leaders can only re-propose batches or proposes empty batches to guatantee that an epoch finishes.
	// The manager announces new segments by pushing them to this channel.
	leaderPolicy leaderPolicy

	epoch int32

	// A map that holds the nodes suspected in the current epoch to make sure
	// we don't update the leader policy more than once per epoch for the same node
	currentSuspects map[int32]bool

	// Segment issued for the current epoch, indexed by leader ID.
	currentSegments map[int32]Segment

	// Channel used to announce segments to the orderer.
	segmentChannel chan Segment

	// The manager announces where checkpoints are necessary by pushing their sequence numbers to this channel.
	checkpointSNChannel chan int32

	// ID of the next segment to issue.
	nextSegmentID int

	entriesChannel chan *log.Entry

	checkpointChannel chan *pb.StableCheckpoint

	// Buffers all the log entries committed during one epoch.
	// Used for garbage collection and client watermark advancing.
	epochEntryBuffer *util.ChannelBuffer
}

// Create a new MirManager with with fresh state
// The set of leaders is initialized to contain all the nodes
func NewMirManager() *MirManager {
	maxEpochLength := config.Config.EpochLength
	if config.Config.SegmentLength != 0 {
		maxEpochLength = membership.NumNodes() * config.Config.SegmentLength
	}
	return &MirManager{
		epoch:               0,
		leaderPolicy:        NewLeaderPolicy(config.Config.LeaderPolicy),
		segmentChannel:      make(chan Segment),
		checkpointSNChannel: make(chan int32),
		nextSegmentID:       0,
		entriesChannel:      log.Entries(),
		checkpointChannel:   log.Checkpoints(),
		epochEntryBuffer:    util.NewChannelBuffer(maxEpochLength),
		currentSuspects:     make(map[int32]bool),
	}
}

// Starts the MirManager. Afer the call to Start(), the MirManager starts observing the log and:
// - Triggers the checkpointing protocol ast the log entries advance.
// - Issues new segments as the watermark window advances with new stable checkpints.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (mm *MirManager) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	// Wait group for the two sub-goroutines
	subWg := sync.WaitGroup{}
	subWg.Add(2)

	// Observe the progressing log entries and trigger the checkpointing protocol as they advance.
	go mm.handleLogEntries(&subWg)

	// Observe the appearing stable checkpoints and fetch state as necessary.
	go mm.handleCheckpoints(&subWg)

	// Wait for the two sub-goroutines
	subWg.Wait()
}

// The node is always an orderer - return channel to which all issued Segments are pushed.
func (mm *MirManager) SubscribeOrderer() chan Segment {
	return mm.segmentChannel
}

// The node is always a potential checkpointer - return channel with all checkpointed sequence numbers.
func (mm *MirManager) SubscribeCheckpointer() chan int32 {
	return mm.checkpointSNChannel
}

// Observes the progressing log entries and triggers the checkpointing protocol as they advance.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (mm *MirManager) handleLogEntries(wg *sync.WaitGroup) {
	defer wg.Done()

	lastEpochSN := config.Config.EpochLength - 1
	if config.Config.SegmentLength != 0 {
		lastEpochSN = (config.Config.SegmentLength * len(mm.leaderPolicy.GetLeaders(0))) - 1
	}

	var stableCheckpoints chan *pb.StableCheckpoint = nil
	if config.Config.WaitForCheckpoints {
		stableCheckpoints = log.Checkpoints()
	}

	// Issue initial segments.
	initialLeaders := mm.leaderPolicy.GetLeaders(0)
	mm.issueSegments([]interface{}{}, initialLeaders, 0)
	tracing.MainTrace.Event(tracing.NEW_EPOCH, 0, int64(len(initialLeaders)))

	// Channel should be closed on shutdown for this loop to exit.
	for entry := <-mm.entriesChannel; entry != nil; entry = <-mm.entriesChannel {
		if entry.Aborted {
			// If its the first time in the current epoch we see this node as a suspect
			if _, ok := mm.currentSuspects[entry.Suspect]; !ok {
				mm.currentSuspects[entry.Suspect] = true
				mm.leaderPolicy.Update(mm.epoch, entry.Suspect)
			}
		}

		mm.epochEntryBuffer.Add(entry)

		// Advance epoch
		if entry.Sn == int32(lastEpochSN) {

			// Trigger the checkpoint protocol. For now we only trigger the checkpoint protocol at the end of the epoch.
			mm.checkpointSNChannel <- entry.Sn

			// Wait for checkpoint to become stable, if configured.
			if stableCheckpoints != nil {
				logger.Info().Int32("sn", entry.Sn).Msg("Epoch finished. Waiting for stable checkpoint.")
				chkp := <-stableCheckpoints
				if chkp.Sn != entry.Sn {
					logger.Fatal().
						Int("lastEpochSn", lastEpochSN).
						Int32("chkpSn", chkp.Sn).
						Msg("Inconsistent stable checkpoint sequence number.")
				} else {
					logger.Info().Int32("chkpSn", chkp.Sn).Msg("Epoch end confirmed.")
				}
			}

			// When the log contains entries for all the current epoch, we can advance watermarks,
			// garbage-collect old requests and compute the new batch size.
			// We do it here instead of in the handleCheckpoints() method, because:
			// - It can happen concurrently with the checkpoint protocol
			// - The Entry buffer must have received all entries of the epoch before calling Get() on it.
			//   If we called from within handleCheckpoints(), it might (and did) happen that a stable checkpoint
			//   is ready before the handleLogEntries() function flushes everything necessary in the Entry buffer.
			//   Waiting for the last entry using log.WaitForEntry() inside handleCheckpoints() does not help,
			//   as some entries might be published by the log, but not added to the Entry buffer by handleLogEntries()
			//   before Get() is called from handleCheckpoints.
			epochEntries := mm.epochEntryBuffer.Get()
			request.AdvanceWatermarks(epochEntries)

			// Only after the watermarks are up to date, we can move on to the next epoch and create new segments.
			// This cannot happen before or even concurrently, as the orderers might misinterpret incoming messages
			// if all the state is not up to date.
			mm.epoch++
			mm.currentSuspects = make(map[int32]bool)

			newLeaders := mm.leaderPolicy.GetLeaders(mm.epoch)

			logger.Debug().Int32("sn", entry.Sn+1).Int32("epoch", mm.epoch).Msg("Issuing new segments.")
			mm.issueSegments(epochEntries, newLeaders, entry.Sn+1)
			tracing.MainTrace.Event(tracing.NEW_EPOCH, int64(mm.epoch), int64(len(newLeaders)))

			if config.Config.SegmentLength != 0 {
				lastEpochSN += config.Config.SegmentLength * len(newLeaders)
			} else {
				lastEpochSN += config.Config.EpochLength
			}
		}
	}
}

// Observes the appearing stable checkpoints and advances the watermark window by issuing new segments.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (mm *MirManager) handleCheckpoints(wg *sync.WaitGroup) {
	defer wg.Done()

	// On each new checkpoint, issue new segments and advance client watermarks
	// Checkpoint channel should be closed on shutdown for this loop to exit.
	for chkp := <-mm.checkpointChannel; chkp != nil; chkp = <-mm.checkpointChannel {

		logger.Info().Int32("sn", chkp.Sn).Msg("Received stable checkpoint notification.")

		// Catch up with the checkpoint if necessary.
		// This is crucial if this peer becomes part of a minority that has fallen behind on a segment
		// and was left behind. (E.g., for PBFT, if this node is the only one to have initiated a view change.)
		statetransfer.CatchUp(chkp)
	}
}

// Create new Segments, announce them to the Orderer,
// and save them in the index (by sequence number) of this epoch's Segments.
func (mm *MirManager) issueSegments(oldEpochEntries []interface{}, leaders []int32, offset int32) {
	// Create new segments
	mm.currentSegments = mm.createSegments(mm.currentSegments, oldEpochEntries, leaders, offset)

	// Announce newly created segments to the orderer.
	for _, segment := range mm.currentSegments {

		//// Introduce an artificial delay in starting a segment for faulty nodes.
		//// Only leave the else branch.
		//// TODO: Consider removing this and only keeping the else branch.
		//if segment.Leaders()[0] == membership.OwnID && membership.OwnID < int32(config.Config.Failures) {
		//
		//	logger.Warn().Int("segID", segment.SegID()).Interface("sns", segment.SNs()).Msg("Delaying segment.")
		//
		//	seg := segment
		//	time.AfterFunc(10 * time.Second, func() {
		//		logger.Warn().Int("segID", seg.SegID()).Interface("sns", seg.SNs()).Msg("Announcing delayed segment.")
		//		mm.segmentChannel <- seg
		//	})
		//} else {
		//	mm.segmentChannel <- segment
		//}
		mm.segmentChannel <- segment
	}
}

// Create segments that completely fit inside the epoch.
// The epoch is an interval of sequence numbers starting at the last stable checkpoint with a length of epoch.
// In each epoch the number of segments issued equals the number of mir-leaders.
// Each leader is responsible for a segment.
// The offset argument is the first sequence number of the epoch.
func (mm *MirManager) createSegments(oldSegments map[int32]Segment, oldEpochEntries []interface{}, leaders []int32, offset int32) map[int32]Segment {

	// The distance of the sequence numbers in the skipping segment equals the number of mir-leaders
	// so that sequence numbers are distributed among leaders in a round robin way
	distance := len(leaders)

	epochLength := config.Config.EpochLength
	if config.Config.SegmentLength != 0 {
		epochLength = config.Config.SegmentLength * len(leaders)
	}

	// The sequence numbers of the epoch are distributed evenly among the segments
	segmentLength, remainder := epochLength/len(leaders), epochLength%len(leaders)
	segmentLengths := make([]int32, len(leaders), len(leaders))
	for i, _ := range leaders {
		segmentLengths[i] = int32(segmentLength)
	}

	for i := 0; i < remainder; i++ {
		segmentLengths[i]++
	}

	// Assign buckets to leaders
	buckets := mm.assignBuckets(leaders)

	// Creating one segment for each leader
	segments := make(map[int32]Segment, len(leaders))
	var ownSegment *SkippingSegment = nil
	for i, leader := range leaders {
		allNodeIDs := membership.AllNodeIDs() // A fresh copy is necessary on each iteration, as the content is changed.
		var leaderOffset int
		var node int32
		for leaderOffset, node = range allNodeIDs {
			if node == leader {
				break
			}
		}

		logger.Debug().Msgf("Buckets for %d-th leader %d: %v", i, leader, buckets[leader])

		// Create new segment
		seg := &SkippingSegment{
			segID:       mm.nextSegmentID,
			snDistance:  int32(distance),
			leaders:     append(allNodeIDs[leaderOffset:], allNodeIDs[:leaderOffset]...), // Rotates slice by leaderOffset
			followers:   allNodeIDs,
			snOffset:    offset + int32(i),
			snLength:    segmentLengths[i],
			startsAfter: offset - 1,
			buckets:     request.NewBucketGroup(buckets[leader]),
			batchSize:   0,
		}
		seg.initSNs()

		if leader == membership.OwnID {
			ownSegment = seg
		}

		segments[leader] = seg

		mm.nextSegmentID++
	}

	if ownSegment != nil {
		// TODO: Return to adaptive batch sizes after considering all the implications
		ownSegment.batchSize = config.Config.BatchSize
		//ownSegment.batchSize = adaptedBatchSize(oldSegments, oldEpochEntries, leaders, segments)
		//logger.Info().Int("batchSize", ownSegment.batchSize).Msg("Adapted batch size.")
	}

	// Announce new bucket assignment
	messenger.AnnounceBucketAssignment(mm.createBucketAssignmentMsg(buckets))

	return segments
}

// Given a list of leader IDs, returns a list of lists of Bucket IDs,
// assigning one list of Bucket IDs to each leader.
func (mm *MirManager) assignBuckets(leaders []int32) map[int32][]int {

	// Convenience variables

	allNodeIDs := membership.AllNodeIDs()

	isLeader := make(map[int32]bool, len(leaders)) // Index of leaders
	for _, l := range leaders {
		isLeader[l] = true
	}

	sortedLeaders := make([]int32, len(leaders))
	copy(sortedLeaders, leaders)
	sort.Slice(sortedLeaders, func(i, j int) bool {
		return sortedLeaders[i] < sortedLeaders[j]
	})

	// First uniformly distribute the buckets to all peers, even those that are not leaders.
	initBuckets := make(map[int32][]int)
	// For each node in the current membership
	for idx, i := range allNodeIDs {
		initBuckets[i] = make([]int, 0, 0)
		// Offset by epoch and assign buckets in a round-robin way
		for b := (idx + int(mm.epoch)) % membership.NumNodes(); b < len(request.Buckets); b += membership.NumNodes() {
			initBuckets[i] = append(initBuckets[i], b)
		}
	}

	// Collect buckets not assigned to leaders
	extraBuckets := make([]int, 0)
	for _, peerID := range allNodeIDs {
		if !isLeader[peerID] {
			extraBuckets = append(extraBuckets, initBuckets[peerID]...)
		}
	}

	logger.Trace().Interface("buckets", extraBuckets).Msg("Extra buckets.")

	// Redistribute buckets to leaders

	// Initialize final buckets by copying initial buckets (a deep copy is needed)
	finalBuckets := make(map[int32][]int)
	for _, leaderID := range sortedLeaders {
		finalBuckets[leaderID] = make([]int, 0)
		finalBuckets[leaderID] = append(finalBuckets[leaderID], initBuckets[leaderID]...)
	}

	// Assign extra buckets
	for _, b := range extraBuckets {
		leaderID := sortedLeaders[(b+int(mm.epoch))%len(sortedLeaders)]
		finalBuckets[leaderID] = append(finalBuckets[leaderID], b)
	}

	return finalBuckets
}

func (mm *MirManager) createBucketAssignmentMsg(assignment map[int32][]int) *pb.BucketAssignment {

	// Allocate new message.
	msg := &pb.BucketAssignment{
		Epoch:   mm.epoch,
		Buckets: make(map[int32]*pb.ListOfInt32),
	}

	// We make a copy of assignment, to not depend on assignment not being modified later.
	// For each bucket list assigned to a peer
	for peerID, buckets := range assignment {

		// Allocate new bucket list.
		bucketList := &pb.ListOfInt32{Vals: make([]int32, len(buckets), len(buckets))}
		msg.Buckets[peerID] = bucketList

		// Copy bucket IDs to bucket list.
		for i, b := range buckets {
			bucketList.Vals[i] = int32(b)
		}
	}

	return msg
}

func adaptedBatchSize(oldSegments map[int32]Segment, entries []interface{}, leaders []int32, newSegments map[int32]Segment) int { // entries must be of type []*log.Entry

	// Convenience variables
	ownID := membership.OwnID
	lastBatchSize := config.Config.BatchSize
	oldSegmentIndex := make(map[int32]Segment) // maps sequence numbers from previous epoch to their leaders
	for _, seg := range oldSegments {
		for _, sn := range seg.SNs() {
			oldSegmentIndex[sn] = seg
		}
	}

	// Initialize map of leader loads.
	// The explicit initialization to 0 is important, as it distinguishes the zero keys that are present in the map
	// from those which are not.
	nRequests := make(map[int32]int, len(leaders))
	epochStarted := make(map[int32]int64, len(leaders))
	epochFinished := make(map[int32]int64, len(leaders))
	for _, leader := range leaders {
		nRequests[leader] = 0
		epochStarted[leader] = 0
		epochFinished[leader] = 0
	}

	// For each leader (of the new epoch), compute the total number of committed requests (in the previous epoch)
	// and the time it took the leader to finish the epoch.
	for _, e := range entries {
		entry := e.(*log.Entry)
		leader := oldSegmentIndex[entry.Sn].Leaders()[0]
		if _, ok := nRequests[leader]; ok {

			// Add number of requests committed by leader
			nRequests[leader] += len(entry.Batch.Requests)

			// The epoch start for a leader is the smallest timestamp of all proposals of that leader.
			if epochStarted[leader] == 0 || entry.ProposeTs < epochStarted[leader] {
				epochStarted[leader] = entry.ProposeTs
			}

			// The epoch finish for a leader is the largest timestamp of all commits of that leader.
			if entry.CommitTs > epochFinished[leader] {
				epochFinished[leader] = entry.CommitTs
			}
		}

		// Find out last own batch size
		if leader == ownID {
			lastBatchSize = oldSegmentIndex[entry.Sn].BatchSize()
		}
	}

	// If I did not submit anything in the previous epoch (i.e. I was most likely not a leader), use maximum batch size.
	ownRequests := nRequests[ownID]
	if ownRequests == 0 {
		logger.Info().Int("batchSize", config.Config.BatchSize).Msg("No own requests committed in last epoch.")
		return config.Config.BatchSize
	}

	// Compute the duration of each leader's segment
	// Always in milliseconds
	durations := make(map[int32]int)
	for _, leader := range leaders {
		durations[leader] = int((epochFinished[leader] - epochStarted[leader]) / 1000000)
		logger.Info().
			Int32("leader", leader).
			Int("duration", durations[leader]).
			Int("nReq", nRequests[leader]).
			Msg("Segment stats.")
	}

	// Get all the other leaders that committed as many requests as me in the last epoch, and did so at least as fast.
	fastLeaders := make([]int32, 0)
	for _, leader := range leaders {
		if leader != ownID && nRequests[leader] >= nRequests[ownID] && durations[leader] < durations[ownID] {
			fastLeaders = append(fastLeaders, leader)
		}
	}

	// If there is fewer than f+1 better leaders, I increase my batch size.
	if len(fastLeaders) <= membership.Faults() {
		batchSize := lastBatchSize + (config.Config.BatchSizeIncrement * (membership.Faults() + 1 - len(fastLeaders)) / (membership.Faults() + 1))
		if batchSize > config.Config.BatchSize {
			batchSize = config.Config.BatchSize
		}
		logger.Info().
			Int("rank", len(fastLeaders)+1).
			Int("batchSize", batchSize).
			Msg("Increasing batch size.")
		return batchSize
	} else {

		// Sort the fast leaders by the time they needed to finish their corresponding segments.
		sort.Slice(fastLeaders, func(i, j int) bool {
			return durations[fastLeaders[i]] < durations[fastLeaders[j]]
		})

		// If there are at least f+1 better leaders, but the f+1st did not have to wait too much for me,
		// I keep the old batch size (StragglerTolerance in milliseconds)
		if durations[ownID]-durations[fastLeaders[membership.Faults()]] <= config.Config.StragglerTolerance {
			logger.Info().
				Int("delay", durations[ownID]-durations[fastLeaders[membership.Faults()]]).
				Int("batchSize", lastBatchSize).
				Msg("Keeping batch size.")
			return lastBatchSize

			// Otherwise, try to match the duration of the f+1st leader, assuming that leader keeps its batch size.
		} else {
			ownThroughput := nRequests[ownID] * 1000 / durations[ownID] // in req/s (durations are in ms)

			targetLeader := fastLeaders[membership.Faults()]
			targetTime := int32(durations[targetLeader]) * newSegments[targetLeader].Len() / oldSegments[targetLeader].Len()
			timePerBatch := targetTime / newSegments[ownID].Len()
			batchSize := int(ownThroughput) * int(timePerBatch) / 1000 // (timePerBatch is in ms, while ownThroughput is in req/s)

			logger.Info().
				Int("delay", durations[ownID]-durations[fastLeaders[membership.Faults()]]).
				Int("reqps", ownThroughput).
				Int("duration", durations[ownID]).
				Int("lastBatchSize", lastBatchSize).
				Int("newBatchSize", batchSize).
				Msg("Straggling. Adapting batch size.")

			if batchSize <= config.Config.BatchSize {
				return batchSize
			} else {
				return config.Config.BatchSize
			}
		}
	}
}
