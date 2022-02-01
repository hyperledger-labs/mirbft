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
	"sync"

	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/util"
)

// Holds the state of the DummyManager.
type DummyManager struct {

	// Length of the issued segments.
	segmentLength int

	// The manager announces new segments by pushing them to this channel.
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

// Create a new DummyManager with with fresh state
func NewDummyManager() *DummyManager {
	return &DummyManager{
		segmentChannel:      make(chan Segment),
		checkpointSNChannel: make(chan int32),
		nextSegmentID:       0,
		entriesChannel:      log.Entries(),
		checkpointChannel:   log.Checkpoints(),
		epochEntryBuffer:    util.NewChannelBuffer(config.Config.EpochLength),

		// After every checkpoint, new segments can be issued (i.e., watermark can be advanced).
		// It is thus natural to advance the watermark by as much as the log advanced through the checkpoint,
		// devided by the number of leaders (Dummy manager has all nodes a leaders).
		segmentLength: config.Config.CheckpointInterval / membership.NumNodes(),
	}
}

// Starts the DummyManager. Afer the call to Start(), the DummyManager starts observing the log and:
// - Triggers the checkpointing protocol ast the log entries advance.
// - Issues new segments as the watermark window advances with new stable checkpints.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (dm *DummyManager) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	// Wait group for the two sub-goroutines
	subWg := sync.WaitGroup{}
	subWg.Add(2)

	// Observe the progressing log entries and trigger the checkpointing protocol as they advance.
	go dm.handleLogEntries(&subWg)

	// Observe the appearing stable checkpoints and advance the watermark window by issuing new segments.
	go dm.handleCheckpoints(&subWg)

	// Wait for the two sub-goroutines
	subWg.Wait()
}

// The node is always an orderer - return channel to which all issued Segments are pushed.
func (dm *DummyManager) SubscribeOrderer() chan Segment {
	return dm.segmentChannel
}

// The node is always a potential checkpointer - return channel with all checkpointed sequence numbers.
func (dm *DummyManager) SubscribeCheckpointer() chan int32 {
	return dm.checkpointSNChannel
}

// Creates a new log Segment consisting of length contiguous sequence numbers starting at offset.
// All nodes are involved as followers.
// The sequence of leaders contains all nodes, starting at leaderOffset and wraps around at the end.
// This is how PBFT behaves. Creating a new segment here corresponds to advancing the watermarks in PBFT.
// The segment only has one bucket wrapped in a BucketGroup
func (dm *DummyManager) NewDummySegment(segID int, offset int32, leaderOffset int) Segment {

	// Create slices of leaders and followers.
	allNodeIDs := membership.AllNodeIDs()
	leaderOffset %= len(allNodeIDs) // Make sure that wrapping doesn't cause an index out of bounds

	// Return new segment.
	return &ContiguousSegment{
		segID:     segID,
		leaders:   append(allNodeIDs[leaderOffset:], allNodeIDs[:leaderOffset]...), // Rotates slice by leaderOffset
		followers: allNodeIDs,
		snOffset:  offset,
		snLength:  int32(dm.segmentLength),
		// To parallelize the segment execution, subtract length for each extra bucket.
		startsAfter: offset - (int32(config.Config.NumBuckets)-1)*int32(dm.segmentLength) - 1,
		buckets:     request.NewBucketGroup([]int{segID % config.Config.NumBuckets}), // Round-robin bucket assignment
	}
}

// Observes the progressing log entries and triggers the checkpointing protocol as they advance.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (dm *DummyManager) handleLogEntries(wg *sync.WaitGroup) {
	defer wg.Done()
	checkpointInterval := int32(config.Config.CheckpointInterval)

	// Channel should be closed on shutdown for this loop to exit.
	for entry := <-dm.entriesChannel; entry != nil; entry = <-dm.entriesChannel {

		dm.epochEntryBuffer.Add(entry)

		// e.g., if checpointInterval is 10, this is true for 9, 19, 29, ...
		if entry.Sn%checkpointInterval == checkpointInterval-1 {

			// This should trigger the checkpoint protocol for sequence number up to (including) entry.Sn
			dm.checkpointSNChannel <- entry.Sn
		}
	}
}

// Observes the appearing stable checkpoints and advances the watermark window by issuing new segments.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (dm *DummyManager) handleCheckpoints(wg *sync.WaitGroup) {
	defer wg.Done()

	// Issue initial segments.
	dm.issueSegments(nil)

	// On each new checkpoint, advance the client watermark window and issue new segments.
	// Checkpoint channel should be closed on shutdown for this loop to exit.
	for chkp := <-dm.checkpointChannel; chkp != nil; chkp = <-dm.checkpointChannel {

		// Advance all client request watermark windows
		request.AdvanceWatermarks(dm.epochEntryBuffer.Get())
		dm.issueSegments(chkp)
	}
}

// Issue segments that completely fit inside the watermark window.
// The watermark window is an interval of sequence numbers starting at the last stable checkpoint with a length of
// watermarkWindowSize.
// Note: The chkp parameter is technically not necessary, as this function could directly access the latest checkpoint
//       through the log, but this solution avoids the data race with concurrent checkpoint updates
func (dm *DummyManager) issueSegments(chkp *pb.StableCheckpoint) {

	// Obtain offset (i.e. the first sequence number) of the watermark window.
	var offset int32
	if chkp == nil {
		offset = 0
	} else {
		offset = chkp.Sn + 1
	}

	// While all the sequence numbers of the next segment are inside the watermark window
	// ( (dm.nextSegmentID + 1) * dummySegmentLength is the first SN of the segment after the next segment
	// and offset + watermarkWindowSize is the first SN not in the watermark window)
	for int32((dm.nextSegmentID+1)*dm.segmentLength) <=
		offset+int32(config.Config.WatermarkWindowSize) {

		// Create new segment
		dm.segmentChannel <- dm.NewDummySegment(
			dm.nextSegmentID,
			int32(dm.nextSegmentID*dm.segmentLength),
			dm.nextSegmentID)

		dm.nextSegmentID++
	}
}

// Issue segments that completely fit inside the epoch.
// The epoch is an interval of sequence numbers starting at the last stable checkpoint with a length of epoch.
// In each epoch the number of segments issued equals the number of mir-leaders.
// Each leader is responsible for a segment.
// The offset argument is the first sequence number of the epoch.
func (dm *DummyManager) issueSkippingSegments(offset int32) {

	// The distance of the sequence numbers in the skipping segment equals the number of mir-leaders
	// so that sequence numbers are distributed among leaders in a round robin way
	nLeaders := membership.NumNodes()
	epochLength := config.Config.WatermarkWindowSize

	// The sequence numbers of the epoch are distributed evenly among the segments
	segmentLength, remainder := epochLength/nLeaders, epochLength%nLeaders
	segmentLengths := make([]int32, nLeaders, nLeaders)
	for i, _ := range membership.AllNodeIDs() {
		segmentLengths[i] = int32(segmentLength)
	}

	for i := 0; i < remainder; i++ {
		segmentLengths[i]++
	}

	buckets := dm.assignBuckets()

	// Creating one segment for each leader
	for i, _ := range membership.AllNodeIDs() {

		allNodeIDs := membership.AllNodeIDs()
		seg := &SkippingSegment{
			segID:       dm.nextSegmentID,
			snDistance:  int32(nLeaders),
			leaders:     append(allNodeIDs[i:], allNodeIDs[:i]...), // Rotates slice by leaderOffset
			followers:   allNodeIDs,
			snOffset:    offset + int32(i),
			snLength:    segmentLengths[i],
			startsAfter: offset - 1,
			buckets:     request.NewBucketGroup(buckets[int32(i)]),
			batchSize:   0,
		}
		seg.initSNs()

		dm.segmentChannel <- seg

		dm.nextSegmentID++
	}
}

// Given a list of leader IDs, returns a list of lists of Bucket IDs,
// assigning one list of Bucket IDs to each leader.
// This dummy function distributes buckets to all nodes, meaning that all nodes have to be leaders.
func (dm *DummyManager) assignBuckets() [][]int {

	buckets := make([][]int, membership.NumNodes(), membership.NumNodes())
	for i := 1; i < membership.NumNodes(); i++ {
		buckets[i] = make([]int, 0, 0)
		for b := i; b < len(request.Buckets); b += membership.NumNodes() {
			buckets[i] = append(buckets[i], b)
		}
	}
	return buckets
}
