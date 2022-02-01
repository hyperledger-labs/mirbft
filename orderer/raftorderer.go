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

package orderer

import (
	"sync"
	"sync/atomic"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

type RaftOrderer struct {
	segmentChan chan manager.Segment // Channel to which the Manager pushes new Segments.
	dispatcher  raftDispatcher       // map[int32]*raftInstance
	backlog     backlog              // map[int32]chan*ordererMsg
	last        int32                // Some sequence number we can ignore backlogMsgs above
	lock        sync.Mutex
}

type raftDispatcher struct {
	mm sync.Map
}

func (d *raftDispatcher) load(key int32) (*raftInstance, bool) {
	if v, ok := d.mm.Load(key); ok {
		return v.(*raftInstance), ok
	}
	return nil, false
}

func (d *raftDispatcher) store(key int32, value *raftInstance) {
	d.mm.Store(key, value)
}

func (d *raftDispatcher) delete(key int32) {
	d.mm.Delete(key)
}

// HandleMessage is called by the messenger each time an Orderer-issued message is received over the network.
func (ro *RaftOrderer) HandleMessage(msg *pb.ProtocolMessage) {
	logger.Trace().
		Int32("sn", msg.Sn).
		Int32("senderID", msg.SenderId).
		Msg("Handling message.")

	if msg.SenderId == membership.OwnID {
		logger.Warn().
			Msg("RaftOrderer ignores message from self.")
		return
	}
	sn := msg.Sn

	// Check if message is from an old segment and needs to be discarded
	last := atomic.LoadInt32(&ro.last)
	if sn <= last {
		logger.Info().
			Int32("sn", sn).
			Int32("senderID", msg.SenderId).
			Msg("RaftOrderer discards message. Message belongs to an old segment.")
	}

	// Check if the message is for a future message and needs to be backlogged
	ri, ok := ro.dispatcher.load(sn)
	if !ok {
		logger.Info().
			Int32("sn", sn).
			Int32("senderID", msg.SenderId).
			Msg("RaftOrderer cannot handle message. No segment is available")
		ro.backlog.add(msg)
		return
	}

	ri.serializer.serialize(msg)
}

func (ro *RaftOrderer) HandleEntry(entry *log.Entry) {
	// Treat the log entry as a MissingEntry message
	// and process it using the instance according to its sequence number.
	ro.HandleMessage(&pb.ProtocolMessage{
		SenderId: -1,
		Sn:       entry.Sn,
		Msg: &pb.ProtocolMessage_MissingEntry{
			MissingEntry: &pb.MissingEntry{
				Sn:      entry.Sn,
				Batch:   entry.Batch,
				Digest:  entry.Digest,
				Aborted: entry.Aborted,
				Suspect: entry.Suspect,
				Proof:   "Dummy Proof.",
			},
		},
	})
}

// Init initializes the Raft orderer.
// Subscribes to new segments issued by the Manager and allocates internal buffers and data structures.
func (ro *RaftOrderer) Init(mngr manager.Manager) {
	if config.Config.SignRequests {
		logger.Warn().Msg("Signature verification should be disabled")
	}
	ro.segmentChan = mngr.SubscribeOrderer()
	ro.backlog = newBacklog()
	ro.lock = sync.Mutex{}
	ro.last = -1
}

// Start starts the Raft orderer.
// Listens on the channel where the Manager issues new segments and starts a goroutine to handle each of them.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (ro *RaftOrderer) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for s, ok := <-ro.segmentChan; ok; s, ok = <-ro.segmentChan {
		logger.Info().
			Int("segId", s.SegID()).
			Int32("length", s.Len()).
			Int32("firstSN", s.FirstSN()).
			Int32("lastSN", s.LastSN()).
			Int32("first leader", s.Leaders()[0]).
			Msgf("RaftOrderer received new a segment: %+v", s.SNs())

		ro.runSegment(s)
		go ro.killSegment(s)
	}
}

// Runs the Raft ordering algorithm for a Segment.
func (ro *RaftOrderer) runSegment(seg manager.Segment) {
	ri := &raftInstance{}
	ri.init(seg, ro)
	for _, sn := range seg.SNs() {
		ro.dispatcher.store(sn, ri)

	}
	logger.Info().Int("segID", seg.SegID()).
		Int32("first", seg.FirstSN()).
		Int32("last", seg.LastSN()).
		Msg("Starting Raft instance.")

	ri.subscribeToBacklog()
	ri.start() // no need to spawn a go routine for start, it spawns a goroutine itself
	go ri.processSerializedMessages()

}

func (ro *RaftOrderer) killSegment(seg manager.Segment) {
	// Wait until this segment is part of a stable checkpoint, AND all the sequence numbers are committed.
	// It might happen that we obtain a stable checkpoint before committing all sequence numbers, if others are faster.
	// It is important to subscribe before getting the current checkpoint, in case of a concurrent checkpoint update.
	checkpoints := log.Checkpoints()
	currentCheckpoint := log.GetCheckpoint()
	for currentCheckpoint == nil || currentCheckpoint.Sn < seg.LastSN() {
		currentCheckpoint = <-checkpoints
	}
	log.WaitForEntry(seg.LastSN())

	// Update the last sequence number the orderer accepts messages for
	ro.lock.Lock()
	if seg.LastSN() > ro.last {
		atomic.StoreInt32(&ro.last, seg.LastSN())
	}
	ro.lock.Unlock()

	// This is only possible because of the existence of the stable checkpoint.
	// Otherwise other segments could be affected, as the sequence numbers interleave.
	ro.backlog.gc <- seg.LastSN()

	// We just need any entry from this segment
	ri, ok := ro.dispatcher.load(seg.LastSN())
	if !ok {
		logger.Error().
			Int("segId", seg.SegID()).
			Msg("No instance available.")
		return
	}

	// Close the message channel for the segment
	logger.Info().Int("segID", seg.SegID()).Msg("Closing message serializers.")

	ri.serializer.stop()
	ri.stopProposing()

	// Delete the pbftInstance for the segment
	for _, sn := range seg.SNs() {
		ro.dispatcher.delete(sn)
	}

}

func (ro *RaftOrderer) Sign(data []byte) ([]byte, error) {
	return nil, nil
}

func (ro *RaftOrderer) CheckSig(data []byte, senderID int32, signature []byte) error {
	return nil
}
