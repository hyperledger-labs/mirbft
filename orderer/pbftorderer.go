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
	"time"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"sort"
)

// Represents a PBFT Orderer implementation.
type PbftOrderer struct {
	segmentChan chan manager.Segment // Channel to which the Manager pushes new Segments.
	dispatcher  pbftDispatcher       // map[int32]*pbftInstance
	backlog     backlog              // map[int32]chan*ordererMsg
	last        int32                // Some sequence number we can ignere messages above
	commitTime  time.Duration		 // Median commit duration
	lock        sync.Mutex
}

type pbftDispatcher struct {
	mm sync.Map
}

func (d *pbftDispatcher) load(key int32) (*pbftInstance, bool) {
	if v, ok := d.mm.Load(key); ok {
		return v.(*pbftInstance), ok
	}
	return nil, false
}

func (d *pbftDispatcher) store(key int32, value *pbftInstance) {
	d.mm.Store(key, value)
}

func (d *pbftDispatcher) delete(key int32) {
	d.mm.Delete(key)
}

// HandleMessage is called by the messenger each time an Orderer-issued message is received over the network.
func (po *PbftOrderer) HandleMessage(msg *pb.ProtocolMessage) {
	//logger.Trace().
	//	Int32("sn", msg.Sn).
	//	Int32("senderID", msg.SenderId).
	//	Msg("Handling message.")

	sn := msg.Sn

	if msg.SenderId == membership.OwnID {
		logger.Warn().Int32("sn", sn).Msg("PbftOrderer handles message from self.")
	}

	// Check if message is from an old segment and needs to be discarded
	last := atomic.LoadInt32(&po.last)
	if sn <= last {
		logger.Debug().
			Int32("sn", sn).
			Int32("senderID", msg.SenderId).
			Msg("PbftOrderer discards message. Message belongs to an old segment.")
		return
	}

	// Set reception timestamp for Preprepare and Commit messages.
	// This is a hook required for measuring the throughput of different leaders.
	// We use this information to control own batch size as a means to deal with stragglers.
	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_Preprepare:
		m.Preprepare.Ts = time.Now().UnixNano()
	case *pb.ProtocolMessage_Commit:
		m.Commit.Ts = time.Now().UnixNano()

		// TODO: Check for message types that should be handled with priority here, once the priority queue is implemented.

	}

	// Check if the message is for a future message and needs to be backlogged
	pi, ok := po.dispatcher.load(sn)
	if !ok {
		//logger.Info().
		//	Int32("sn",sn).
		//	Int32("senderID",  msg.SenderId).
		//	Msg("PbftOrderer cannot handle message. No segment is available")
		po.backlog.add(msg)
		return
	}

	// If we are not distinguishing priority from non-priority messages, do not check type.
	pi.serializer.serialize(msg)

	//// Check the tye of the message.
	//switch msg.Msg.(type) {
	//case *pb.ProtocolMessage_Viewchange:
	//	//pi.priority.serialize(msg)
	//	pi.serializer.serialize(msg)
	//case *pb.ProtocolMessage_Newview:
	//	//pi.priority.serialize(msg)
	//	pi.serializer.serialize(msg)
	//default:
	//	pi.serializer.serialize(msg)
	//}
}

// Handles entries produced externally.
func (po *PbftOrderer) HandleEntry(entry *log.Entry) {
	// Treat the log entry as a MissingEntry message
	// and process it using the instance according to its sequence number.
	po.HandleMessage(&pb.ProtocolMessage{
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

// Initializes the PbftOrderer.
// Subscribes to new segments issued by the Manager and allocates internal buffers and data structures.
func (po *PbftOrderer) Init(mngr manager.Manager) {
	po.segmentChan = mngr.SubscribeOrderer()
	po.backlog = newBacklog()
	po.last = -1
}

// Starts the PbftOrderer. Listens on the channel where the Manager issues new Segemnts and starts a goroutine to
// handle each of them.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (po *PbftOrderer) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	//go func() {
	//	time.Sleep(20*time.Second)
	//	if membership.OwnID == 0 {
	//		logger.Fatal().Msg("Peer is crashing")
	//	}
	//}()

	for s, ok := <-po.segmentChan; ok; s, ok = <-po.segmentChan {

		logger.Info().
			Int("segId", s.SegID()).
			Int32("length", s.Len()).
			Int32("firstSN", s.FirstSN()).
			Int32("lastSN", s.LastSN()).
			Int32("first leader", s.Leaders()[0]).
			Int32("len", s.Len()).
			//Int("bucket", s.Bucket().GetId()).
			Msgf("PbftOrderer received a new segment: %+v", s.SNs())

		po.runSegment(s)
		go po.killSegment(s)
	}
}

// Runs the pbft ordering algorithm for a Segment.
func (po *PbftOrderer) runSegment(seg manager.Segment) {
	pi := &pbftInstance{}
	pi.init(seg, po)
	for _, sn := range seg.SNs() {
		po.dispatcher.store(sn, pi)

	}
	logger.Info().Int("segID", seg.SegID()).
		Int32("first", seg.FirstSN()).
		Int32("last", seg.LastSN()).
		Msg("Starting PBFT instance.")

	pi.subscribeToBacklog()

	if isLeading(seg, membership.OwnID, pi.view) {
		go pi.lead()
	}
	go pi.processSerializedMessages()

}

func (po *PbftOrderer) killSegment(seg manager.Segment) {
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
	po.lock.Lock()
	if seg.LastSN() > po.last {
		atomic.StoreInt32(&po.last, seg.LastSN())
	}
	po.lock.Unlock()

	// This is only possible because of the existence of the stable checkpoint.
	// Otherwise other segments could be affected, as the sequence numbers interleave.
	po.backlog.gc <- seg.LastSN()

	// We just need any entry from this segment
	pi, ok := po.dispatcher.load(seg.LastSN())
	if !ok {
		logger.Error().
			Int("segId", seg.SegID()).
			Msg("No instance available.")
		return
	}

	// Close the message channel for the segment
	logger.Info().Int("segID", seg.SegID()).Msg("Closing message serializers.")

	pi.priority.stop()
	pi.serializer.stop()
	pi.stopProposing()

	po.setMedianCommitTime(seg)
	logger.Info().Int("segID", seg.SegID()).Int64("commit", int64(po.commitTime)).Msg("Median commit time")

	// Delete the pbftInstance for the segment
	for _, sn := range seg.SNs() {
		po.dispatcher.delete(sn)
	}
}

func (po *PbftOrderer) Sign(data []byte) ([]byte, error) {
	// TODO
	return nil, nil
}

func (po *PbftOrderer) CheckSig(data []byte, senderID int32, signature []byte) error {
	// TODO
	return nil
}

func (po *PbftOrderer) setMedianCommitTime(seg manager.Segment) {
	commits := make([]time.Duration, 0, 0)
	for _, sn := range seg.SNs() {
		duration := log.GetEntry(sn).CommitTs - log.GetEntry(sn).ProposeTs
		logger.Info().Int32("sn", sn).Int64("commitTs", log.GetEntry(sn).CommitTs).Int64("proposeTs", log.GetEntry(sn).ProposeTs).Int64("duration", duration).Msg("Statistics")
		commits = append(commits, time.Duration(duration) * time.Nanosecond)
	}
	sort.Slice(commits, func(i, j int) bool { return commits[i] < commits[j] })
	po.commitTime = commits[len(commits)/2]
}