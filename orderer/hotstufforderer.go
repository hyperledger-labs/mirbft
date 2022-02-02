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
    "github.com/hyperledger-labs/mirbft/crypto"
    "github.com/hyperledger-labs/mirbft/membership"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// Represents a HotStuff Orderer implementation.
type HotStuffOrderer struct {
	hotStuffInstances map[int]*hotStuffInstance // HotStuff instance per segment
	segmentChan       chan manager.Segment      // Channel to which the Manager pushes new Segments.
	dispatcher        hotStuffDispatcher        // map[int32]*hotStuffInstance
	backlog           backlog                   // map[int32]chan*ordererMsg
	last              int32                     // Some sequence number we can ignore backlogMsgs above
	lock              sync.Mutex
}

type hotStuffDispatcher struct {
	mm sync.Map
}

func (d *hotStuffDispatcher) load(key int32) (*hotStuffInstance, bool) {
	if v, ok := d.mm.Load(key); ok {
		return v.(*hotStuffInstance), ok
	}
	return nil, false
}

func (d *hotStuffDispatcher) store(key int32, value *hotStuffInstance) {
	d.mm.Store(key, value)
}

func (d *hotStuffDispatcher) delete(key int32) {
	d.mm.Delete(key)
}

// HandleMessage is called by the messenger each time an Orderer-issued message is received over the network.
func (ho *HotStuffOrderer) HandleMessage(msg *pb.ProtocolMessage) {
	logger.Trace().
		Int32("sn", msg.Sn).
		Int32("senderID", msg.SenderId).
		Msg("Handling message.")

	sn := msg.Sn

	// Check if message is from an old segment and needs to be discarded
	last := atomic.LoadInt32(&ho.last)
	if sn <= last {
		logger.Info().
			Int32("sn", sn).
			Int32("senderID", msg.SenderId).
			Msg("Hotstuff orderer discards message. Message belongs to an old segment.")
	}

	// Check if the message is for a future message and needs to be backlogged
	hi, ok := ho.dispatcher.load(sn)
	if !ok {
		//logger.Info().
		//	Int32("sn", sn).
		//	Int32("senderID", msg.SenderId).
		//	Msgf("HotStuff cannot handle message. No segment is available. Adding message to backlog")
		// Add message to backlog
		ho.backlog.add(msg)
		return
	}

	hi.serializer.serialize(msg)

}

func (ho *HotStuffOrderer) HandleEntry(entry *log.Entry) {
	// Treat the log entry as a MissingEntry message
	// and process it using the instance according to its sequence number.
	ho.HandleMessage(&pb.ProtocolMessage{
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

func (ho *HotStuffOrderer) Init(mngr manager.Manager) {
	ho.segmentChan = mngr.SubscribeOrderer()
	ho.backlog = newBacklog()
	ho.lock = sync.Mutex{}
	ho.last = -1
}

// Starts the HotStuff orderer. Listens on the channel where the Manager issues new Segemnts and starts a goroutine to
// handle each of them.
// Meant to be run as a separate goroutine.
// Decrements the provided wait group when done.
func (ho *HotStuffOrderer) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for s, ok := <-ho.segmentChan; ok; s, ok = <-ho.segmentChan {
		logger.Info().
			Int("segId", s.SegID()).
			Int32("length", s.Len()).
			Int32("firstSN", s.FirstSN()).
			Int32("lastSN", s.LastSN()).
			Int32("first leader", s.Leaders()[0]).
			Msgf("HotStuff Orderer received new a segment: %+v", s.SNs())

		ho.runSegment(s)
		go ho.killSegment(s)
	}
}

// Starts the HotStuff ordering algorithm for a Segment.
func (ho *HotStuffOrderer) runSegment(seg manager.Segment) {
	hi := &hotStuffInstance{}
	hi.init(seg, ho)
	for _, sn := range seg.SNs() {
		ho.dispatcher.store(sn, hi)
	}
	logger.Info().Int("segID", seg.SegID()).
		Int32("first", seg.FirstSN()).
		Int32("last", seg.LastSN()).
		Msg("Starting HotStuff instance.")

	hi.subscribeToBacklog()

	go hi.start()
	go hi.processSerializedMessages()
}

func (ho *HotStuffOrderer) killSegment(seg manager.Segment) {
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
	ho.lock.Lock()
	if seg.LastSN() > ho.last {
		ho.last = seg.LastSN()
	}
	ho.lock.Unlock()

	epochLength := int32(config.Config.EpochLength)
	if seg.LastSN()%epochLength == epochLength-1 {
		ho.backlog.gc <- seg.LastSN()
	}

	// We just need any entry from this segment
	hi, ok := ho.dispatcher.load(seg.LastSN())
	if !ok {
		logger.Error().
			Int("segId", seg.SegID()).
			Msg("No instance available.")
		return
	}

	// Close the message channel for the segment
	logger.Info().Int("segID", seg.SegID()).Msg("Closing message serializers.")

	hi.serializer.stop()
	hi.stopProposing()

	// Delete the pbftInstance for the segment
	for _, sn := range seg.SNs() {
		ho.dispatcher.delete(sn)
	}

}

// Sign generates a signature share.
func (ho *HotStuffOrderer) Sign(data []byte) ([]byte, error) {
	//return nil, nil
	return crypto.TBLSSigShare(membership.TBLSPrivKeyShare, data)
}

// CheckSig checks if a signature share is valid.
func (ho *HotStuffOrderer) CheckSig(data []byte, senderID int32, signature []byte) error {
	//return nil
	return crypto.TBLSSigShareVerification(membership.TBLSPublicKey, data, signature)
}

// CheckCert checks if the certificate is a valid threshold signature
func (ho *HotStuffOrderer) CheckCert(data []byte, signature []byte) error {
	//return nil
	return crypto.TBLSVerifySingature(membership.TBLSPublicKey, data, signature)
}

// AssembleCert combines validates signature shares in a threshold signature.
// A threshold signature form the quorum certificate.
func (ho *HotStuffOrderer) AssembleCert(data []byte, signatures [][]byte) ([]byte, error) {
	//return nil, nil
	return crypto.TBLSRecoverSignature(membership.TBLSPublicKey, data, signatures, 2*membership.Faults()+1, membership.NumNodes())
}
