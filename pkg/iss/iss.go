/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package iss

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

type missingRequestInfo struct {
	Sn             t.SeqNr
	Requests       map[string]struct{}
	Orderer        sbInstance
	TicksUntilNAck int // TODO: implement NAcks
}

type segment struct {
	Leader     t.NodeID
	Membership []t.NodeID
	SeqNrs     []t.SeqNr
	BucketIDs  []int
}

type commitLogEntry struct {
	Sn      t.SeqNr
	Batch   *requestpb.Batch
	Digest  []byte
	Aborted bool
	Suspect t.NodeID
}

type ISS struct {
	// These should only be set at initialization and remain static
	ownID   t.NodeID
	buckets []*requestBucket
	logger  logging.Logger

	// These might change from epoch to epoch.
	// Modified only by initEpoch()
	config           *Config // (config will only be able to change when dynamic reconfiguration is supported.)
	epoch            t.EpochNr
	nextSBInstanceID t.SBInstanceID
	orderers         map[t.SBInstanceID]sbInstance
	bucketOrderers   map[int]sbInstance

	// These values are modified throughout an epoch, but NOT by initEpoch
	commitLog           map[t.SeqNr]*commitLogEntry
	nextDeliveredSN     t.SeqNr
	missingRequests     map[t.SeqNr]*missingRequestInfo
	missingRequestIndex map[string]*missingRequestInfo
}

func New(ownID t.NodeID, config *Config, logger logging.Logger) (*ISS, error) {

	if err := CheckConfig(config); err != nil {
		return nil, fmt.Errorf("invalid ISS configuration: %w", err)
	}

	// Initialize bucket data structures.
	buckets := make([]*requestBucket, config.NumBuckets, config.NumBuckets)
	for i := 0; i < config.NumBuckets; i++ {
		buckets[i] = newRequestBucket(i)
	}

	iss := &ISS{
		// Static
		ownID:   ownID,
		buckets: buckets,
		logger:  logger,

		// Modified only by initEpoch
		config:           config,
		epoch:            0,
		nextSBInstanceID: 0,
		orderers:         make(map[t.SBInstanceID]sbInstance),
		bucketOrderers:   nil,

		// Modified throughout an epoch, but NOT by initEpoch
		commitLog:           make(map[t.SeqNr]*commitLogEntry),
		nextDeliveredSN:     0,
		missingRequests:     make(map[t.SeqNr]*missingRequestInfo),
		missingRequestIndex: make(map[string]*missingRequestInfo),
	}

	iss.initEpoch(0)
	return iss, nil
}

func (iss *ISS) ApplyEvent(event *eventpb.Event) *events.EventList {
	switch e := event.Type.(type) {
	case *eventpb.Event_Tick:
		return iss.handleTick()
	case *eventpb.Event_RequestReady:
		return iss.handleRequest(e.RequestReady.RequestRef)
	case *eventpb.Event_MessageReceived:
		// ISS only accepts ISS messages. If another message is applied, processing panics.
		return iss.handleMessage(e.MessageReceived.Msg.Type.(*messagepb.Message_Iss).Iss,
			t.NodeID(e.MessageReceived.From))
	case *eventpb.Event_Iss:
		switch issEvent := e.Iss.Type.(type) {
		case *isspb.ISSEvent_Sb:
			return iss.handleSBEvent(issEvent.Sb)
		default:
			panic(fmt.Sprintf("unknown ISS event type: %T", event.Type))
		}
	default:
		panic(fmt.Sprintf("unknown protocol (ISS) event type: %T", event.Type))
	}
}

func (iss *ISS) Status() (s *statuspb.ProtocolStatus, err error) {
	return nil, nil
}

func (iss *ISS) initEpoch(newEpoch t.EpochNr) {
	// Compute the set of leaders for the first epoch
	leaders := iss.config.LeaderPolicy.Leaders(newEpoch)

	// Compute initial bucket assignment
	leaderBuckets := distributeBuckets(iss.buckets, leaders, newEpoch)

	iss.bucketOrderers = make(map[int]sbInstance)
	for i, leader := range leaders {

		seg := &segment{
			Leader:     leader,
			Membership: iss.config.Membership,
			SeqNrs: sequenceNumbers(
				iss.nextDeliveredSN+t.SeqNr(i),
				t.SeqNr(len(leaders)),
				iss.config.SegmentLength),
			BucketIDs: leaderBuckets[leader],
		}

		sbInst := newPbftInstance(
			iss.ownID,
			seg,
			iss.config,
			&sbEventCreator{epoch: newEpoch, instanceID: iss.nextSBInstanceID},
			logging.Decorate(iss.logger, "PBFT: ", "epoch", newEpoch, "instance", iss.nextSBInstanceID))
		iss.orderers[iss.nextSBInstanceID] = sbInst
		iss.nextSBInstanceID++
		for _, bID := range seg.BucketIDs {
			iss.bucketOrderers[bID] = sbInst
		}
	}

	iss.epoch = newEpoch
}

func (iss *ISS) epochFinished() bool {

	// TODO: Instead of checking all sequence numbers every time,
	//       remember the last sequence number of the epoch and compare it to iss.nextDeliveredSN
	for _, orderer := range iss.orderers {
		for _, sn := range orderer.Segment().SeqNrs {
			if iss.commitLog[sn] == nil {
				return false
			}
		}
	}
	return true
}

func (iss *ISS) handleRequest(ref *requestpb.RequestRef) *events.EventList {
	eventsOut := &events.EventList{}

	iss.logger.Log(logging.LevelDebug, "Handling request.")

	// Compute bucket ID for the new request.
	bID := bucketId(ref, iss.config.NumBuckets)

	// Add request to its bucket. TODO: Maybe move this to the SB instance?
	iss.buckets[bID].Add(ref)

	// If some orderer instance is waiting for this request, notify that orderer.
	reqKey := reqStrKey(ref)
	if missingRequests, ok := iss.missingRequestIndex[reqKey]; ok {
		delete(missingRequests.Requests, reqKey)
		delete(iss.missingRequestIndex, reqKey)
		if len(missingRequests.Requests) == 0 {
			delete(iss.missingRequests, missingRequests.Sn)
			eventsOut.PushBackList(missingRequests.Orderer.ApplyEvent(SBRequestsReady(missingRequests.Sn)))
		}
	}

	// Count number of requests in all the buckets assigned to the same instance as the bucket of the received request.
	// These are all the requests pending to be proposed by the instance.
	// TODO: This is extremely inefficient (done on every request reception).
	//       Maintain a counter of requests in assigned buckets instead.
	pendingRequests := t.NumRequests(0)
	for _, b := range iss.bucketOrderers[bID].Segment().BucketIDs {
		pendingRequests += t.NumRequests(iss.buckets[b].Len())
	}

	// Announce the total number of pending requests to the corresponding SB instance.
	return eventsOut.PushBackList(iss.bucketOrderers[bID].ApplyEvent(SBPendingRequestsEvent(pendingRequests)))
}

func (iss *ISS) handleMessage(message *isspb.ISSMessage, from t.NodeID) *events.EventList {
	switch msg := message.Type.(type) {
	case *isspb.ISSMessage_Sb:
		return iss.handleSBMessage(msg.Sb, from)
	default:
		panic(fmt.Errorf("unknown ISS message type: %T", message.Type))
	}
}

func (iss *ISS) handleTick() *events.EventList {
	eventsOut := &events.EventList{}

	// Relay tick to each orderer.
	sbTick := SBTickEvent()
	for _, orderer := range iss.orderers {
		eventsOut.PushBackList(orderer.ApplyEvent(sbTick))
	}

	// TODO: Check if request NAcks need to be sent.

	return eventsOut
}

func (iss *ISS) cutBatch(instanceID t.SBInstanceID, maxBatchSize t.NumRequests) *events.EventList {
	orderer := iss.orderers[instanceID]
	bucketIDs := orderer.Segment().BucketIDs
	buckets := make([]*requestBucket, len(bucketIDs))
	for i, bID := range bucketIDs {
		buckets[i] = iss.buckets[bID]
	}

	batch := cutBatch(buckets, int(maxBatchSize))

	requestsLeft := t.NumRequests(0)
	for _, b := range buckets {
		requestsLeft += t.NumRequests(b.Len())
	}

	return orderer.ApplyEvent(SBBatchReadyEvent(batch, requestsLeft))
}

func (iss *ISS) waitForRequests(instanceID t.SBInstanceID, sn t.SeqNr, requests []*requestpb.RequestRef) *events.EventList {

	missingReqs := &missingRequestInfo{
		Sn:             sn,
		Requests:       make(map[string]struct{}, 0),
		Orderer:        iss.orderers[instanceID],
		TicksUntilNAck: iss.config.RequestNAckTimeout,
	}

	for _, reqRef := range requests {
		if !iss.buckets[bucketId(reqRef, iss.config.NumBuckets)].Contains(reqRef) {
			reqKey := reqStrKey(reqRef)
			missingReqs.Requests[reqKey] = struct{}{}
			iss.missingRequestIndex[reqKey] = missingReqs
		}
	}

	if len(missingReqs.Requests) > 0 {
		iss.missingRequests[sn] = missingReqs
		return &events.EventList{}
	} else {
		return missingReqs.Orderer.ApplyEvent(SBRequestsReady(sn))
	}
}

func (iss *ISS) deliverCommitted() *events.EventList {
	eventsOut := &events.EventList{}

	for iss.commitLog[iss.nextDeliveredSN] != nil {
		eventsOut.PushBack(events.Deliver(iss.nextDeliveredSN, iss.commitLog[iss.nextDeliveredSN].Batch))
		iss.nextDeliveredSN++
	}

	if iss.epochFinished() {
		iss.initEpoch(iss.epoch + 1)
	}

	return eventsOut
}

func (iss *ISS) removeFromBuckets(requests []*requestpb.RequestRef) {
	for _, reqRef := range requests {
		iss.buckets[bucketId(reqRef, len(iss.buckets))].Remove(reqRef)
	}
}

func sequenceNumbers(start t.SeqNr, step t.SeqNr, length int) []t.SeqNr {
	sns := make([]t.SeqNr, length)
	for i, nextsn := 0, start; i < length; i, nextsn = i+1, nextsn+step {
		sns[i] = nextsn
	}
	return sns
}

// Takes a request reference and transforms it to a string for using as a map key.
func reqStrKey(reqRef *requestpb.RequestRef) string {
	return fmt.Sprintf("%d-%d.%v", reqRef.ClientId, reqRef.ReqNo, reqRef.Digest)
}
