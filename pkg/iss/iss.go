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

const (
	RequestNAckTimeout = 10
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

type ISS struct {
	// These should only be set at initialization and remain static
	ownID          t.NodeID
	buckets        []*requestBucket
	bucketOrderers map[int]sbInstance
	logger         logging.Logger

	// These might change from epoch to epoch.
	config   *Config // (config will only be able to change when dynamic reconfiguration is supported.)
	epoch    t.EpochNr
	orderers map[t.SBInstanceID]sbInstance

	// These might change even within an epoch
	missingRequests     map[t.SeqNr]*missingRequestInfo
	missingRequestIndex map[string]*missingRequestInfo
}

func New(ownID t.NodeID, config *Config, logger logging.Logger) (*ISS, error) {

	if err := CheckConfig(config); err != nil {
		return nil, fmt.Errorf("invalid ISS configuration: %w", err)
	}

	// Compute the set of leaders for the first epoch
	leaders := config.LeaderPolicy.Leaders(0)

	// Initialize bucket data structures.
	buckets := make([]*requestBucket, config.NumBuckets, config.NumBuckets)
	for i := 0; i < config.NumBuckets; i++ {
		buckets[i] = newRequestBucket(i)
	}

	// Compute initial bucket assignment
	leaderBuckets := distributeBuckets(buckets, leaders)

	orderers := make(map[t.SBInstanceID]sbInstance)
	bucketOrderers := make(map[int]sbInstance)
	for i, leader := range leaders {

		seg := &segment{
			Leader:     leader,
			Membership: config.Membership,
			SeqNrs:     sequenceNumbers(t.SeqNr(i), t.SeqNr(len(leaders)), config.SegmentLength),
			BucketIDs:  leaderBuckets[leader],
		}

		sbInst := newPbftInstance(
			ownID,
			seg,
			config,
			&sbEventCreator{epoch: 0, instanceID: t.SBInstanceID(i)},
			logging.Decorate(logger, "PBFT: ", "epoch", 0, "instance", i))
		orderers[t.SBInstanceID(i)] = sbInst
		for _, bID := range seg.BucketIDs {
			bucketOrderers[bID] = sbInst
		}
	}
	return &ISS{
		ownID:               ownID,
		config:              config,
		buckets:             buckets,
		bucketOrderers:      bucketOrderers,
		epoch:               0,
		orderers:            orderers,
		missingRequests:     make(map[t.SeqNr]*missingRequestInfo),
		missingRequestIndex: make(map[string]*missingRequestInfo),
		logger:              logger,
	}, nil
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
		TicksUntilNAck: RequestNAckTimeout,
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
