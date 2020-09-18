/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"
	"math"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
)

var zeroPtr *uint64

func init() {
	var zero uint64
	zeroPtr = &zero
}

type readyEntry struct {
	next        *readyEntry
	clientReqNo *clientReqNo
}

type clientTracker struct {
	clients       map[uint64]*client
	clientIDs     []uint64
	networkConfig *pb.NetworkState_Config
	msgBuffers    map[nodeID]*msgBuffer
	logger        Logger
	readyHead     *readyEntry
	readyTail     *readyEntry
	correctList   *list.List // A list of requests which have f+1 ACKs and the requestData
	myConfig      *pb.StateEvent_InitialParameters
}

func newClientWindows(persisted *persisted, myConfig *pb.StateEvent_InitialParameters, logger Logger) *clientTracker {
	maxSeq := uint64(math.MaxUint64)
	readyAnchor := &readyEntry{
		clientReqNo: &clientReqNo{
			// An entry that will never be garbage collected,
			// to anchor the list
			committed: &maxSeq,
		},
	}

	ct := &clientTracker{
		logger:      logger,
		clients:     map[uint64]*client{},
		correctList: list.New(),
		readyHead:   readyAnchor,
		readyTail:   readyAnchor,
		myConfig:    myConfig,
		msgBuffers:  map[nodeID]*msgBuffer{},
	}

	batches := map[string][]*pb.RequestAck{}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			// Note, we're guaranteed to see this first
			if ct.networkConfig == nil {
				ct.networkConfig = d.CEntry.NetworkState.Config

				for _, clientState := range d.CEntry.NetworkState.Clients {
					client := newClient(clientState, d.CEntry.NetworkState.Config, logger)
					ct.insert(clientState.Id, client)
				}
			}

			// TODO, handle new clients added at checkpoints
		case *pb.Persistent_QEntry:
			batches[string(d.QEntry.Digest)] = d.QEntry.Requests
		case *pb.Persistent_PEntry:
			batch, ok := batches[string(d.PEntry.Digest)]
			if !ok {
				panic("dev sanity test")
			}

			for _, request := range batch {
				ct.clients[request.ClientId].ack(nodeID(myConfig.Id), request)
			}
		}
	}

	for _, clientID := range ct.clientIDs {
		ct.advanceReady(ct.clients[clientID])
	}

	for _, id := range ct.networkConfig.Nodes {
		ct.msgBuffers[nodeID(id)] = newMsgBuffer(myConfig, logger)
	}

	return ct
}

func (ct *clientTracker) filter(msg *pb.Msg) applyable {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, prevent ack spam of multiple msg digests from the same node
		ack := innerMsg.RequestAck
		client, ok := ct.client(ack.ClientId)
		if !ok {
			return future
		}
		switch {
		case client.lowWatermark > ack.ReqNo:
			return past
		case client.lowWatermark+client.width < ack.ReqNo:
			return future
		default:
			return current
		}
	case *pb.Msg_FetchRequest:
		return current // TODO decide if this is actually current
	case *pb.Msg_ForwardRequest:
		requestAck := innerMsg.ForwardRequest.RequestAck
		client, ok := ct.client(requestAck.ClientId)
		if !ok {
			return future
		}
		switch {
		case client.lowWatermark > requestAck.ReqNo:
			return past
		case client.lowWatermark+client.width < requestAck.ReqNo:
			return future
		default:
			return current
		}
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

func (ct *clientTracker) step(source nodeID, msg *pb.Msg) *Actions {
	switch ct.filter(msg) {
	case past:
		// discard
		return &Actions{}
	case future:
		ct.msgBuffers[source].store(msg)
		return &Actions{}
	}

	// current
	return ct.applyMsg(source, msg)
}

func (ct *clientTracker) applyMsg(source nodeID, msg *pb.Msg) *Actions {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, make sure nodeMsgs ignores this if client is not defined
		ct.ack(source, innerMsg.RequestAck)
		return &Actions{}
	case *pb.Msg_FetchRequest:
		msg := innerMsg.FetchRequest
		return ct.replyFetchRequest(source, msg.ClientId, msg.ReqNo, msg.Digest)
	case *pb.Msg_ForwardRequest:
		if source == nodeID(ct.myConfig.Id) {
			// We've already pre-processed this
			// TODO, once we implement unicasting to only those
			// who don't know this should go away.
			return &Actions{}
		}
		return ct.applyForwardRequest(source, innerMsg.ForwardRequest)
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

func (ct *clientTracker) applyRequestDigest(ack *pb.RequestAck, data []byte) *Actions {
	client, ok := ct.clients[ack.ClientId]
	if !ok {
		// Unusual, client must have been removed since we processed the request
		return &Actions{}
	}

	if !client.inWatermarks(ack.ReqNo) {
		// We've already committed this reqno
		return &Actions{}
	}

	return client.reqNo(ack.ReqNo).applyRequestDigest(ack, data)
}

// Note, this seems like it should take a seqNo as a parameter, but since the garbage
// collection is controlled by the state machine, and it will always invoke this method
// before any seqNos are committed beyond the checkpoint, we are safe.
func (ct *clientTracker) clientConfigs() []*pb.NetworkState_Client {
	clients := make([]*pb.NetworkState_Client, len(ct.clientIDs))
	for i, clientID := range ct.clientIDs {
		cw, ok := ct.clients[clientID]
		if !ok {
			panic("dev sanity test")
		}

		var firstUncommitted, lastCommitted *uint64

		for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
			crn := el.Value.(*clientReqNo)
			if crn.committed != nil {
				lastCommitted = &crn.reqNo
				continue
			}
			if firstUncommitted == nil {
				firstUncommitted = &crn.reqNo
			}
		}

		if firstUncommitted == nil {
			if *lastCommitted != cw.lowWatermark+cw.width {
				panic("dev sanity test, if no client reqs are uncommitted, then all though the high watermark should be committed")
			}

			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.width),
				LowWatermark: *lastCommitted + 1,
			}
			continue
		}

		if lastCommitted == nil {
			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.width),
				LowWatermark: *firstUncommitted,
			}
			continue
		}

		mask := bitmask(make([]byte, int(*lastCommitted-*firstUncommitted)/8+1))
		for i := 0; i < int(*lastCommitted-*firstUncommitted); i++ {
			reqNo := *firstUncommitted + uint64(i)
			if cw.reqNoMap[reqNo].Value.(*clientReqNo).committed == nil {
				continue
			}

			if i == 0 {
				panic("dev sanity test, if this is the first uncommitted, how is it committed")
			}

			mask.setBit(i)

		}

		clients[i] = &pb.NetworkState_Client{
			Id:            clientID,
			Width:         uint32(cw.width),
			LowWatermark:  *firstUncommitted,
			CommittedMask: mask,
		}

	}

	return clients
}

func (ct *clientTracker) replyFetchRequest(source nodeID, clientID, reqNo uint64, digest []byte) *Actions {
	cw, ok := ct.client(clientID)
	if !ok {
		return &Actions{}
	}

	if !cw.inWatermarks(reqNo) {
		return &Actions{}
	}

	creq := cw.reqNo(reqNo)
	data, ok := creq.requests[string(digest)]
	if !ok {
		return &Actions{}
	}

	if _, ok := data.agreements[nodeID(ct.myConfig.Id)]; !ok {
		return &Actions{}
	}

	return (&Actions{}).forwardRequest(
		[]uint64{uint64(source)},
		&pb.RequestAck{
			ClientId: clientID,
			ReqNo:    reqNo,
			Digest:   digest,
		},
	)
}

func (ct *clientTracker) applyForwardRequest(source nodeID, msg *pb.ForwardRequest) *Actions {
	cw, ok := ct.client(msg.RequestAck.ClientId)
	if !ok {
		// TODO log oddity
		return &Actions{}
	}

	// TODO, make sure that we only allow one vote per replica for a reqno, or bounded
	cr := cw.reqNo(msg.RequestAck.ReqNo)
	req, ok := cr.requests[string(msg.RequestAck.Digest)]
	if !ok {
		return &Actions{}
	}

	if _, ok := req.agreements[nodeID(ct.myConfig.Id)]; !ok {
		return &Actions{}
	}

	req.agreements[source] = struct{}{}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: [][]byte{
					uint64ToBytes(msg.RequestAck.ClientId),
					uint64ToBytes(msg.RequestAck.ReqNo),
					msg.RequestData,
				},
				Origin: &pb.HashResult{
					Type: &pb.HashResult_VerifyRequest_{
						VerifyRequest: &pb.HashResult_VerifyRequest{
							Source:      uint64(source),
							RequestAck:  msg.RequestAck,
							RequestData: msg.RequestData,
						},
					},
				},
			},
		},
	}
}

func (ct *clientTracker) ack(source nodeID, ack *pb.RequestAck) *clientRequest {
	cw, ok := ct.clients[ack.ClientId]
	if !ok {
		panic("dev sanity test")
	}

	clientRequest, clientReqNo, newlyCorrectReq := cw.ack(source, ack)

	if newlyCorrectReq {
		ct.correctList.PushBack(ack)
	}

	ct.checkReady(cw, clientReqNo)

	return clientRequest
}

func (ct *clientTracker) checkReady(client *client, ocrn *clientReqNo) {
	if ocrn.reqNo != client.nextReadyMark {
		return
	}

	if len(ocrn.strongRequests) == 0 {
		return
	}

	for digest := range ocrn.strongRequests {
		if _, ok := ocrn.myRequests[digest]; ok {
			ct.advanceReady(client)
			return
		}
	}

}

func (ct *clientTracker) advanceReady(client *client) {
	for i := client.nextReadyMark; i <= client.lowWatermark+client.width; i++ {
		if i != client.nextReadyMark {
			// last time through the loop, we must not have updated the ready mark
			return
		}

		crne, ok := client.reqNoMap[i]
		if !ok {
			panic(fmt.Sprintf("dev sanity test: no mapping from reqNo %d", i))
		}

		crn := crne.Value.(*clientReqNo)

		for digest := range crn.strongRequests {
			if _, ok := crn.myRequests[digest]; !ok {
				continue
			}

			newReadyEntry := &readyEntry{
				clientReqNo: crn,
			}

			ct.readyTail.next = newReadyEntry
			ct.readyTail = newReadyEntry

			client.nextReadyMark = i + 1

			break
		}
	}
}

func (ct *clientTracker) garbageCollect(seqNo uint64) {
	for _, id := range ct.clientIDs {
		ct.clients[id].garbageCollect(seqNo)
	}

	// TODO, gc the correctList
	el := ct.readyHead
	for el.next != nil {
		nextEl := el.next

		c := nextEl.clientReqNo.committed
		if c == nil || *c > seqNo {
			el = nextEl
			continue
		}

		if nextEl.next == nil {
			// do not garbage collect the tail of the log
			break
		}

		el.next = &readyEntry{
			clientReqNo: nextEl.next.clientReqNo,
			next:        nextEl.next.next,
		}
	}

	for _, id := range ct.networkConfig.Nodes {
		msgBuffer := ct.msgBuffers[nodeID(id)]
		for {
			// TODO, really inefficient
			msg := msgBuffer.next(ct.filter)
			if msg == nil {
				break
			}
			ct.applyMsg(nodeID(id), msg)
		}
	}
}

func (ct *clientTracker) client(clientID uint64) (*client, bool) {
	// TODO, we could do lazy initialization here
	cw, ok := ct.clients[clientID]
	return cw, ok
}

func (ct *clientTracker) insert(clientID uint64, cw *client) {
	ct.clients[clientID] = cw
	ct.clientIDs = append(ct.clientIDs, clientID)
	sort.Slice(ct.clientIDs, func(i, j int) bool {
		return ct.clientIDs[i] < ct.clientIDs[j]
	})
}

// clientReqNo accumulates acks for this request number
// and attempts to determine which ack is correct, and if the request
// we have is correct.  A replica only ever acks requests it knows to be correct,
// and has the data for.  This means it was injected locally via the Propose API,
// or, a weak quorum exists and something triggered a fetch of the request such
// as an epoch change, or tick based reconcilliation logic.  Because in both the epoch
// change, or reconcilliation paths at least some correct replica validated the
// request, we know the request is correct.  Once we observe two valid requests
// we know that the client is behaving in a byzantine way, and will allow a null
// request to be substituted.  Further, the replica will not ack any request other
// than the null request once two correct requests are observed.
// Additionally, a client may inject a null request via the propose API when attempting
// to recover from a crash without persistence, which will also cause other acks to cease.
// A correct replica will never ack two different non-null requests.  We therefore
// track which replicas have already acked a non-null request and ignore any further
// non-null acks.
type clientReqNo struct {
	networkConfig  *pb.NetworkState_Config
	clientID       uint64
	reqNo          uint64
	nonNullVoters  map[nodeID]struct{}
	requests       map[string]*clientRequest // all requests, correct or not we've observed
	weakRequests   map[string]*clientRequest // all correct requests we have observed
	strongRequests map[string]*clientRequest // strongly correct requests (at most 1 null, 1 non-null)
	myRequests     map[string]*clientRequest // requests we have persisted
	committed      *uint64
}

func (crn *clientReqNo) clientReq(ack *pb.RequestAck) *clientRequest {
	var digestKey string
	if len(ack.Digest) == 0 {
		digestKey = ""
	} else {
		digestKey = string(ack.Digest)
	}

	clientReq, ok := crn.requests[digestKey]
	if !ok {
		clientReq = &clientRequest{
			ack:        ack,
			agreements: map[nodeID]struct{}{},
		}
		crn.requests[digestKey] = clientReq
	}

	return clientReq
}

func (crn *clientReqNo) applyRequestDigest(ack *pb.RequestAck, data []byte) *Actions {
	_, ok := crn.myRequests[string(ack.Digest)]
	if ok {
		// We have already persisted this request, likely
		// a race between a forward and a local proposal, do nothing
		return &Actions{}
	}

	clientReq := crn.clientReq(ack)

	crn.myRequests[string(ack.Digest)] = clientReq

	actions := (&Actions{}).storeRequest(
		&pb.ForwardRequest{
			RequestAck:  ack,
			RequestData: data,
		},
	)

	if len(crn.myRequests) == 1 {
		return actions.send(
			crn.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_RequestAck{
					RequestAck: ack,
				},
			},
		)
	}

	// More than one request persisted
	if _, ok := crn.myRequests[""]; !ok {
		// already persisted and acked null request
		return actions
	}

	nullAck := &pb.RequestAck{
		ClientId: crn.clientID,
		ReqNo:    crn.reqNo,
	}

	nullReq := crn.clientReq(nullAck)
	crn.myRequests[""] = nullReq

	return actions.send(
		crn.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_RequestAck{
				RequestAck: nullAck,
			},
		},
	).storeRequest(
		&pb.ForwardRequest{
			RequestAck: nullAck,
		},
	)
}

func (crn *clientReqNo) applyRequestAck(source nodeID, ack *pb.RequestAck) *Actions {
	if len(ack.Digest) != 0 {
		_, ok := crn.nonNullVoters[source]
		if !ok {
			return &Actions{}
		}

		crn.nonNullVoters[source] = struct{}{}
	}

	clientReq := crn.clientReq(ack)
	clientReq.agreements[source] = struct{}{}

	if len(clientReq.agreements) < someCorrectQuorum(crn.networkConfig) {
		return &Actions{}
	}

	crn.weakRequests[string(ack.Digest)] = clientReq

	if len(clientReq.agreements) < intersectionQuorum(crn.networkConfig) {
		return &Actions{}
	}

	crn.strongRequests[string(ack.Digest)] = clientReq

	return &Actions{}
}

type clientRequest struct {
	ack              *pb.RequestAck
	agreements       map[nodeID]struct{}
	pendingStoreData []byte
}

type client struct {
	clientID      uint64
	nextReadyMark uint64
	lowWatermark  uint64
	width         uint64
	reqNoList     *list.List
	reqNoMap      map[uint64]*list.Element
	clientWaiter  *clientWaiter // Used to throttle clients
	logger        Logger
	networkConfig *pb.NetworkState_Config
}

type clientWaiter struct {
	lowWatermark  uint64
	highWatermark uint64
	expired       chan struct{}
}

func newClient(clientState *pb.NetworkState_Client, networkConfig *pb.NetworkState_Config, logger Logger) *client {
	width := uint64(clientState.Width)
	lowWatermark := clientState.LowWatermark
	highWatermark := clientState.LowWatermark + width

	cw := &client{
		clientID:      clientState.Id,
		logger:        logger,
		networkConfig: networkConfig,
		lowWatermark:  lowWatermark,
		nextReadyMark: lowWatermark,
		width:         width,
		reqNoList:     list.New(),
		reqNoMap:      map[uint64]*list.Element{},
		clientWaiter: &clientWaiter{
			lowWatermark:  lowWatermark,
			highWatermark: highWatermark,
			expired:       make(chan struct{}),
		},
	}

	bm := bitmask(clientState.CommittedMask)
	for i := 0; i <= int(clientState.Width); i++ {
		var committed *uint64
		if bm.isBitSet(i) {
			committed = zeroPtr
		}

		reqNo := uint64(i) + clientState.LowWatermark

		el := cw.reqNoList.PushBack(&clientReqNo{
			networkConfig:  networkConfig,
			clientID:       cw.clientID,
			reqNo:          reqNo,
			requests:       map[string]*clientRequest{},
			weakRequests:   map[string]*clientRequest{},
			strongRequests: map[string]*clientRequest{},
			myRequests:     map[string]*clientRequest{},
			nonNullVoters:  map[nodeID]struct{}{},
			committed:      committed,
		})
		cw.reqNoMap[reqNo] = el
	}

	return cw
}

func (cw *client) garbageCollect(maxSeqNo uint64) {
	removed := uint64(0)

	for el := cw.reqNoList.Front(); el != nil; {
		crn := el.Value.(*clientReqNo)
		if crn.committed == nil || *crn.committed > maxSeqNo {
			break
		}

		oel := el
		el = el.Next()

		if crn.reqNo >= cw.nextReadyMark {
			// It's possible that a request we never saw as ready commits
			// because it was correct, so advance the ready mark
			cw.nextReadyMark = crn.reqNo
		}

		cw.reqNoList.Remove(oel)
		delete(cw.reqNoMap, crn.reqNo)
		removed++
	}

	for i := uint64(1); i <= removed; i++ {
		reqNo := i + cw.lowWatermark + cw.width
		el := cw.reqNoList.PushBack(&clientReqNo{
			clientID:       cw.clientID,
			networkConfig:  cw.networkConfig,
			reqNo:          reqNo,
			requests:       map[string]*clientRequest{},
			weakRequests:   map[string]*clientRequest{},
			strongRequests: map[string]*clientRequest{},
			myRequests:     map[string]*clientRequest{},
			nonNullVoters:  map[nodeID]struct{}{},
		})
		cw.reqNoMap[reqNo] = el
	}

	cw.lowWatermark += removed

	close(cw.clientWaiter.expired)
	cw.clientWaiter = &clientWaiter{
		lowWatermark:  cw.lowWatermark,
		highWatermark: cw.lowWatermark + cw.width,
		expired:       make(chan struct{}),
	}
}

func (cw *client) ack(source nodeID, ack *pb.RequestAck) (*clientRequest, *clientReqNo, bool) {
	crne, ok := cw.reqNoMap[ack.ReqNo]
	if !ok {
		panic("dev sanity check")
	}

	crn := crne.Value.(*clientReqNo)

	cr := crn.clientReq(ack)
	cr.agreements[source] = struct{}{}

	newlyCorrectReq := false
	if len(cr.agreements) == someCorrectQuorum(cw.networkConfig) {
		newlyCorrectReq = true
		crn.weakRequests[string(ack.Digest)] = cr
	}

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequests[string(ack.Digest)] = cr
	}

	return cr, crn, newlyCorrectReq
}

func (cw *client) inWatermarks(reqNo uint64) bool {
	return reqNo <= cw.lowWatermark+cw.width && reqNo >= cw.lowWatermark
}

func (cw *client) reqNo(reqNo uint64) *clientReqNo {
	return cw.reqNoMap[reqNo].Value.(*clientReqNo)
}

func (cw *client) status() *status.ClientTracker {
	allocated := make([]uint64, cw.reqNoList.Len())
	i := 0
	lastNonZero := 0
	for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
		crn := el.Value.(*clientReqNo)
		if crn.committed != nil {
			allocated[i] = 2 // TODO, actually report the seqno it committed to
			lastNonZero = i
		} else if len(crn.requests) > 0 {
			allocated[i] = 1
			lastNonZero = i
		}
		i++
	}

	return &status.ClientTracker{
		LowWatermark:  cw.lowWatermark,
		HighWatermark: cw.lowWatermark + cw.width,
		Allocated:     allocated[:lastNonZero+1],
	}
}
