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

type clientWindows struct {
	windows       map[uint64]*clientWindow
	clients       []uint64
	networkConfig *pb.NetworkState_Config
	msgBuffers    map[NodeID]*msgBuffer
	logger        Logger
	readyHead     *readyEntry
	readyTail     *readyEntry
	correctList   *list.List // A list of requests which have f+1 ACKs and the requestData
	myConfig      *pb.StateEvent_InitialParameters
}

func newClientWindows(persisted *persisted, myConfig *pb.StateEvent_InitialParameters, logger Logger) *clientWindows {
	maxSeq := uint64(math.MaxUint64)
	readyAnchor := &readyEntry{
		clientReqNo: &clientReqNo{
			// An entry that will never be garbage collected,
			// to anchor the list
			committed: &maxSeq,
		},
	}

	cws := &clientWindows{
		logger:      logger,
		windows:     map[uint64]*clientWindow{},
		correctList: list.New(),
		readyHead:   readyAnchor,
		readyTail:   readyAnchor,
		myConfig:    myConfig,
		msgBuffers:  map[NodeID]*msgBuffer{},
	}

	batches := map[string][]*pb.ForwardRequest{}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			// Note, we're guaranteed to see this first
			if cws.networkConfig == nil {
				cws.networkConfig = d.CEntry.NetworkState.Config

				for _, client := range d.CEntry.NetworkState.Clients {
					clientWindow := newClientWindow(client, d.CEntry.NetworkState.Config, logger)
					cws.insert(client.Id, clientWindow)
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
				clientReqNo, _ := cws.windows[request.Request.ClientId].allocate(request.Request, request.Digest)
				clientReqNo.strongRequest = clientReqNo.digests[string(request.Digest)]
			}
		}
	}

	for _, clientID := range cws.clients {
		cws.advanceReady(cws.windows[clientID])
	}

	for _, id := range cws.networkConfig.Nodes {
		cws.msgBuffers[NodeID(id)] = newMsgBuffer(myConfig, logger)
	}

	return cws
}

func (cws *clientWindows) filter(msg *pb.Msg) applyable {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, prevent ack spam of multiple msg digests from the same node
		ack := innerMsg.RequestAck
		clientWindow, ok := cws.clientWindow(ack.ClientId)
		if !ok {
			return future
		}
		switch {
		case clientWindow.lowWatermark > ack.ReqNo:
			return past
		case clientWindow.highWatermark < ack.ReqNo:
			return future
		default:
			return current
		}
	case *pb.Msg_FetchRequest:
		return current // TODO decide if this is actually current
	case *pb.Msg_ForwardRequest:
		requestData := innerMsg.ForwardRequest.Request
		clientWindow, ok := cws.clientWindow(requestData.ClientId)
		if !ok {
			return future
		}
		switch {
		case clientWindow.lowWatermark > requestData.ReqNo:
			return past
		case clientWindow.highWatermark < requestData.ReqNo:
			return future
		default:
			return current
		}
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

func (cws *clientWindows) step(source NodeID, msg *pb.Msg) *Actions {
	switch cws.filter(msg) {
	case past:
		// discard
		return &Actions{}
	case future:
		cws.msgBuffers[source].store(msg)
		return &Actions{}
	}

	// current
	return cws.applyMsg(source, msg)
}

func (cws *clientWindows) applyMsg(source NodeID, msg *pb.Msg) *Actions {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, make sure nodeMsgs ignores this if client is not defined
		cws.ack(source, innerMsg.RequestAck)
		return &Actions{}
	case *pb.Msg_FetchRequest:
		msg := innerMsg.FetchRequest
		return cws.replyFetchRequest(source, msg.ClientId, msg.ReqNo, msg.Digest)
	case *pb.Msg_ForwardRequest:
		if source == NodeID(cws.myConfig.Id) {
			// We've already pre-processed this
			// TODO, once we implement unicasting to only those
			// who don't know this should go away.
			return &Actions{}
		}
		return cws.applyForwardRequest(source, innerMsg.ForwardRequest)
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

// Note, this seems like it should take a seqNo as a parameter, but since the garbage
// collection is controlled by the state machine, and it will always invoke this method
// before any seqNos are committed beyond the checkpoint, we are safe.
func (cws *clientWindows) clientConfigs() []*pb.NetworkState_Client {
	clients := make([]*pb.NetworkState_Client, len(cws.clients))
	for i, clientID := range cws.clients {
		cw, ok := cws.windows[clientID]
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
			if *lastCommitted != cw.highWatermark {
				panic("dev sanity test, if no client reqs are uncommitted, then all though the high watermark should be committed")
			}

			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.highWatermark - cw.lowWatermark),
				LowWatermark: *lastCommitted + 1,
			}
			continue
		}

		if lastCommitted == nil {
			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.highWatermark - cw.lowWatermark),
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
			Width:         uint32(cw.highWatermark - cw.lowWatermark),
			LowWatermark:  *firstUncommitted,
			CommittedMask: mask,
		}

	}

	return clients
}

func (cws *clientWindows) replyFetchRequest(source NodeID, clientID, reqNo uint64, digest []byte) *Actions {
	cw, ok := cws.clientWindow(clientID)
	if !ok {
		return &Actions{}
	}

	if !cw.inWatermarks(reqNo) {
		return &Actions{}
	}

	creq := cw.request(reqNo)
	data, ok := creq.digests[string(digest)]
	if !ok {
		return &Actions{}
	}

	if data.data == nil {
		return &Actions{}
	}

	return (&Actions{}).send(
		[]uint64{uint64(source)},
		&pb.Msg{
			Type: &pb.Msg_ForwardRequest{
				ForwardRequest: &pb.ForwardRequest{
					Request: data.data,
					Digest:  digest,
				},
			},
		},
	)
}

func (cws *clientWindows) applyForwardRequest(source NodeID, msg *pb.ForwardRequest) *Actions {
	cw, ok := cws.clientWindow(msg.Request.ClientId)
	if !ok {
		// TODO log oddity
		return &Actions{}
	}

	// TODO, make sure that we only allow one vote per replica for a reqno, or bounded
	cr := cw.request(msg.Request.ReqNo)
	req, ok := cr.digests[string(msg.Digest)]
	if !ok || req.data != nil {
		return &Actions{}
	}

	req.agreements[source] = struct{}{}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: [][]byte{
					uint64ToBytes(msg.Request.ClientId),
					uint64ToBytes(msg.Request.ReqNo),
					msg.Request.Data,
				},
				Origin: &pb.HashResult{
					Type: &pb.HashResult_VerifyRequest_{
						VerifyRequest: &pb.HashResult_VerifyRequest{
							Source:         uint64(source),
							Request:        msg.Request,
							ExpectedDigest: msg.Digest,
						},
					},
				},
			},
		},
	}
}

func (cws *clientWindows) ack(source NodeID, ack *pb.RequestAck) *clientRequest {
	cw, ok := cws.windows[ack.ClientId]
	if !ok {
		panic("dev sanity test")
	}

	clientRequest, clientReqNo, newlyCorrectReq := cw.ack(source, ack.ReqNo, ack.Digest)

	if newlyCorrectReq != nil {
		cws.correctList.PushBack(&pb.ForwardRequest{
			Request: newlyCorrectReq,
			Digest:  ack.Digest,
		})
	}

	cws.checkReady(cw, clientReqNo)

	return clientRequest
}

func (cws *clientWindows) allocate(requestData *pb.Request, digest []byte) {
	cw, ok := cws.windows[requestData.ClientId]
	if !ok {
		panic("dev sanity test")
	}

	if requestData.ReqNo < cw.lowWatermark {
		// This can happen when the primary and the client send us a request
		// concurrently, we can process both in parallel, one returns before
		// the other, and, the watermarks have already moved on
		return
	}

	clientReqNo, newlyCorrectReq := cw.allocate(requestData, digest)

	if newlyCorrectReq != nil {
		cws.correctList.PushBack(&pb.ForwardRequest{
			Request: newlyCorrectReq,
			Digest:  digest,
		})
	}

	cws.checkReady(cw, clientReqNo)
}

func (cws *clientWindows) checkReady(clientWindow *clientWindow, ocrn *clientReqNo) {
	if ocrn.reqNo != clientWindow.nextReadyMark {
		return
	}

	if ocrn.strongRequest == nil {
		return
	}

	if ocrn.strongRequest.data == nil {
		return
	}

	cws.advanceReady(clientWindow)
}

func (cws *clientWindows) advanceReady(clientWindow *clientWindow) {
	for i := clientWindow.nextReadyMark; i <= clientWindow.highWatermark; i++ {
		crne, ok := clientWindow.reqNoMap[i]
		if !ok {
			panic(fmt.Sprintf("dev sanity test: no mapping from reqNo %d", i))
		}

		crn := crne.Value.(*clientReqNo)

		if crn.strongRequest == nil {
			break
		}

		if crn.strongRequest.data == nil {
			break
		}

		newReadyEntry := &readyEntry{
			clientReqNo: crn,
		}

		cws.readyTail.next = newReadyEntry
		cws.readyTail = newReadyEntry

		clientWindow.nextReadyMark = i + 1
	}
}

func (cws *clientWindows) garbageCollect(seqNo uint64) {
	for _, id := range cws.clients {
		cws.windows[id].garbageCollect(seqNo)
	}

	// TODO, gc the correctList
	el := cws.readyHead
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

	for _, nodeID := range cws.networkConfig.Nodes {
		msgBuffer := cws.msgBuffers[NodeID(nodeID)]
		for {
			// TODO, really inefficient
			msg := msgBuffer.next(cws.filter)
			if msg == nil {
				break
			}
			cws.applyMsg(NodeID(nodeID), msg)
		}
	}
}

func (cws *clientWindows) clientWindow(clientID uint64) (*clientWindow, bool) {
	// TODO, we could do lazy initialization here
	cw, ok := cws.windows[clientID]
	return cw, ok
}

func (cws *clientWindows) insert(clientID uint64, cw *clientWindow) {
	cws.windows[clientID] = cw
	cws.clients = append(cws.clients, clientID)
	sort.Slice(cws.clients, func(i, j int) bool {
		return cws.clients[i] < cws.clients[j]
	})
}

type clientReqNo struct {
	clientID      uint64
	reqNo         uint64
	digests       map[string]*clientRequest
	committed     *uint64
	strongRequest *clientRequest
}

type clientRequest struct {
	digest     []byte
	data       *pb.Request
	agreements map[NodeID]struct{}
}

type clientWindow struct {
	clientID      uint64
	nextReadyMark uint64
	lowWatermark  uint64
	highWatermark uint64
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

func newClientWindow(client *pb.NetworkState_Client, networkConfig *pb.NetworkState_Config, logger Logger) *clientWindow {
	lowWatermark := client.LowWatermark
	highWatermark := client.LowWatermark + uint64(client.Width)

	cw := &clientWindow{
		clientID:      client.Id,
		logger:        logger,
		networkConfig: networkConfig,
		lowWatermark:  lowWatermark,
		nextReadyMark: lowWatermark,
		highWatermark: highWatermark,
		reqNoList:     list.New(),
		reqNoMap:      map[uint64]*list.Element{},
		clientWaiter: &clientWaiter{
			lowWatermark:  lowWatermark,
			highWatermark: highWatermark,
			expired:       make(chan struct{}),
		},
	}

	bm := bitmask(client.CommittedMask)
	for i := 0; i <= int(client.Width); i++ {
		var committed *uint64
		if bm.isBitSet(i) {
			committed = zeroPtr
		}

		reqNo := uint64(i) + client.LowWatermark

		el := cw.reqNoList.PushBack(&clientReqNo{
			clientID:  cw.clientID,
			reqNo:     reqNo,
			digests:   map[string]*clientRequest{},
			committed: committed,
		})
		cw.reqNoMap[reqNo] = el
	}

	return cw
}

func (cw *clientWindow) garbageCollect(maxSeqNo uint64) {
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
		reqNo := i + cw.highWatermark
		el := cw.reqNoList.PushBack(&clientReqNo{
			clientID: cw.clientID,
			reqNo:    reqNo,
			digests:  map[string]*clientRequest{},
		})
		cw.reqNoMap[reqNo] = el
	}

	cw.lowWatermark += removed
	cw.highWatermark += removed

	close(cw.clientWaiter.expired)
	cw.clientWaiter = &clientWaiter{
		lowWatermark:  cw.lowWatermark,
		highWatermark: cw.highWatermark,
		expired:       make(chan struct{}),
	}
}

func (cw *clientWindow) ack(source NodeID, reqNo uint64, digest []byte) (*clientRequest, *clientReqNo, *pb.Request) {
	if reqNo > cw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, cw.highWatermark))
	}

	if reqNo < cw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, cw.lowWatermark))
	}

	crne, ok := cw.reqNoMap[reqNo]
	if !ok {
		panic("dev sanity check")
	}

	crn := crne.Value.(*clientReqNo)

	cr, ok := crn.digests[string(digest)]
	if !ok {
		cr = &clientRequest{
			digest:     digest,
			agreements: map[NodeID]struct{}{},
		}
		crn.digests[string(digest)] = cr
	}

	cr.agreements[source] = struct{}{}

	var newlyCorrectReq *pb.Request
	if len(cr.agreements) == someCorrectQuorum(cw.networkConfig) {
		newlyCorrectReq = cr.data
	}

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequest = cr
	}

	return cr, crn, newlyCorrectReq
}

func (cw *clientWindow) allocate(requestData *pb.Request, digest []byte) (*clientReqNo, *pb.Request) {
	reqNo := requestData.ReqNo
	if reqNo > cw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, cw.highWatermark))
	}

	if reqNo < cw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, cw.lowWatermark))
	}

	crne, ok := cw.reqNoMap[reqNo]
	if !ok {
		panic("dev sanity check")
	}

	crn := crne.Value.(*clientReqNo)

	cr, ok := crn.digests[string(digest)]
	if !ok {
		cr = &clientRequest{
			digest:     digest,
			agreements: map[NodeID]struct{}{},
		}
		crn.digests[string(digest)] = cr
	}
	cr.data = requestData

	var newlyCorrectReq *pb.Request
	if len(cr.agreements) >= someCorrectQuorum(cw.networkConfig) {
		newlyCorrectReq = requestData
	}

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequest = cr
	}

	return crn, newlyCorrectReq
}

func (cw *clientWindow) inWatermarks(reqNo uint64) bool {
	return reqNo <= cw.highWatermark && reqNo >= cw.lowWatermark
}

func (cw *clientWindow) request(reqNo uint64) *clientReqNo {
	if reqNo > cw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, cw.highWatermark))
	}

	if reqNo < cw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, cw.lowWatermark))
	}

	return cw.reqNoMap[reqNo].Value.(*clientReqNo)
}

func (cw *clientWindow) status() *status.ClientTracker {
	allocated := make([]uint64, cw.reqNoList.Len())
	i := 0
	lastNonZero := 0
	for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
		crn := el.Value.(*clientReqNo)
		if crn.committed != nil {
			allocated[i] = 2 // TODO, actually report the seqno it committed to
			lastNonZero = i
		} else if len(crn.digests) > 0 {
			allocated[i] = 1
			lastNonZero = i
		}
		i++
	}

	return &status.ClientTracker{
		LowWatermark:  cw.lowWatermark,
		HighWatermark: cw.highWatermark,
		Allocated:     allocated[:lastNonZero+1],
	}
}
