/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type clientWindows struct {
	windows       map[uint64]*clientWindow
	clients       []uint64
	networkConfig *pb.NetworkState_Config
	logger        Logger
	readyList     *list.List
	readyMap      map[*clientReqNo]*list.Element
	correctList   *list.List // A list of requests which have f+1 ACKs and the requestData
}

func newClientWindows(persisted *persisted, logger Logger) *clientWindows {
	cws := &clientWindows{
		logger:      logger,
		windows:     map[uint64]*clientWindow{},
		readyList:   list.New(),
		readyMap:    map[*clientReqNo]*list.Element{},
		correctList: list.New(),
	}

	clientWindowWidth := uint64(100) // XXX this should be configurable

	batches := map[string][]*pb.ForwardRequest{}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			// Note, we're guaranteed to see this first
			if cws.networkConfig == nil {
				cws.networkConfig = d.CEntry.NetworkState.Config

				for _, client := range d.CEntry.NetworkState.Clients {
					lowWatermark := client.BucketLowWatermarks[0]
					for _, blw := range client.BucketLowWatermarks {
						if blw < lowWatermark {
							lowWatermark = blw
						}
					}

					clientWindow := newClientWindow(client.Id, lowWatermark, lowWatermark+clientWindowWidth, d.CEntry.NetworkState.Config, logger)
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

	return cws
}

func (cws *clientWindows) clientConfigs() []*pb.NetworkState_Client { // XXX I think this needs to take a seqno?
	clients := make([]*pb.NetworkState_Client, len(cws.clients))
	for i, clientID := range cws.clients {
		blws := make([]uint64, cws.networkConfig.NumberOfBuckets)
		cw, ok := cws.windows[clientID]
		if !ok {
			panic("dev sanity test")
		}
		for i := range blws {
			firstOutOfWindowBucket := (cw.highWatermark + 1 + clientID) % uint64(len(blws))
			blws[int((firstOutOfWindowBucket+uint64(i))%uint64(len(blws)))] = cw.highWatermark + 1 + uint64(i)
		}
		for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
			crn := el.Value.(*clientReqNo)
			if crn.committed != nil {
				continue
			}

			bucket := int((crn.reqNo + crn.clientID) % uint64(cws.networkConfig.NumberOfBuckets))
			if blws[bucket] <= crn.reqNo {
				continue
			}

			blws[bucket] = crn.reqNo
		}

		clients[i] = &pb.NetworkState_Client{
			Id:                  clientID,
			BucketLowWatermarks: blws,
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

func (cws *clientWindows) ack(source NodeID, ack *pb.RequestAck) {
	cw, ok := cws.windows[ack.ClientId]
	if !ok {
		panic("dev sanity test")
	}

	clientReqNo, newlyCorrectReq := cw.ack(source, ack.ReqNo, ack.Digest)

	if newlyCorrectReq != nil {
		cws.correctList.PushBack(&pb.ForwardRequest{
			Request: newlyCorrectReq,
			Digest:  ack.Digest,
		})
	}

	cws.checkReady(cw, clientReqNo)
}

func (cws *clientWindows) allocate(requestData *pb.Request, digest []byte) {
	cw, ok := cws.windows[requestData.ClientId]
	if !ok {
		panic("dev sanity test")
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
			panic("dev sanity test")
		}

		crn := crne.Value.(*clientReqNo)

		if crn.strongRequest == nil {
			break
		}

		el := cws.readyList.PushBack(crn)
		cws.readyMap[crn] = el
		clientWindow.nextReadyMark = i + 1
	}
}

func (cws *clientWindows) garbageCollect(seqNo uint64) {
	for _, id := range cws.clients {
		cws.windows[id].garbageCollect(seqNo)
	}

	// TODO, gc the correctList

	for el := cws.readyList.Front(); el != nil; {
		oel := el
		el = el.Next()

		c := oel.Value.(*clientReqNo).committed
		if c == nil || *c > seqNo {
			continue
		}

		cws.readyList.Remove(oel)
		delete(cws.readyMap, oel.Value.(*clientReqNo))
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

func newClientWindow(clientID, lowWatermark, highWatermark uint64, networkConfig *pb.NetworkState_Config, logger Logger) *clientWindow {
	cw := &clientWindow{
		clientID:      clientID,
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

	cw.garbageCollect(0)

	return cw
}

func (cw *clientWindow) garbageCollect(maxSeqNo uint64) {
	logWidth := cw.highWatermark - cw.lowWatermark

	for el := cw.reqNoList.Front(); el != nil; {
		crn := el.Value.(*clientReqNo)
		if crn.committed == nil || *crn.committed > maxSeqNo {
			break
		}

		oel := el
		el = el.Next()

		if crn.reqNo >= cw.nextReadyMark {
			panic("dev sanity test")
		}

		cw.reqNoList.Remove(oel)
		delete(cw.reqNoMap, crn.reqNo)
	}

	if cw.reqNoList.Len() > 0 {
		cw.lowWatermark = cw.reqNoList.Front().Value.(*clientReqNo).reqNo
	}
	for i := cw.lowWatermark + uint64(cw.reqNoList.Len()); i <= cw.lowWatermark+logWidth; i++ {
		el := cw.reqNoList.PushBack(&clientReqNo{
			clientID: cw.clientID,
			reqNo:    i,
			digests:  map[string]*clientRequest{},
		})
		cw.reqNoMap[i] = el
	}
	cw.highWatermark = cw.reqNoList.Back().Value.(*clientReqNo).reqNo

	close(cw.clientWaiter.expired)
	cw.clientWaiter = &clientWaiter{
		lowWatermark:  cw.lowWatermark,
		highWatermark: cw.highWatermark,
		expired:       make(chan struct{}),
	}
}

func (cw *clientWindow) ack(source NodeID, reqNo uint64, digest []byte) (*clientReqNo, *pb.Request) {
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

	return crn, newlyCorrectReq
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
			data:       requestData,
			agreements: map[NodeID]struct{}{},
		}
		crn.digests[string(digest)] = cr
	}

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

func (cw *clientWindow) status() *ClientWindowStatus {
	allocated := make([]uint64, cw.reqNoList.Len())
	i := 0
	for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
		crn := el.Value.(*clientReqNo)
		if crn.committed != nil {
			allocated[i] = 2 // TODO, actually report the seqno it committed to
		} else if len(crn.digests) > 0 {
			allocated[i] = 1
		}
		i++
	}

	return &ClientWindowStatus{
		LowWatermark:  cw.lowWatermark,
		HighWatermark: cw.highWatermark,
		Allocated:     allocated,
	}
}
