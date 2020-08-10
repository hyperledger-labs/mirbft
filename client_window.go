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
	networkConfig *pb.NetworkConfig
	myConfig      *Config
	readyList     *list.List
	readyMap      map[*clientReqNo]*list.Element
}

func newClientWindows(networkConfig *pb.NetworkConfig, myConfig *Config) *clientWindows {
	cws := &clientWindows{
		myConfig:      myConfig,
		networkConfig: networkConfig,
		windows:       map[uint64]*clientWindow{},
		readyList:     list.New(),
		readyMap:      map[*clientReqNo]*list.Element{},
	}

	clientWindowWidth := uint64(100) // XXX this should be configurable

	for _, client := range networkConfig.Clients {
		lowWatermark := client.BucketLowWatermarks[0]
		for _, blw := range client.BucketLowWatermarks {
			if blw < lowWatermark {
				lowWatermark = blw
			}
		}

		clientWindow := newClientWindow(lowWatermark, lowWatermark+clientWindowWidth, networkConfig, myConfig)
		cws.insert(client.Id, clientWindow)
	}

	return cws
}

func (cws *clientWindows) ack(source NodeID, clientID, reqNo uint64, digest []byte) {
	cw, ok := cws.windows[clientID]
	if !ok {
		panic("dev sanity test")
	}

	clientReqNo := cw.ack(source, reqNo, digest)
	if clientReqNo.strongRequest == nil {
		return
	}

	_, ok = cws.readyMap[clientReqNo]
	if ok {
		return
	}

	el := cws.readyList.PushBack(clientReqNo)

	cws.readyMap[clientReqNo] = el
}

func (cws *clientWindows) garbageCollect(seqNo uint64) {
	cwi := cws.iterator()
	for _, cw := cwi.next(); cw != nil; _, cw = cwi.next() {
		cw.garbageCollect(seqNo)
	}

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

func (cws *clientWindows) iterator() *clientWindowIterator {
	return &clientWindowIterator{
		clientWindows: cws,
	}
}

type clientWindowIterator struct {
	index         int
	clientWindows *clientWindows
}

func (cwi *clientWindowIterator) next() (uint64, *clientWindow) {
	if cwi.index >= len(cwi.clientWindows.clients) {
		return 0, nil
	}
	client := cwi.clientWindows.clients[cwi.index]
	clientWindow := cwi.clientWindows.windows[client]
	cwi.index++
	return client, clientWindow
}

type clientReqNo struct {
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
	lowWatermark  uint64
	highWatermark uint64
	reqNoList     *list.List
	reqNoMap      map[uint64]*list.Element
	clientWaiter  *clientWaiter // Used to throttle clients
	myConfig      *Config
	networkConfig *pb.NetworkConfig
}

type clientWaiter struct {
	lowWatermark  uint64
	highWatermark uint64
	expired       chan struct{}
}

func newClientWindow(lowWatermark, highWatermark uint64, networkConfig *pb.NetworkConfig, myConfig *Config) *clientWindow {
	cw := &clientWindow{
		myConfig:      myConfig,
		networkConfig: networkConfig,
		lowWatermark:  lowWatermark,
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

		cw.reqNoList.Remove(oel)
		delete(cw.reqNoMap, crn.reqNo)
	}

	if cw.reqNoList.Len() > 0 {
		cw.lowWatermark = cw.reqNoList.Front().Value.(*clientReqNo).reqNo
	}
	for i := cw.lowWatermark + uint64(cw.reqNoList.Len()); i <= cw.lowWatermark+logWidth; i++ {
		el := cw.reqNoList.PushBack(&clientReqNo{
			reqNo:   i,
			digests: map[string]*clientRequest{},
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

func (cw *clientWindow) ack(source NodeID, reqNo uint64, digest []byte) *clientReqNo {
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

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequest = cr
	}

	return crn
}

func (cw *clientWindow) allocate(requestData *pb.Request, digest []byte) {
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

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequest = cr
	}

	// TODO, this could trigger readiness
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
