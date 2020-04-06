/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type clientWindows struct {
	windows map[string]*clientWindow
	clients []string
}

func (cws *clientWindows) clientWindow(clientID []byte) (*clientWindow, bool) {
	cw, ok := cws.windows[string(clientID)]
	return cw, ok
}

func (cws *clientWindows) insert(clientID []byte, cw *clientWindow) {
	cws.windows[string(clientID)] = cw
	cws.clients = append(cws.clients, string(clientID))
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

func (cwi *clientWindowIterator) next() ([]byte, *clientWindow) {
	if cwi.index >= len(cwi.clientWindows.clients) {
		return nil, nil
	}
	client := cwi.clientWindows.clients[cwi.index]
	clientWindow := cwi.clientWindows.windows[client]
	cwi.index++
	return []byte(client), clientWindow
}

type request struct {
	requestData *pb.Request
	digest      []byte
	state       SequenceState
	seqNo       uint64
}

type clientWindow struct {
	myConfig      *Config
	lowWatermark  uint64
	highWatermark uint64
	requests      []*request
	clientWaiter  *clientWaiter // Used to throttle clients
}

type clientWaiter struct {
	lowWatermark  uint64
	highWatermark uint64
	expired       chan struct{}
}

func newClientWindow(lowWatermark, highWatermark uint64, myConfig *Config) *clientWindow {
	return &clientWindow{
		myConfig:      myConfig,
		lowWatermark:  lowWatermark,
		highWatermark: highWatermark,
		requests:      make([]*request, int(highWatermark-lowWatermark)+1),
		clientWaiter: &clientWaiter{
			lowWatermark:  lowWatermark,
			highWatermark: highWatermark,
			expired:       make(chan struct{}),
		},
	}
}

func (cw *clientWindow) garbageCollect(maxSeqNo uint64) {
	newRequests := make([]*request, int(cw.highWatermark-cw.lowWatermark)+1)
	i := 0
	j := uint64(0)
	copying := false
	for _, request := range cw.requests {
		if request == nil || request.state != Committed || request.seqNo > maxSeqNo {
			copying = true
		}

		if copying {
			newRequests[i] = request
			i++
		} else {
			if request.seqNo == 0 {
				panic("this should be initialized if here")
			}
			j++
		}

	}

	cw.lowWatermark += j
	cw.highWatermark += j
	cw.requests = newRequests
	close(cw.clientWaiter.expired)
	cw.clientWaiter = &clientWaiter{
		lowWatermark:  cw.lowWatermark,
		highWatermark: cw.highWatermark,
		expired:       make(chan struct{}),
	}
}

func (cw *clientWindow) allocate(requestData *pb.Request, digest []byte) {
	reqNo := requestData.ReqNo
	if reqNo > cw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, cw.highWatermark))
	}

	if reqNo < cw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, cw.lowWatermark))
	}

	offset := int(reqNo - cw.lowWatermark)
	if cw.requests[offset] != nil {
		if !bytes.Equal(cw.requests[offset].digest, digest) {
			panic("we don't handle byzantine clients yet, but two different requests for the same reqno")
		}
		return
	}

	cw.requests[offset] = &request{
		requestData: requestData,
		digest:      digest,
	}
}

func (cw *clientWindow) request(reqNo uint64) *request {
	if reqNo > cw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, cw.highWatermark))
	}

	if reqNo < cw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, cw.lowWatermark))
	}

	offset := int(reqNo - cw.lowWatermark)

	return cw.requests[offset]
}

func (cw *clientWindow) status() *ClientWindowStatus {
	allocated := make([]uint64, len(cw.requests))
	for i, request := range cw.requests {
		if request == nil {
			continue
		}
		if request.state == Committed {
			allocated[i] = 2
		} else {
			allocated[i] = 1
		}
		// allocated[i] = bytesToUint64(request.preprocessResult.Proposal.Data)
	}

	return &ClientWindowStatus{
		LowWatermark:  cw.lowWatermark,
		HighWatermark: cw.highWatermark,
		Allocated:     allocated,
	}
}
