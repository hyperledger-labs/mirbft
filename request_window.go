/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type request struct {
	requestData *pb.RequestData
	digest      []byte
	state       SequenceState
	seqNo       uint64
}

type requestWindow struct {
	lowWatermark    uint64
	highWatermark   uint64
	nextUnallocated uint64
	requests        []*request
}

func newRequestWindow(lowWatermark, highWatermark uint64) *requestWindow {
	return &requestWindow{
		nextUnallocated: lowWatermark,
		lowWatermark:    lowWatermark,
		highWatermark:   highWatermark,
		requests:        make([]*request, int(highWatermark-lowWatermark)+1),
	}
}

func (rw *requestWindow) garbageCollect(maxSeqNo uint64) {
	newRequests := make([]*request, int(rw.highWatermark-rw.lowWatermark)+1)
	i := 0
	j := uint64(0)
	copying := false
	for _, request := range rw.requests {
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

	rw.lowWatermark += j
	rw.highWatermark += j
	rw.requests = newRequests
}

func (rw *requestWindow) hasRoomToAllocate() bool {
	return rw.nextUnallocated <= rw.highWatermark
}

func (rw *requestWindow) allocateNext() uint64 {
	result := rw.nextUnallocated
	rw.nextUnallocated++
	return result
}

func (rw *requestWindow) allocate(requestData *pb.RequestData, digest []byte) {
	reqNo := requestData.ReqNo
	if reqNo > rw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, rw.highWatermark))
	}

	if reqNo < rw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, rw.lowWatermark))
	}

	offset := int(reqNo - rw.lowWatermark)
	if rw.requests[offset] != nil {
		panic("unexpected")
	}

	rw.requests[offset] = &request{
		requestData: requestData,
		digest:      digest,
	}
}

func (rw *requestWindow) request(reqNo uint64) *request {
	if reqNo > rw.highWatermark {
		panic(fmt.Sprintf("unexpected: %d > %d", reqNo, rw.highWatermark))
	}

	if reqNo < rw.lowWatermark {
		panic(fmt.Sprintf("unexpected: %d < %d", reqNo, rw.lowWatermark))
	}

	offset := int(reqNo - rw.lowWatermark)

	return rw.requests[offset]
}

type RequestWindowStatus struct {
	LowWatermark  uint64
	HighWatermark uint64
	Allocated     []uint64
}

func (rw *requestWindow) status() *RequestWindowStatus {
	allocated := make([]uint64, len(rw.requests))
	for i, request := range rw.requests {
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

	return &RequestWindowStatus{
		LowWatermark:  rw.lowWatermark,
		HighWatermark: rw.highWatermark,
		Allocated:     allocated,
	}
}
