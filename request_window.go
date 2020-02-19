/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type request struct {
	preprocessResult *PreprocessResult
	state            SequenceState
}

type requestWindow struct {
	lowWatermark    uint64
	highWatermark   uint64
	nextToDrain     uint64
	nextUnallocated uint64
	requests        []*request
}

func newRequestWindow(lowWatermark, highWatermark uint64) *requestWindow {
	return &requestWindow{
		nextToDrain:     lowWatermark,
		nextUnallocated: lowWatermark,
		lowWatermark:    lowWatermark,
		highWatermark:   highWatermark,
		requests:        make([]*request, int(highWatermark-lowWatermark)+1),
	}
}

func (rw *requestWindow) hasRoomToAllocate() bool {
	return rw.nextUnallocated <= rw.highWatermark
}

func (rw *requestWindow) allocateNext() uint64 {
	result := rw.nextUnallocated
	rw.nextUnallocated++
	return result
}

func (rw *requestWindow) allocate(preProcessResult *PreprocessResult) {
	reqNo := preProcessResult.Proposal.ReqNo
	if reqNo > rw.highWatermark {
		panic("unexpected")
	}

	if reqNo < rw.lowWatermark {
		panic("unexpected")
	}

	offset := int(reqNo - rw.lowWatermark)
	if rw.requests[offset] != nil {
		panic("unexpected")
	}

	rw.requests[offset] = &request{
		preprocessResult: preProcessResult,
	}
}

func (rw *requestWindow) request(reqNo uint64) *request {
	if reqNo > rw.highWatermark {
		panic("unexpected")
	}

	if reqNo < rw.lowWatermark {
		panic("unexpected")
	}

	offset := int(reqNo - rw.lowWatermark)
	if rw.requests[offset] != nil {
		panic("unexpected")
	}

	return rw.requests[offset]
}
