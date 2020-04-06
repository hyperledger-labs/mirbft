/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type batchTracker struct {
	batchesByDigest map[string]*batch
	fetchInFlight   map[string][]uint64
}

type batch struct {
	observedSequences map[uint64]struct{}
	requests          []*pb.Request
}

func newBatchTracker() *batchTracker {
	return &batchTracker{
		batchesByDigest: map[string]*batch{},
		fetchInFlight:   map[string][]uint64{},
	}
}

func (bt *batchTracker) truncate(seqNo uint64) {
	for digest, batch := range bt.batchesByDigest {
		for seq := range batch.observedSequences {
			if seq < seqNo {
				delete(batch.observedSequences, seq)
			}
		}
		if len(batch.observedSequences) == 0 {
			delete(bt.batchesByDigest, digest)
		}
	}
}

func (bt *batchTracker) addBatch(seqNo uint64, digest []byte, requests []*pb.Request) {
	b, ok := bt.batchesByDigest[string(digest)]
	if !ok {
		b = &batch{
			observedSequences: map[uint64]struct{}{},
			requests:          requests,
		}
		bt.batchesByDigest[string(digest)] = b
	}

	inFlight, ok := bt.fetchInFlight[string(digest)]
	if ok {
		for _, ifSeqNo := range inFlight {
			b.observedSequences[ifSeqNo] = struct{}{}
		}
		delete(bt.fetchInFlight, string(digest))
	}

	b.observedSequences[seqNo] = struct{}{}
}

func (bt *batchTracker) fetchBatch(seqNo uint64, digest []byte) *Actions {
	inFlight, ok := bt.fetchInFlight[string(digest)]
	if ok {
		// It's a weird, but possible case, that two batches have
		// identical digests, for different seqNos.  If so, we need
		// to track them separately.
		for _, ifSeqNo := range inFlight {
			if ifSeqNo == seqNo {
				return &Actions{}
			}
		}
	}
	inFlight = append(inFlight, seqNo)
	bt.fetchInFlight[string(digest)] = inFlight

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_FetchBatch{
					FetchBatch: &pb.FetchBatch{
						SeqNo:  seqNo,
						Digest: digest,
					},
				},
			},
		},
	}
}

func (bt *batchTracker) replyFetchBatch(seqNo uint64, digest []byte) *Actions {
	batch, ok := bt.getBatch(digest)
	if !ok {
		// TODO, is this worth logging, or just ignore? (It's not necessarily byzantine)
		return &Actions{}
	}

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_ForwardBatch{
					ForwardBatch: &pb.ForwardBatch{
						SeqNo:    seqNo,
						Digest:   digest,
						Requests: batch.requests,
					},
				},
			},
		},
	}
}

func (bt *batchTracker) applyForwardBatchMsg(source NodeID, seqNo uint64, digest []byte, requests []*pb.Request) *Actions {
	_, ok := bt.fetchInFlight[string(digest)]
	if !ok {
		// We did not request this batch digest, so we don't know if we can trust it, discard
		// TODO, maybe log? Maybe not though, since we delete from the map when we get it.
		return &Actions{}
	}

	data := make([][]byte, len(requests))
	for i, request := range requests {
		data[i] = request.Digest
	}
	return &Actions{
		Hash: []*HashRequest{
			{
				Data: data,
				VerifyBatch: &VerifyBatch{
					Source:         uint64(source),
					SeqNo:          seqNo,
					Requests:       requests,
					ExpectedDigest: digest,
				},
			},
		},
	}
}

func (bt *batchTracker) applyVerifyBatchHashResult(digest []byte, verifyBatch *VerifyBatch) {
	if !bytes.Equal(verifyBatch.ExpectedDigest, digest) {
		panic("byzantine")
		// XXX this should be a log only, but panic-ing to make dev easier for now
	}

	inFlight, ok := bt.fetchInFlight[string(digest)]
	if !ok {
		// We must have gotten multiple responses, and already
		// committed one, which is fine.
		return
	}

	b, ok := bt.batchesByDigest[string(digest)]
	if !ok {
		b = &batch{
			observedSequences: map[uint64]struct{}{},
			requests:          verifyBatch.Requests,
		}
		bt.batchesByDigest[string(digest)] = b
	}

	for _, seqNo := range inFlight {
		b.observedSequences[seqNo] = struct{}{}
	}

	delete(bt.fetchInFlight, string(digest))
}

func (bt *batchTracker) hasFetchInFlight() bool {
	return len(bt.fetchInFlight) > 0
}

func (bt *batchTracker) getBatch(digest []byte) (*batch, bool) {
	b, ok := bt.batchesByDigest[string(digest)]
	return b, ok
}
