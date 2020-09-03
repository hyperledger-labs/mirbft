/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type batchTracker struct {
	batchesByDigest map[string]*batch
	fetchInFlight   map[string][]uint64
}

type batch struct {
	observedSequences map[uint64]struct{}
	requestAcks       []*pb.RequestAck
}

func newBatchTracker(persisted *persisted) *batchTracker {
	bt := &batchTracker{
		batchesByDigest: map[string]*batch{},
		fetchInFlight:   map[string][]uint64{},
	}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_QEntry:
			qEntry := d.QEntry
			acks := make([]*pb.RequestAck, len(qEntry.Requests))
			for i, req := range qEntry.Requests {
				acks[i] = &pb.RequestAck{
					ClientId: req.Request.ClientId,
					ReqNo:    req.Request.ReqNo,
					Digest:   req.Digest,
				}
			}
			bt.addBatch(qEntry.SeqNo, qEntry.Digest, acks)
		}
	}

	return bt
}

func (bt *batchTracker) step(source nodeID, msg *pb.Msg) *Actions {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_FetchBatch:
		msg := innerMsg.FetchBatch
		return bt.replyFetchBatch(uint64(source), msg.SeqNo, msg.Digest)
	case *pb.Msg_ForwardBatch:
		msg := innerMsg.ForwardBatch
		return bt.applyForwardBatchMsg(source, msg.SeqNo, msg.Digest, msg.RequestAcks)
	default:
		panic(fmt.Sprintf("unexpected bad batch message type %T, this indicates a bug", msg.Type))
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

func (bt *batchTracker) addBatch(seqNo uint64, digest []byte, requestAcks []*pb.RequestAck) {
	b, ok := bt.batchesByDigest[string(digest)]
	if !ok {
		b = &batch{
			observedSequences: map[uint64]struct{}{},
			requestAcks:       requestAcks,
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

func (bt *batchTracker) fetchBatch(seqNo uint64, digest []byte, sources []uint64) *Actions {
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

	return (&Actions{}).send(
		sources,
		&pb.Msg{
			Type: &pb.Msg_FetchBatch{
				FetchBatch: &pb.FetchBatch{
					SeqNo:  seqNo,
					Digest: digest,
				},
			},
		},
	)
}

func (bt *batchTracker) replyFetchBatch(source uint64, seqNo uint64, digest []byte) *Actions {
	batch, ok := bt.getBatch(digest)
	if !ok {
		// TODO, is this worth logging, or just ignore? (It's not necessarily byzantine)
		return &Actions{}
	}

	return (&Actions{}).send(
		[]uint64{source},
		&pb.Msg{
			Type: &pb.Msg_ForwardBatch{
				ForwardBatch: &pb.ForwardBatch{
					SeqNo:       seqNo,
					Digest:      digest,
					RequestAcks: batch.requestAcks,
				},
			},
		},
	)
}

func (bt *batchTracker) applyForwardBatchMsg(source nodeID, seqNo uint64, digest []byte, requestAcks []*pb.RequestAck) *Actions {
	_, ok := bt.fetchInFlight[string(digest)]
	if !ok {
		// We did not request this batch digest, so we don't know if we can trust it, discard
		// TODO, maybe log? Maybe not though, since we delete from the map when we get it.
		return &Actions{}
	}

	data := make([][]byte, len(requestAcks))
	for i, requestAck := range requestAcks {
		data[i] = requestAck.Digest
	}
	return &Actions{
		Hash: []*HashRequest{
			{
				Data: data,
				Origin: &pb.HashResult{
					Type: &pb.HashResult_VerifyBatch_{
						VerifyBatch: &pb.HashResult_VerifyBatch{
							Source:         uint64(source),
							SeqNo:          seqNo,
							RequestAcks:    requestAcks,
							ExpectedDigest: digest,
						},
					},
				},
			},
		},
	}
}

func (bt *batchTracker) applyVerifyBatchHashResult(digest []byte, verifyBatch *pb.HashResult_VerifyBatch) {
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
			requestAcks:       verifyBatch.RequestAcks,
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
