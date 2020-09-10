/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sample

import (
	"context"
	"fmt"
	"runtime"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
)

type ParallelProcessor struct {
	Link                Link
	Hasher              Hasher
	Log                 Log
	WAL                 WAL
	RequestStore        RequestStore
	Node                *mirbft.Node
	TransmitParallelism int
	HashParallelism     int
	ActionsC            chan *mirbft.Actions
	ActionsDoneC        chan *mirbft.ActionResults
}

type workerPools struct {
	processor *ParallelProcessor

	// these four channels are buffered and serviced
	// by the worker pool routines
	transmitC     chan mirbft.Send
	transmitDoneC chan struct{}
	hashC         chan *mirbft.HashRequest
	hashDoneC     chan *mirbft.HashResult

	// doneC is supplied when the pools are started
	// and when closed causes all processing to halt
	doneC <-chan struct{}
}

func (wp *workerPools) serviceSendPool() {
	for {
		select {
		case send := <-wp.transmitC:
			for _, replica := range send.Targets {
				if replica == wp.processor.Node.Config.ID {
					wp.processor.Node.Step(context.Background(), replica, send.Msg)
				} else {
					wp.processor.Link.Send(replica, send.Msg)
				}
			}
			select {
			case wp.transmitDoneC <- struct{}{}:
			case <-wp.doneC:
				return
			}
		case <-wp.doneC:
			return
		}
	}
}

func (wp *workerPools) persistThenSendInParallel(
	persist []*pb.Persistent,
	store []*pb.ForwardRequest,
	sends []mirbft.Send,
	forwards []mirbft.Forward,
	sendDoneC chan<- struct{},
) {
	// First begin forwarding requests over the network, this may be done concurrently
	// with persistence
	go func() {
		for _, r := range forwards {
			requestData, err := wp.processor.RequestStore.Get(r.RequestAck)
			if err != nil {
				panic("io error? this should always return successfully")
			}
			fr := &pb.Msg{
				Type: &pb.Msg_ForwardRequest{
					&pb.ForwardRequest{
						Request: &pb.Request{
							ReqNo:    r.RequestAck.ReqNo,
							ClientId: r.RequestAck.ClientId,
							Data:     requestData,
						},
						Digest: r.RequestAck.Digest,
					},
				},
			}

			select {
			case wp.transmitC <- mirbft.Send{
				Targets: r.Targets,
				Msg:     fr,
			}:
			case <-wp.doneC:
				return
			}
		}
	}()

	// Next, begin persisting the WAL, plus any pending requests, once done,
	// send the other protocol messages
	go func() {
		for _, p := range persist {
			if err := wp.processor.WAL.Append(p); err != nil {
				panic(fmt.Sprintf("could not persist entry: %s", err))
			}
		}
		if err := wp.processor.WAL.Sync(); err != nil {
			panic(fmt.Sprintf("could not sync WAL: %s", err))
		}

		// TODO, this could probably be parallelized with the WAL write
		for _, r := range store {
			wp.processor.RequestStore.Store(
				&pb.RequestAck{
					ReqNo:    r.Request.ReqNo,
					ClientId: r.Request.ClientId,
					Digest:   r.Digest,
				},
				r.Request.Data,
			)
		}

		for _, send := range sends {
			select {
			case wp.transmitC <- send:
			case <-wp.doneC:
				return
			}
		}
	}()

	go func() {
		sent := 0
		for sent < len(sends)+len(forwards) {
			select {
			case <-wp.transmitDoneC:
				sent++
			case <-wp.doneC:
				return
			}
		}

		sendDoneC <- struct{}{}
	}()
}

func (wp *workerPools) serviceHashPool() {
	h := wp.processor.Hasher()
	for {
		select {
		case hashReq := <-wp.hashC:
			for _, data := range hashReq.Data {
				h.Write(data)
			}

			result := &mirbft.HashResult{
				Request: hashReq,
				Digest:  h.Sum(nil),
			}
			h.Reset()
			select {
			case wp.hashDoneC <- result:
			case <-wp.doneC:
				return
			}
		case <-wp.doneC:
			return
		}
	}
}

func (wp *workerPools) hashInParallel(hashReqs []*mirbft.HashRequest, hashBatchDoneC chan<- []*mirbft.HashResult) {
	go func() {
		for _, hashReq := range hashReqs {
			select {
			case wp.hashC <- hashReq:
			case <-wp.doneC:
				return
			}
		}
	}()

	go func() {
		hashResults := make([]*mirbft.HashResult, 0, len(hashReqs))
		for len(hashResults) < len(hashReqs) {
			select {
			case hashResult := <-wp.hashDoneC:
				hashResults = append(hashResults, hashResult)
			case <-wp.doneC:
				return
			}
		}

		hashBatchDoneC <- hashResults
	}()
}

func (wp *workerPools) commitInParallel(commits []*mirbft.Commit, commitBatchDoneC chan<- []*mirbft.CheckpointResult) {
	go func() {
		var checkpoints []*mirbft.CheckpointResult

		for _, commit := range commits {
			wp.processor.Log.Apply(commit.QEntry) // Apply the entry

			if commit.Checkpoint {
				value := wp.processor.Log.Snap()
				checkpoints = append(checkpoints, &mirbft.CheckpointResult{
					Commit: commit,
					Value:  value,
				})
			}
		}

		commitBatchDoneC <- checkpoints
	}()
}

func (pp *ParallelProcessor) Start(doneC <-chan struct{}) {
	if pp.TransmitParallelism == 0 {
		pp.TransmitParallelism = runtime.NumCPU()
	}

	if pp.HashParallelism == 0 {
		pp.HashParallelism = runtime.NumCPU()
	}

	wp := &workerPools{
		processor: pp,
		doneC:     doneC,

		transmitC:     make(chan mirbft.Send, pp.TransmitParallelism),
		transmitDoneC: make(chan struct{}, pp.TransmitParallelism),
		hashC:         make(chan *mirbft.HashRequest, pp.HashParallelism),
		hashDoneC:     make(chan *mirbft.HashResult, pp.HashParallelism),
	}

	for i := 0; i < pp.TransmitParallelism; i++ {
		go wp.serviceSendPool()
	}

	for i := 0; i < pp.HashParallelism; i++ {
		go wp.serviceHashPool()
	}

	sendBatchDoneC := make(chan struct{}, 1)
	hashBatchDoneC := make(chan []*mirbft.HashResult, 1)
	commitBatchDoneC := make(chan []*mirbft.CheckpointResult, 1)

	for {
		select {
		case actions := <-pp.ActionsC:
			wp.persistThenSendInParallel(actions.Persist, actions.StoreRequests, actions.Send, actions.ForwardRequests, sendBatchDoneC)
			wp.hashInParallel(actions.Hash, hashBatchDoneC)
			wp.commitInParallel(actions.Commits, commitBatchDoneC)

			select {
			case <-sendBatchDoneC:
			case <-doneC:
				return
			}

			actionResults := &mirbft.ActionResults{}

			select {
			case actionResults.Digests = <-hashBatchDoneC:
			case <-doneC:
				return
			}

			select {
			case actionResults.Checkpoints = <-commitBatchDoneC:
			case <-doneC:
				return
			}

			select {
			case pp.ActionsDoneC <- actionResults:
			case <-doneC:
				return
			}
		case <-doneC:
			return
		}
	}
}

func (pp *ParallelProcessor) Process(actions *mirbft.Actions, doneC <-chan struct{}) *mirbft.ActionResults {
	select {
	case pp.ActionsC <- actions:
	case <-doneC:
		return &mirbft.ActionResults{}
	}

	select {
	case results := <-pp.ActionsDoneC:
		return results
	case <-doneC:
		return &mirbft.ActionResults{}
	}
}
