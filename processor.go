/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"fmt"
	"hash"
	"runtime"
	"sync"

	pb "github.com/IBM/mirbft/mirbftpb"
)

type Hasher func() hash.Hash

type Link interface {
	Send(dest uint64, msg *pb.Msg)
}

type Log interface {
	Apply(*pb.QEntry)
	Snap() (id []byte)
}

type WAL interface {
	Append(entry *pb.Persistent) error
	Sync() error
}

type RequestStore interface {
	Store(requestAck *pb.RequestAck, data []byte) error
	Get(requestAck *pb.RequestAck) ([]byte, error)
	Sync() error
}

type Processor struct {
	Link         Link
	Hasher       Hasher
	Log          Log
	WAL          WAL
	RequestStore RequestStore
	Node         *Node
}

func ProcessSerially(actions *Actions, p *Processor) *ActionResults {
	// Persist
	for _, r := range actions.StoreRequests {
		p.RequestStore.Store(
			r.RequestAck,
			r.RequestData,
		)
	}

	if err := p.RequestStore.Sync(); err != nil {
		panic(fmt.Sprintf("could not sync request store, unsafe to continue: %s\n", err))
	}

	for _, ps := range actions.Persist {
		if err := p.WAL.Append(ps); err != nil {
			panic(fmt.Sprintf("could not persist entry, not safe to continue: %s", err))
		}
	}

	if err := p.WAL.Sync(); err != nil {
		panic(fmt.Sprintf("could not sync WAL, not safe to continue: %s", err))
	}

	// Transmit
	for _, send := range actions.Send {
		for _, replica := range send.Targets {
			if replica == p.Node.Config.ID {
				p.Node.Step(context.Background(), replica, send.Msg)
			} else {
				p.Link.Send(replica, send.Msg)
			}
		}
	}

	for _, r := range actions.ForwardRequests {
		requestData, err := p.RequestStore.Get(r.RequestAck)
		if err != nil {
			panic(fmt.Sprintf("could not store request, unsafe to continue: %s\n", err))
		}

		fr := &pb.Msg{
			Type: &pb.Msg_ForwardRequest{
				&pb.ForwardRequest{
					RequestAck:  r.RequestAck,
					RequestData: requestData,
				},
			},
		}
		for _, replica := range r.Targets {
			if replica == p.Node.Config.ID {
				p.Node.Step(context.Background(), replica, fr)
			} else {
				p.Link.Send(replica, fr)
			}
		}
	}

	// Apply
	actionResults := &ActionResults{
		Digests: make([]*HashResult, len(actions.Hash)),
	}

	for i, req := range actions.Hash {
		h := p.Hasher()
		for _, data := range req.Data {
			h.Write(data)
		}

		actionResults.Digests[i] = &HashResult{
			Request: req,
			Digest:  h.Sum(nil),
		}
	}

	for _, commit := range actions.Commits {
		p.Log.Apply(commit.QEntry) // Apply the entry

		if commit.Checkpoint {
			value := p.Log.Snap()
			actionResults.Checkpoints = append(actionResults.Checkpoints, &CheckpointResult{
				Commit: commit,
				Value:  value,
			})
		}
	}

	return actionResults
}

type ParallelProcessor struct {
	Link                Link
	Hasher              Hasher
	Log                 Log
	WAL                 WAL
	RequestStore        RequestStore
	Node                *Node
	TransmitParallelism int
	HashParallelism     int

	actionsC     chan *Actions
	actionsDoneC chan *ActionResults
	exitC        chan struct{}
	doneC        chan struct{}
}

type workerPools struct {
	processor *ParallelProcessor

	// these four channels are buffered and serviced
	// by the worker pool routines
	transmitC     chan Send
	transmitDoneC chan struct{}
	hashC         chan *HashRequest
	hashDoneC     chan *HashResult

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
	sends []Send,
	forwards []Forward,
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
						RequestAck: &pb.RequestAck{
							ReqNo:    r.RequestAck.ReqNo,
							ClientId: r.RequestAck.ClientId,
							Digest:   r.RequestAck.Digest,
						},
						RequestData: requestData,
					},
				},
			}

			select {
			case wp.transmitC <- Send{
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
				r.RequestAck,
				r.RequestData,
			)
		}

		wp.processor.RequestStore.Sync()

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

			result := &HashResult{
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

func (wp *workerPools) hashInParallel(hashReqs []*HashRequest, hashBatchDoneC chan<- []*HashResult) {
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
		hashResults := make([]*HashResult, 0, len(hashReqs))
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

func (wp *workerPools) commitInParallel(commits []*Commit, commitBatchDoneC chan<- []*CheckpointResult) {
	go func() {
		var checkpoints []*CheckpointResult

		for _, commit := range commits {
			wp.processor.Log.Apply(commit.QEntry) // Apply the entry

			if commit.Checkpoint {
				value := wp.processor.Log.Snap()
				checkpoints = append(checkpoints, &CheckpointResult{
					Commit: commit,
					Value:  value,
				})
			}
		}

		commitBatchDoneC <- checkpoints
	}()
}

func (pp *ParallelProcessor) Stop() {
	close(pp.doneC)
	<-pp.exitC
}

func (pp *ParallelProcessor) Start() {
	pp.actionsC = make(chan *Actions)
	pp.actionsDoneC = make(chan *ActionResults)
	pp.exitC = make(chan struct{})
	doneC := make(chan struct{})
	pp.doneC = doneC
	wg := &sync.WaitGroup{}
	defer func() {
		wg.Wait()
		close(pp.exitC)
	}()

	if pp.TransmitParallelism == 0 {
		pp.TransmitParallelism = runtime.NumCPU()
	}

	if pp.HashParallelism == 0 {
		pp.HashParallelism = runtime.NumCPU()
	}

	wp := &workerPools{
		processor: pp,
		doneC:     doneC,

		transmitC:     make(chan Send, pp.TransmitParallelism),
		transmitDoneC: make(chan struct{}, pp.TransmitParallelism),
		hashC:         make(chan *HashRequest, pp.HashParallelism),
		hashDoneC:     make(chan *HashResult, pp.HashParallelism),
	}

	wg.Add(pp.TransmitParallelism + pp.HashParallelism)

	for i := 0; i < pp.TransmitParallelism; i++ {
		go func() {
			wp.serviceSendPool()
			wg.Done()
		}()
	}

	for i := 0; i < pp.HashParallelism; i++ {
		go func() {
			wp.serviceHashPool()
			wg.Done()
		}()
	}

	sendBatchDoneC := make(chan struct{}, 1)
	hashBatchDoneC := make(chan []*HashResult, 1)
	commitBatchDoneC := make(chan []*CheckpointResult, 1)

	for {
		select {
		case actions := <-pp.actionsC:
			wp.persistThenSendInParallel(actions.Persist, actions.StoreRequests, actions.Send, actions.ForwardRequests, sendBatchDoneC)
			wp.hashInParallel(actions.Hash, hashBatchDoneC)
			wp.commitInParallel(actions.Commits, commitBatchDoneC)

			select {
			case <-sendBatchDoneC:
			case <-doneC:
				return
			}

			actionResults := &ActionResults{}

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
			case pp.actionsDoneC <- actionResults:
			case <-doneC:
				return
			}
		case <-doneC:
			return
		}
	}
}

func (pp *ParallelProcessor) Process(actions *Actions, doneC <-chan struct{}) *ActionResults {
	select {
	case pp.actionsC <- actions:
	case <-doneC:
		return &ActionResults{}
	}

	select {
	case results := <-pp.actionsDoneC:
		return results
	case <-doneC:
		return &ActionResults{}
	}
}
