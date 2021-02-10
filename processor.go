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

// TODO, convert panics into errors

type Hasher func() hash.Hash

type Link interface {
	Send(dest uint64, msg *pb.Msg)
}

// TODO allow these to return errors
type Log interface {
	Apply(*pb.QEntry)
	Snap(networkConfig *pb.NetworkState_Config, clientsState []*pb.NetworkState_Client) (id []byte)
}

type WAL interface {
	Write(index uint64, entry *pb.Persistent) error
	Truncate(index uint64) error
	Sync() error
}

type RequestStore interface {
	GetAllocation(clientID, reqNo uint64) ([]byte, error)
	PutAllocation(clientID, reqNo uint64, digest []byte) error
	GetRequest(requestAck *pb.RequestAck) ([]byte, error)
	PutRequest(requestAck *pb.RequestAck, data []byte) error
	Commit(requestAck *pb.RequestAck) error
	Sync() error
}

// Processor provides an implementation of action processing for the
// mirbft Actions returned from the Ready() channel.  It is an optional
// component and provides an implementation only for common case usage,
// but may instead be implemented with normal application code.  This base
// processor operates in a serial fashion, first persisting requests,
// then writing the WAL, then sending requests, then computing hashes, then
// applying entries to the provided Log, and finally returning the requested
// hash and checkpoint results.  Because these operations include IO and are
// blocking, this base implementation is most suitable for test or resource
// constrained environments.
type Processor struct {
	Link         Link
	Hasher       Hasher
	Log          Log
	WAL          WAL
	RequestStore RequestStore
	Node         *Node
}

func (p *Processor) Process(actions *Actions) *ActionResults {
	// Persist
	for _, r := range actions.AllocatedRequests {
		digest, err := p.RequestStore.GetAllocation(r.ClientID, r.ReqNo)
		if err != nil {
			panic(fmt.Sprintf("could not get key for %d.%d: %s", r.ClientID, r.ReqNo, err))
		}

		if digest != nil {
			if err := p.Node.Propose(context.Background(), r, digest); err != nil {
				panic(err)
			}
			continue
		}

		// TODO, do something like
		//   p.RequestStore.PutLocalRequest(r.ClientID, r.ReqNo, nil)
		// with a client lock held, that way when the client goes to propose this
		// sequence it will find that it's been allocated, then continue
		// for now to sort of not break the tests, we're automatically generating
		// client requests.
	}

	if err := p.RequestStore.Sync(); err != nil {
		panic(fmt.Sprintf("could not sync request store, unsafe to continue: %s\n", err))
	}

	for _, write := range actions.WriteAhead {
		if write.Truncate != nil {
			if err := p.WAL.Truncate(*write.Truncate); err != nil {
				panic(fmt.Sprintf("could truncate WAL, not safe to continue: %s", err))
			}
		} else {
			if err := p.WAL.Write(write.Append.Index, write.Append.Data); err != nil {
				panic(fmt.Sprintf("could not persist entry, not safe to continue: %s", err))
			}
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

	// XXX address
	/*
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
	*/

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
		if commit.Batch != nil {
			p.Log.Apply(commit.Batch) // Apply the entry

			// TODO, we need to make it clear that committing should not actually
			// delete the data until the checkpoint.
			for _, reqAck := range commit.Batch.Requests {
				err := p.RequestStore.Commit(reqAck)
				if err != nil {
					panic("could not mark ack as committed, unsafe to continue")
				}
			}

			continue
		}

		// Not a batch, so, must be a checkpoint

		value := p.Log.Snap(commit.Checkpoint.NetworkConfig, commit.Checkpoint.ClientsState)
		actionResults.Checkpoints = append(actionResults.Checkpoints, &CheckpointResult{
			Checkpoint: commit.Checkpoint,
			Value:      value,
		})
	}

	return actionResults
}

// ProcessorWorkPool is a work pool based version of the standard Processor.
// It fulfills the same purpose as the base Processor, which is to provide an
// implementation of processing logic suitable for most applications, but instead
// of processing in a serial manner, the ProcessorWorkPool dispatches work to
// different work pools, both for storage, network sends, as well as computations.
// The parallelism occurs across three areas.  Request forwarding, hashing, committing, and
// persistence/networking.  Since request forwarding, hashing, and committing all operate
// without effect on state machine safety, these may safely be performed in parallel with
// the persistence/networking which does impact state machine safety.  Further paralellism
// is employed within each of these subactions where safe.
type ProcessorWorkPool struct {
	processor *Processor

	waitGroup sync.WaitGroup
	mutex     sync.Mutex

	// these four channels are buffered and serviced
	// by the worker pool routines
	transmitC     chan Send
	transmitDoneC chan struct{}
	hashC         chan *HashRequest
	hashDoneC     chan *HashResult

	doneC chan struct{}
}

func (wp *ProcessorWorkPool) serviceSendPool() {
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

func (wp *ProcessorWorkPool) persistThenSendInParallel(
	writeAhead []*Write,
	allocated []RequestSlot,
	sends []Send,
	forwards []Forward,
	sendDoneC chan<- struct{},
) {
	// First begin forwarding requests over the network, this may be done concurrently
	// with persistence
	// XXX address
	/*
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
	*/

	// Next, begin persisting the WAL, plus any pending requests, once done,
	// send the other protocol messages
	go func() {
		for _, write := range writeAhead {
			if write.Truncate != nil {
				if err := wp.processor.WAL.Truncate(*write.Truncate); err != nil {
					panic(fmt.Sprintf("could truncate WAL, not safe to continue: %s", err))
				}
			} else {
				if err := wp.processor.WAL.Write(write.Append.Index, write.Append.Data); err != nil {
					panic(fmt.Sprintf("could not persist entry, not safe to continue: %s", err))
				}
			}
		}
		if err := wp.processor.WAL.Sync(); err != nil {
			panic(fmt.Sprintf("could not sync WAL: %s", err))
		}

		// TODO, this could probably be parallelized with the WAL write
		for _, r := range allocated {
			digest, err := wp.processor.RequestStore.GetAllocation(r.ClientID, r.ReqNo)
			if err != nil {
				panic(fmt.Sprintf("could not get key for %d.%d: %s", r.ClientID, r.ReqNo, err))
			}

			if digest != nil {
				if err := wp.processor.Node.Propose(context.Background(), r, digest); err != nil {
					panic(err)
				}
				continue
			}

			// TODO, do something like
			//   p.RequestStore.PutLocalRequest(r.ClientID, r.ReqNo, nil)
			// with a client lock held, that way when the client goes to propose this
			// sequence it will find that it's been allocated, then continue
			// for now to sort of not break the tests, we're automatically generating
			// client requests.
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
		for sent < len(sends) { // +len(forwards) { // TODO, handle forwards again
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

func (wp *ProcessorWorkPool) serviceHashPool() {
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

func (wp *ProcessorWorkPool) hashInParallel(hashReqs []*HashRequest, hashBatchDoneC chan<- []*HashResult) {
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

func (wp *ProcessorWorkPool) commitInParallel(commits []*Commit, commitBatchDoneC chan<- []*CheckpointResult) {
	go func() {
		var checkpoints []*CheckpointResult

		for _, commit := range commits {
			if commit.Batch != nil {
				wp.processor.Log.Apply(commit.Batch) // Apply the entry

				// TODO, we need to make it clear that committing should not actually
				// delete the data until the checkpoint.
				for _, reqAck := range commit.Batch.Requests {
					err := wp.processor.RequestStore.Commit(reqAck)
					if err != nil {
						panic("could not mark ack as committed, unsafe to continue")
					}
				}

				continue
			}

			// Not a batch, so, must be a checkpoint

			value := wp.processor.Log.Snap(commit.Checkpoint.NetworkConfig, commit.Checkpoint.ClientsState)
			checkpoints = append(checkpoints, &CheckpointResult{
				Checkpoint: commit.Checkpoint,
				Value:      value,
			})
		}

		commitBatchDoneC <- checkpoints
	}()
}

type ProcessorWorkPoolOpts struct {
	TransmitWorkers int
	HashWorkers     int
}

func NewProcessorWorkPool(p *Processor, opts ProcessorWorkPoolOpts) *ProcessorWorkPool {
	if opts.TransmitWorkers == 0 {
		opts.TransmitWorkers = runtime.NumCPU()
	}

	if opts.HashWorkers == 0 {
		opts.HashWorkers = runtime.NumCPU()
	}

	wp := &ProcessorWorkPool{
		processor: p,

		doneC: make(chan struct{}),

		transmitC:     make(chan Send, opts.TransmitWorkers),
		transmitDoneC: make(chan struct{}, opts.TransmitWorkers),
		hashC:         make(chan *HashRequest, opts.HashWorkers),
		hashDoneC:     make(chan *HashResult, opts.HashWorkers),
	}

	wp.waitGroup.Add(opts.TransmitWorkers + opts.HashWorkers)

	for i := 0; i < opts.TransmitWorkers; i++ {
		go func() {
			wp.serviceSendPool()
			wp.waitGroup.Done()
		}()
	}

	for i := 0; i < opts.HashWorkers; i++ {
		go func() {
			wp.serviceHashPool()
			wp.waitGroup.Done()
		}()
	}

	return wp
}

func (wp *ProcessorWorkPool) Stop() {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()
	close(wp.doneC)
	wp.waitGroup.Wait()
}

func (wp *ProcessorWorkPool) Process(actions *Actions) *ActionResults {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()
	sendBatchDoneC := make(chan struct{}, 1)
	hashBatchDoneC := make(chan []*HashResult, 1)
	commitBatchDoneC := make(chan []*CheckpointResult, 1)

	wp.persistThenSendInParallel(
		actions.WriteAhead,
		actions.AllocatedRequests,
		actions.Send,
		actions.ForwardRequests,
		sendBatchDoneC,
	)
	wp.hashInParallel(actions.Hash, hashBatchDoneC)
	wp.commitInParallel(actions.Commits, commitBatchDoneC)

	<-sendBatchDoneC

	return &ActionResults{
		Digests:     <-hashBatchDoneC,
		Checkpoints: <-commitBatchDoneC,
	}
}
