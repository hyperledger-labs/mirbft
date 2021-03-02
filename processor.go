/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"context"
	"github.com/pkg/errors"
	"hash"
	"runtime"
	"sync"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"golang.org/x/sync/errgroup"
)

type Hasher interface {
	New() hash.Hash
}

type Link interface {
	Send(dest uint64, msg *msgs.Msg)
}

// TODO allow these to return errors
type Log interface {
	Apply(*msgs.QEntry)
	Snap(networkConfig *msgs.NetworkState_Config, clientsState []*msgs.NetworkState_Client) (id []byte)
}

type WAL interface {
	Write(index uint64, entry *msgs.Persistent) error
	Truncate(index uint64) error
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
	Link   Link
	Hasher Hasher
	Log    Log
	WAL    WAL
	Node   *Node
}

func (p *Processor) Process(actions *Actions) (*ActionResults, error) {
	// Persist
	for _, write := range actions.WriteAhead {
		if write.Truncate != nil {
			if err := p.WAL.Truncate(*write.Truncate); err != nil {
				return nil, errors.WithMessage(err, "could not truncate WAL")
			}
		} else {
			if err := p.WAL.Write(write.Append.Index, write.Append.Data); err != nil {
				return nil, errors.WithMessage(err, "could not persist entry")
			}
		}
	}

	if err := p.WAL.Sync(); err != nil {
		return nil, errors.WithMessage(err, "could not sync WAL")
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

	// Apply
	actionResults := &ActionResults{
		Digests: make([]*HashResult, len(actions.Hash)),
	}

	for i, req := range actions.Hash {
		h := p.Hasher.New()
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
			continue
		}

		// Not a batch, so, must be a checkpoint

		value := p.Log.Snap(commit.Checkpoint.NetworkConfig, commit.Checkpoint.ClientsState)
		actionResults.Checkpoints = append(actionResults.Checkpoints, &CheckpointResult{
			Checkpoint: commit.Checkpoint,
			Value:      value,
		})
	}

	return actionResults, nil
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

func (wp *ProcessorWorkPool) persistThenSend(writeAhead []*Write, sends []Send) error {
	g := errgroup.Group{}

	// Next, begin persisting the WAL, plus any pending requests, once done,
	// send the other protocol messages
	g.Go(func() error {
		for _, write := range writeAhead {
			if write.Truncate != nil {
				if err := wp.processor.WAL.Truncate(*write.Truncate); err != nil {
					return errors.WithMessage(err, "could not truncate WAL")
				}
			} else {
				if err := wp.processor.WAL.Write(write.Append.Index, write.Append.Data); err != nil {
					return errors.WithMessage(err, "could not persist entry")
				}
			}
		}
		if err := wp.processor.WAL.Sync(); err != nil {
			return errors.WithMessage(err, "could not sync WAL")
		}

		for _, send := range sends {
			select {
			case wp.transmitC <- send:
			case <-wp.doneC:
				return nil
			}
		}

		return nil
	})

	g.Go(func() error {
		sent := 0
		for sent < len(sends) { // +len(forwards) { // TODO, handle forwards again
			select {
			case <-wp.transmitDoneC:
				sent++
			case <-wp.doneC:
				return nil
			}
		}
		return nil
	})

	return g.Wait()
}

func (wp *ProcessorWorkPool) serviceHashPool() {
	h := wp.processor.Hasher.New()
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

func (wp *ProcessorWorkPool) hash(hashReqs []*HashRequest) []*HashResult {
	go func() {
		for _, hashReq := range hashReqs {
			select {
			case wp.hashC <- hashReq:
			case <-wp.doneC:
				return
			}
		}
	}()

	hashResults := make([]*HashResult, 0, len(hashReqs))
	for len(hashResults) < len(hashReqs) {
		select {
		case hashResult := <-wp.hashDoneC:
			hashResults = append(hashResults, hashResult)
		case <-wp.doneC:
			return nil
		}
	}

	return hashResults
}

func (wp *ProcessorWorkPool) commit(commits []*Commit) []*CheckpointResult {
	var checkpoints []*CheckpointResult

	for _, commit := range commits {
		if commit.Batch != nil {
			wp.processor.Log.Apply(commit.Batch) // Apply the entry
			continue
		}

		// Not a batch, so, must be a checkpoint

		value := wp.processor.Log.Snap(commit.Checkpoint.NetworkConfig, commit.Checkpoint.ClientsState)
		checkpoints = append(checkpoints, &CheckpointResult{
			Checkpoint: commit.Checkpoint,
			Value:      value,
		})
	}

	return checkpoints
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

func (wp *ProcessorWorkPool) Process(actions *Actions) (*ActionResults, error) {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()

	results := &ActionResults{}
	g := errgroup.Group{}

	g.Go(func() error {
		return wp.persistThenSend(actions.WriteAhead, actions.Send)
	})

	g.Go(func() error {
		results.Digests = wp.hash(actions.Hash)
		return nil
	})

	g.Go(func() error {
		results.Checkpoints = wp.commit(actions.Commits)
		return nil
	})

	return results, g.Wait()
}
