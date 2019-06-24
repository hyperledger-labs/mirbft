# MirBFT Library

MirBFT is a library implementing the [Mir byzantine fault tolerant consensus protocol](https://arxiv.org/abs/1906.05552) in a network transport, storage, and cryptographic algorithm agnostic way.  MirBFT hopes to be a building block of a next generation of distributed systems, providing an implementation of [atomic broadcast](https://en.wikipedia.org/wiki/Atomic_broadcast) which can be utilized by any distributed system.

Mir improves on traditional atomic broadcast protocols like PBFT and Raft which always have a single active leader by allowing concurrent leaders and reconciling total order in a deterministic and provably safe way.  The multi-leader nature of Mir should lead to exceptional performance especially on wide area networks but should be suitable for LAN deployments as well.

[![Build Status](https://travis-ci.org/IBM/mirbft.svg?branch=master)](https://travis-ci.org/IBM/mirbft)
[![GoDoc](https://godoc.org/github.com/IBM/mirbft?status.svg)](https://godoc.org/github.com/IBM/mirbft)

## Architecture

The high level structure of the MirBFT library steals heavily from the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft). A single threaded state machine is mutated by short lived, non-blocking operations.  Operations which might block or which have any degree of computational complexity (like signing, hashing, etc.) are delegated to the caller.

For more information, see the detailed [design document](/docs/Design.md).

## Using Mir
 
This repository is a new project and under active development and as such, is not suitable for use (yet!). Watch for releases occurring later in 2019.

### Preview

Currently, there are severe limitations ot the Mir implementation, notably no view/epoch change, and no state transfer, as well as no ability to resume a node after stopping it.  However, sample processors are included, and the basic client implementation will look something like this:

```
replicas := []mirbft.Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}

config := &mirbft.Config{
	ID:     uint64(i),
	Logger: zap.NewProduction(),
	BatchParameters: mirbft.BatchParameters{
		CutSizeBytes: 1,
	},
}

doneC := make(chan struct{})

node, err := mirbft.StartNewNode(config, doneC, replicas)
// handle err

processor := &sample.SerialProcessor{
	Node:      node,
	Validator: validator, // sample.Validator interface impl
	Hasher:    hasher,    // sample.Hasher interface impl
	Committer: &sample.SerialCommitter{
		Log:                  log, // sample.Log interface impl
		OutstandingSeqBucket: map[uint64]map[uint64]*mirbft.Entry{},
	},
	Link: network, // sample.Link interface impl
}

go func() {
	for {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()


		select {
		case actions := <-node.Ready():
			processor.Process(&actions)
		case <-ticker.C:
			node.Tick()
		case <-doneC:
			// exit
		}
	}
}

// Perform application logic
err := node.Propose(context.TODO(), []byte("some-data"))
...
```

Note that `sample.SerialProcessor` and `sample.SerialCommitter` are rudimentary implementations which for simplicity do not exploit parallelism across the hashing/validation/committing, but could parallelized for a production system.  Future samples may include a parallelized Procecessor and or Committer.
