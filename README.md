# MirBFT Library

MirBFT is a library implementing the [Mir byzantine fault tolerant consensus protocol](https://arxiv.org/abs/1906.05552) in a network transport, storage, and cryptographic algorithm agnostic way.  MirBFT hopes to be a building block of a next generation of distributed systems, providing an implementation of [atomic broadcast](https://en.wikipedia.org/wiki/Atomic_broadcast) which can be utilized by any distributed system.

Mir improves on traditional atomic broadcast protocols like PBFT and Raft which always have a single active leader by allowing concurrent leaders and reconciling total order in a deterministic and provably safe way.  The multi-leader nature of Mir should lead to exceptional performance especially on wide area networks but should be suitable for LAN deployments as well.

[![Build Status](https://travis-ci.org/IBM/mirbft.svg?branch=master)](https://travis-ci.org/IBM/mirbft)
[![GoDoc](https://godoc.org/github.com/IBM/mirbft?status.svg)](https://godoc.org/github.com/IBM/mirbft)

## Architecture

The high level structure of the MirBFT library steals heavily from the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft). A single threaded state machine is mutated by short lived, non-blocking operations.  Operations which might block or which have any degree of computational complexity (like hashing, network calls, etc.) are delegated to the caller.

For more information, see the detailed [design document](/docs/Design.md).  Note, the documentation has fallen a bit behind based on the implementation work that has happened over the last few months.  The documentation should be taken with a grain of salt.

## Using Mir
 
This repository is a new project and under active development and as such, is not suitable for use (yet!). It's a fair ways further along that it was in early 2020, and we hope to have a release in the coming months.

### Preview

Currently, the Mir APIs are still stabilizing, and there are significant caveats associated with assorted features.  Notably, there are not yet APIs for state transfer (though this feature is nearly complete), there are no APIs for reconfiguration (though once more, this feature is close to done), and there are some assorted unhandled internal cases (like some known missing validation in new epoch messages, poor new epoch leader selection, slow graceful epoch rotation).  However, a sample processor is included, and the basic client implementation will look something like this:

```
networkState := mirbft.StandardInitialNetworkState(4, 0)

nodeConfig := &mirbft.Config{
	ID:     uint64(i),
	Logger: zap.NewExample(),
	BatchSize: 20,
	HeartbeatTicks:       2,
	SuspectTicks:         4,
	NewEpochTimeoutTicks: 8,
	BufferSize:           500,
}

doneC := make(chan struct{})

node, err := mirbft.StartNewNode(nodeConfig, doneC, mockStorage(networkState))
// handle err

processor := &sample.SerialProcessor{
	Node:      node,
	Hasher:    sha256.New,
	Log:       log,        // sample.Log interface impl
	Link:      network,    // sample.Link interface impl
}

go func() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case actions := <-node.Ready():
			processor.Process(&actions)
		case <-ticker.C:
			node.Tick()
		case <-doneC:
			return
		}
	}
}()

// Perform application logic
err := node.Propose(context.TODO(), &pb.Request{
	ClientId: 0,
	ReqNo: 0,
	Data: []byte("some-data"),
})
...
```

Note that `sample.SerialProcessor` is a rudimentary implementations which for simplicity does not exploit parallelism across the hashing/persisting/sending, but could parallelized for a production system.  Future samples may include a parallelized Procecessor.
