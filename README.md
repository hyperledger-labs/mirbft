# MirBFT Library

MirBFT is a library implementing the [Mir byzantine fault tolerant consensus protocol](https://arxiv.org/abs/1906.05552) in a network transport, storage, and cryptographic algorithm agnostic way.  MirBFT hopes to be a building block of a next generation of distributed systems, providing an implementation of [atomic broadcast](https://en.wikipedia.org/wiki/Atomic_broadcast) which can be utilized by any distributed system.

MirBFT improves on traditional atomic broadcast protocols like PBFT and Raft which always have a single active leader by allowing concurrent leaders and reconciling total order in a deterministic and provably safe way.  The multi-leader nature of Mir should lead to exceptional performance especially on wide area networks but should be suitable for LAN deployments as well.

MirBFT, like the original [PBFT](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/01/p398-castro-bft-tocs.pdf) protocol, emphasizes consenting over digests, rather than over actual request data.  This allows for more novel methods of request dissemination, and in the optimal case, the state machine may never need to fetch request data.

MirBFT, also like the original PBFT shuns signatures internally, instead preferring to collect quorum certificates during epoch change and other such events.  In many ways, this complicates the internal implementation of the library, but it drastically simplifies the external interface and prevents signature validation from becoming a bottleneck under certain workloads.

[![Build Status](https://travis-ci.org/IBM/mirbft.svg?branch=master)](https://travis-ci.org/IBM/mirbft)
[![GoDoc](https://godoc.org/github.com/IBM/mirbft?status.svg)](https://godoc.org/github.com/IBM/mirbft)

## Architecture

The high level structure of the MirBFT library steals heavily from the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft). A single threaded state machine is mutated by short lived, non-blocking operations.  Operations which might block or which have any degree of computational complexity (like hashing, network calls, etc.) are delegated to the caller.

The required components not dictated by the implementation include:

1. A write-ahead-log for persisting the state machine log entries. (or use [the provided one](https://github.com/IBM/mirbft/blob/master/simplewal/wal.go)).
2. A hashing implementation (such as the builtin [sha256](https://golang.org/pkg/crypto/sha256/)).
3. A request store for persisting application requests while they are consented upon (or use [the provided one](https://github.com/IBM/mirbft/blob/master/reqstore/reqstore.go)).
4. An application state which can apply committed requests and may be snapshotted.

For basic applications, only (4) may need to be written, though for applications which wish to optimize for throughput (for instance avoiding committing request data to disk twice), a custom implementation of (3) which integrates with (4) may be desirable.

For more information, see the detailed [design document](/docs/Design.md).  Note, the documentation has fallen a bit behind based on the implementation work that has happened over the last few months.  The documentation should be taken with a grain of salt.

## Using Mir
 
This repository is a new project and under active development and as such, is not suitable for use (yet!). It's a fair ways further along that it was in early 2020, and we hope to have a release in the coming months.  For now, if you'd like to see a sample application, please look at [mirbft-sample](https://github.com/jyellick/mirbft-sample).

### Preview

Currently, the Mir APIs are still stabilizing, and there are significant caveats associated with assorted features.  Notably, there are not yet APIs for state transfer (though this feature is nearly complete), although there are APIs for reconfiguration, it does not entirely work, and there are some assorted unhandled internal cases (like some known missing validation in new epoch messages, poor new epoch leader selection, slow graceful epoch rotation, and more).  However, the APIs are stabilizing, and the overall code architecture is finalizing.

```
networkState := mirbft.StandardInitialNetworkState(4, 0)

nodeConfig := &mirbft.Config{
	ID:     uint64(i),
	Logger: mirbft.ConsoleInfoLogger,
	BatchSize: 20,
	HeartbeatTicks:       2,
	SuspectTicks:         4,
	NewEpochTimeoutTicks: 8,
	BufferSize:           500,
}

applicationLog := MyNewApplicationLog(networkState)

node, err := mirbft.StartNewNode(nodeConfig, networkState, applicationLog.Snap())
// handle err

processor := &mirbft.SerialProcessor{
	Node:      node,
	Hasher:    sha256.New,
	Log:       applicationLog,   // mirbft.Log interface impl
	Link:      network,          // mirbft.Link interface impl
}

go func() {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case actions := <-node.Ready():
			node.AddResults(processor.Process(&actions))
		case <-ticker.C:
			node.Tick()
		case <-node.Err():
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
