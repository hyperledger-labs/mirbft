# MirBFT Library Architecture

MirBFT is a Total Order Broadcast (TOB) / State Machine Replication (SMR) library.
This document describes the architecture and inner workings of the library
and is intended for its developers rather than for its users.


## _Node_

The top-level component of the library is the [_Node_](/node.go).
The user of the library instantiates a _Node_ object which serves as the interface to the MirBFT library.
Here we focus on the inner workings of a _Node_.
A _Node_ contains several [`modules`](/pkg/modules) that,
inspired by the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft),
interact through [_Events_](/protos/eventpb/eventpb.proto), also borrowing concepts from the actor model.


## _Events_

_Events_ are serializable objects (implemented using Protocol Buffers) that internally serve as messages
(not to be confused with [protocol messages](/protos/messagepb/messagepb.proto)) passed between the _Node_'s modules.
_Events_ are created either by the _Node_ itself when external events occur
(the library user calling some of the _Node_'s methods) or by the _Node_'s modules.
The _Node_'s modules, independently of each other, take _Events_ as input, process them,
and produce new _Events_ as output.
In this sense, each event constitutes a "work item" that needs to be processed by some module.
All created events are stored in the [_WorkItems_](/workitems.go) buffer,
from where they are dispatched (by the `Node.process()` method) to the appropriate module for processing.
This processing creates new _Events_ which are in turn added to the _WorkItems_, and so on.

For debugging purposes, all _Events_ can be recorded using an event _Interceptor_ ([see below](#interceptor))
and inspected or replayed in debug mode using the [mircat](/cmd/mircat/) utility.

### Follow-up _Events_

In certain cases, it is necessary that certain _Events_ are processed only after other _Events_ have been processed.
For example, before sending a protocol message and delivering a request batch to the application,
the protocol state might need to be persisted in the WAL to prevent unwanted behavior in case of a crash and recovery.
In such a case, the protocol module creates three _Events_: one for persisting the protocol state
(to be processed by the WAL module), one for sending a protocol message (to be processed by the Net module),
and one for delivering the protocol batch to the application (to be processed by the App module).
Since the WAL, the Net, and the App modules work independently and in parallel,
there is no guarantee about which of those modules will be the first to process _Events_.

An _Event_ thus contains a `Next` field, pointing to a slice of follow-up _Events_
that can only be processed after the _Event_ itself has been processed by the appropriate module.
In the example above, the _Events_ for the Net and App module would be included (as a 2-element list)
in the `Next` field of the "parent" WAL _Event_.

When a module (the WAL module in this case) processes an _Event_, it strips off all follow-up _Events_ and
only processes the parent _Event_.
It then adds all follow-up _Events_ to its output, as if they resulted from processing the parent _Event_.
In the example case, the _Events_ for the Net and App modules will be added to the WorkItems buffer at the same time,
without any guarantee about the order of their processing.
Follow-up _Events_ can be arbitrarily nested to express more complex dependency trees.


## _Modules_

Modules are components of the system, each responsible for a part of the library's implementation.
Each module is defined in the [`modules`](/pkg/modules) package by the tasks it has to perform.
The interface of each module is task-specific.

Multiple implementations of a module can exist.
When instantiating a _Node_, the library user chooses which module implementations to use.
This is done by means of the [_Modules_](/pkg/modules/modules.go) object passed to the `mirbft.NewNode()` function.
The user instantiates the desired modules, groups them in a _Modules_ object and passes this object to `mirbft.NewNode()`.
The library provides default module implementations (not yet - TODO) for the user to use out of the box,
but the user is free to supply its own ones.

The following modules constitute the MirBFT implementation (not all of them are implemented yet - TODO).
- [Net](/pkg/modules/net.go) sends messages produced by MirBFT through the network.
- [Hasher](/pkg/modules/hasher.go) computes hashes of requests and other data.
- [Crypto](/pkg/modules/crypto.go) performs cryptographic operations (except for computing hashes).
- [App](/pkg/modules/app.go) implements the user application logic. The user is expected to provide this module.
- [WAL](/pkg/modules/wal.go) implements a persistent write-ahead log for the case of crashes and restarts.
- [ClientTracker](/pkg/modules/clienttracker.go) keeps the state related to clients and validates submitted requests.
- [RequestStore](/pkg/modules/requeststore.go) provides persistent storage for request data.
- [Protocol](/pkg/modules/protocol.go) implements the logic of the distributed protocol.
- [Interceptor](/pkg/modules/eventinterceptor.go) intercepts and logs all internal _Events_ for debugging purposes.

### Net

The [Net](/pkg/modules/net.go) module provides a simple interface for a node to exchange messages with other nodes.
Currently, it only deals with numeric node IDs
that it has to translate to network addresses based on some (static) initial configuration
provided by the user at instantiation.
The messages the Net module sends can either be received by user code on the other end of the line
and manually injected to the _Node_ using the `Node.Step()` function,
or received by the corresponding remote Net module that in turn makes them available to the remote _Node_.

### Hasher

The [Hasher](/pkg/modules/hasher.go) module computes hashes of requests and other data.
It accepts hash request _Events_ and produces hash result _Events_.
Whenever any module needs to compute a hash,
instead of computing the hash directly inside the implementation of that module,
the module produces a hash request _Event_.
This _Event_ will be inserted in the [_WorkItems_](/workitems.go) buffer and
routed to the Hasher module by the `Node.process()` method.
When the Hasher computes the digest, a hash result event will be routed (usually) back to the requesting module
using metadata (`HashOrigin`) attached to the _Event_.
The interface of the Hasher corresponds to Go's built-in hashing interface, e.g. `crypto.SHA256`,
so Go's built-in hashers can be used directly.

### Crypto

The [Crypto](/pkg/modules/crypto.go) module is responsible for producing and verifying cryptographic signatures.
It internally stores information about which clients and nodes are associated with which public keys.
This information can be updated by the Node through appropriate _Events_.
(TODO: Not yet implemented. Say which Events once implemented.)
The Crypto module also processes _Events_ related to signature computation and verification, e.g. `VerifyRequestSig`,
and produces corresponding result _Events_, e.g. `RequestSigVerified`.


### App

The [App](/pkg/modules/app.go) module implements the user application logic
and is expected to always be provided by the user.
An ordered sequence of request batches is delivered to the App module by this library.
The application needs to be able to serialize its state and restore it from a serialized representation.
The library will periodically (at checkpoints) create a snapshot of the application state and may, if necessary,
reset the application state using a snapshot (this happens when state transfer is necessary).

### Write-Ahead Log (WAL)

The [WAL](/pkg/modules/wal.go) module implements a persistent write-ahead log for the case of crashes and restarts.
Whenever any module needs to append a persistent entry to the write-ahead log,
it outputs an _Event_ to be processed by the WAL module.
The WAL module will simply persist (a serialized form of) this _Event_.
On _Node_ startup (e.g. after restarting), the write-ahead log might be non-empty.
In such a case, before processing any external _Events_, the _Node_ loads all _Events_ stored in the WAL
and adds them to the WorkItems buffer.
It is then up to the individual modules to re-initialize their state
based on the stored _Events_ they receive as first input.

TODO: Implement and describe truncation of the WAL.

### ClientTracker

The [ClientTracker](/pkg/modules/clienttracker.go) module manages the state related to clients.
It is the first module to handle incoming client requests.
It makes sure that the payload of a request is persisted, the request is authenticated
and fulfills protocol-specific criteria.
Only then the client module passes on a reference to the request to the ordering protocol.

TODO: Give a brief explanation of client watermarks when implemented.

### RequestStore

The [RequestStore](/pkg/modules/requeststore.go) module is a persistent key-value store
that stores request payload data and request authentication metadata.
This way, the protocol need not care about the authenticity, validity and durability of client request data.
It will probably be the ClientTracker module to interact most with the RequestStore,
making sure that the received requests are persisted and authenticated before handing them over to the protocol.

TODO: Implement and document garbage collection of old requests included in a checkpointed state.

### Protocol

The [Protocol](/pkg/modules/protocol.go) module implements the logic of the distributed protocol
executed by the library.
It consumes and produces a large variety of _Events_.

The Protocol module (similarly to the expected implementation of the App module)
implements a deterministic state machine.
Processing of _Events_ by the Protocol module is sequential and deterministic (watch out for iteration over maps!).
Thus, the same sequence of input _Events_
will always result in the same protocol state and the same sequence of output _Events_.
This is important for debugging (TODO: Implement and document `mircat`)

For performance reasons, the processing of each event should be simple, fast, and non-blocking.
All "expensive" operations should be delegated to the other modules, which exist also for this reason.
For example, instead of computing a cryptographic hash inside the Protocol module,
the module should rather output a hash request event, have it processed by the Hasher module,
and wait for a hash result event (while sequentially processing other incoming events).

### Interceptor

The [Interceptor](/pkg/modules/eventinterceptor.go) intercepts and logs all internal _Events_ for debugging purposes,
producing what we call the event log.
When the `Node.process()` method dispatches a list of events
from the WorkItems buffer to the appropriate module for processing,
the Interceptor appends this list to its log.
Thus, events are always intercepted in the exact order as they are dispatched to their corresponding modules,
in order to be able to faithfully reproduce what happened during a run.
The log can then later be inspected or replayed.
The Interceptor module is not essential and would probably be disabled in a production environment,
but it is priceless for debugging.

#### Difference between the _WAL_ and the _Interceptor_

Note that both the Write-Ahead Log (WAL) and the Interceptor produce logs of _Events_ in stable storage.
Their purposes, however, are very different and largely orthogonal.

**_The WAL_** produces the **_write-ahead log_**
and is crucial for protocol correctness during recovery after restarting a _Node_.
It is explicitly used by other modules (mostly the _Protocol_ module) that creates WALAppend events
for persisting **_only certain events_** that are crucial for recovery.
The implementation of these modules (e.g., the protocol logic) decides what to store there
and the same logic must be capable to reinitialize itself when those stored events are played back to it on restart.

**_The Interceptor_** produces the **_event log_**.
This is a list of **_all events_** that occurred and is meant only for debugging (not for recovery).
The _Node_'s modules have no influence on what is intercepted.
The event log is intended to be processed by the `mircat` utility
to gain more insight into what exactly is happening inside the _Node_.

TODO: Link mircat here when it's ready.
