# MirBFT Library Design

The high level structure of the MirBFT library steals heavily from the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft). A single threaded state machine is mutated by short lived, non-blocking operations.  Operations which might block or which have any degree of computational complexity (like signing, hashing, etc.) are delegated to the caller.

## Top Level Components

At a high level, the `*mirbft.Node` component handles all of the interactions between the state machine and the components of the consumer network.

![Top level components](http://yuml.me/diagram/plain/usecase/(State%20Machine)-(Node),(Node)-(Net%20Ingress),(Node)-(Processor),(Node)-(Client%20Ingress),(Processor)-(Net%20Egress),(Processor)-(Persistence))

### Node and State Machine
*State Machine* is the heart of the MirBFT library, and is intentionally exported in only a very limited way and is otherwise hidden behind the `*mirbft.Node` APIs.  The *Node* component spawns a dedicated go routine at creation which serializes all access to the the *State Machine*.

The *Node* component proxies interactions from the *Network Ingress*, *Client Ingress*, and *Processor* components, to the state machine and builds a list of required actions dictated by the *State Machine*.  These actions are periodically consumed by the *Processor* component, and require actions such as broadcasting to the network, committing data to the filesystem, etc.  Some actions have results which must be returned to the state machine to continue processing.

For more details see the [State Machine design document](StateMachine.md).

### Network Ingress
*Network Ingress* is responsible for taking messages from other nodes in the network, and delivering it to the state machine.  It is the responsibility of this component to ensure that messages are authenticated as originating from an authorized node in the network, and to unmarshal message contents before delivering them to the state machine.

The network ingress may be trivially parallelized, performing checks on a per connection basis, calling into the `*mirbft.Node` under the safe assumption that it will be serialized.

### Client Ingress
*Client ingress* is responsible for injecting new client requests into the system.  It is assumed that the data of the request has been validated and that the originating node would approve of the data being ordered into the system.

Just like with *Network ingress*, *Data Ingress* may be trivially parallelized, as the *Node* will serialize parallel requests.

Unlike some BFT consensus systems which allow leaders to assign an order to client requests, Mir's mechanics depends heavily upon client messages having a designated request number which is unique to their client ID. This can introduce some complexity into clients, but there are several strategies which can mitigate this complexity.  TODO, expand on this section -- basic types, persist requests at client before submitting, sign messages and verify your signature from the node, or go with worst case pessimism sending null requests to flush to a new start point.

For more details see the [Clients document](Clients.md).

### Processor
*Processor* is where the bulk of the application logic lives.  It is responsible for performing the actions requested by the state machine, such as hashing, sending to the network, persisting state to disk, and finally, processing commits from the state machine (the actual application logic).

There are two builtin processors which are suitable for many tasks, but it's easy enough to write your own if the provided ones are inadequate.  The first processor is the [SerialProcessor](https://github.com/hyperledger-labs/mirbft/blob/master/processor.go) which performs the persistence, network sends, and finally commits serially.  This processor is useful for resource constrained environments, or for more deterministic development/testing of an application.  There is the additional [ParallelProcessor](https://github.com/hyperledger-labs/mirbft/blob/master/processor.go) which performs all steps of the processing in parallel where safe (for instance, data is persisted in parallel, but must complete before networking requests are also sent in parallel).

For more details see the [Processor document](Processor.md).

### Persistence

There are two types of persistence required to consume the MirBFT library, persistence of requests and persistence of state machine transitions.

Persistence of client requests is relatively simple.  When a client request arrives, the state machine will instruct the processor to persist the request (including a client id and request number) along with a hash of the request.  The state machine may request that the processor forward this request to another node at a later time, and will reference this transaction by client id, request number, and digest at commit time.

The second form of persistence is more complex, but fortunately opaque to the consumer.  When the state machine undergoes critical operations, for instance prepreparing or preparing a sequence number, it must first persist this operation to disk to ensure that on crash, it does not accidentally behave in byzantine ways.  Therefore, the state machine internally constructs a 'persisted' log, and requests that the processor persist the components of that log prior to performing network sends for a particular set of actions.  From a consumer perspective, the semantics are very simple, write an entry at a particular index to a write-ahead-log on disk stably, before performing network sends, and on startup, provide the state machine access to replay this log.  From an implementation perspective, the structure of this write-ahead-log is perhaps the most important component to understand, as it is leveraged to safely deal with state transfer, reconfiguration, and other complex topics.

For more details see the [Log movement document](LogMovement.md).
required for client requests which are undergoing consensus, usually, this will take the form of a key-value store.  There is additionally peristence required for operations created by the 
