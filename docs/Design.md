# MirBFT Library Design

The high level structure of the MirBFT library steals heavily from the architecture of [etcdraft](https://github.com/etcd-io/etcd/tree/master/raft). A single threaded state machine is mutated by short lived, non-blocking operations.  Operations which might block or which have any degree of computational complexity (like signing, hashing, etc.) are delegated to the caller.

## Top Level Components

At a high level, the `*mirbft.Node` component handles all of the interactions between the state machine and the components of the consumer network.

![Top level components](http://yuml.me/diagram/plain/usecase/(State%20Machine)-(Node),(Node)-(Net%20Ingress),(Node)-(Consumer),(Node)-(Data%20Ingress),(Consumer)-(Net%20Egress),(Consumer)-(Persistence))

### Node and State Machine
*State Machine* is the heart of the MirBFT library, and is intentionally exported in only a very limited way and is otherwise hidden behind the `*mirbft.Node` APIs.  The *Node* component spawns a dedicated go routine at creation which serializes all access to the the *State Machine*.

The *Node* component proxies interactions from the *Network Ingress*, *Data Ingress*, and *Consumer* components, to the state machine and builds a list of required actions dictated by the *State Machine*.  These actions are periodically consumed by the *Consumer* component, and require actions such as broadcasting to the network, committing data to the filesystem, etc.  Some actions have results which must be returned to the state machine to continue processing.

For more details see the [State Machine design document](StateMachine.md).

### Network Ingress
*Network Ingress* is responsible for taking messages from other nodes in the network, and delivering it to the state machine.  It is the responsibility of this component to ensure that messages are authenticated as originating from an authorized node in the network, and to unmarshal message contents before delivering them to the state machine.

The network ingress may be trivially parallelized, performing checks on a per connection basis, calling into the `*mirbft.Node` under the safe assumption that it will be serialized.

### Data Ingress
*Data ingress* is responsible for injecting new messages into the system.  It is assumed that the data has been validated and that the originating node would approve of the data being ordered into the system.

Just like with *Network ingress*, *Data Ingress* may be trivially parallelized, as the *Node* will serialize parallel requests.

### Consumer
*Consumer* is where the bulk of the application logic lives.  It is responsible for performing the actions requested by the state machine, such as validation, sending to the network, persisting state to disk, etc..
