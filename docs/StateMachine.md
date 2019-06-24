# State Machine

Thie Mir state machine is the core component of the MirBFT implementation.  It is responsible for accumulating messages and actions from the network and consumer, translating those into new required actions, forming the heart of the feedback loop between the library consumer and the library itself.

![State Machine](http://yuml.me/diagram/plain/activity/(Serializer)->(Epoch),(Epoch)->(Nodes)->(NextMsgs),(Epoch)->(Buckets)->(Sequences),(Epoch)->(Proposer))

TODO add checkpoint windows

## Serializer

The [Serializer](../serializer.go) is the point of contact for components outside of the state machine, notably the `*mirbft.Node*` component.  The *Serializer* is created with a single dedicated go routine in a infinite loop, interacting with the callers of the state machine.

When a new consensus message arrives from the *Network Ingress* component, it is sent in through the `stepC` channel, and applied to the state machine.  When a new message is to be ordered, the *Data Ingress* component sends it to the `PropC` channel.

For the *Consumer* component, there are two channels to interact with.  The first, is the `readyC` channel.  This channel will return actions requested by the state machine for the consumer to perform.  The second, is the `resultsC` channel, which the consumer returns the results of applying the actions from the `readyC`.  The serializer maintains a set of outstanding `*consumer.Actions`, which is appended to every time the serializer handles a request from `resultsC`, `propC`, or `stepC`, which is copied to the consumer and cleared whenever `readyC` is read from.  Finally, the consumer is expected to periodically send a message on `tickC`, to help correct unbalanced bucket distribution and or failed nodes.

The serializer additionally exposes a `StatusC` which allows for the caller to request a dump of the current state of the state machine, either in a console loggable or JSON format.  The status feature is useful for demonstration and debugging, but is not expected to be called with frequency during normal execution.  If the caller needs access to specific information with high frequency, we should consider adding dedicated lighter APIs.

## Epoch

The [Epoch](../epoch.go) is responsible for dispatching requests to each of the necessary sub-components.  For instance, when the state machine receives a _Prepare_ message, it first needs to check that the message is within the watermarks and for the appropriate epoch, then it needs to check that this is an anticipated next message from the *Nodes* component, then it needs to apply it to the correct *Bucket* component.  The *Epoch* component is also responsible for aggregating top level events, like checking if the arrival of a checkpoint qualifies the state machine for watermark movement, then calling into each of the other components indicating that they should respond to watermark movement.

## Proposer

The [Proposer](../proposer.go) is responsible for aggregating messages into batches, and assigning those batches into particular buckets and sequence numbers.  The Epoch configuration indicates which buckets a node is responsible for, and the Proposer assigns to bucket and sequence number in a simple round-robin fashion.  Note, the original Mir paper calls for a deterministic assignment of message to bucket (which the network may verify), but the goal is two-fold.

1. Assigning a message to a bucket is meant to distribute load over the network, to allow greater throughput.  An optimal bucket dissemination has all buckets receive exactly the same number of messages.  A good hashing algorithm and a large number of buckets should asymptotically approach optimal, but by routing proposals to a bucket in a round robin fashion, we can actually approach optimal with an even greater efficiency.
2. Assigning a message to a bucket and rotating bucket ownership is meant to add censorship resistance.  In this case, it is more important that the same node not be responsible for the same message when in different epochs.  So, if a node is assigned the responibility for a message, it does not actually matter which bucket it assigns the message to, as it is the selection of node, not of bucket, which adds the censorship resistancee.

The proposer component will buffer messages (presently indefinitely), and drain the queue whenever there is space available in the watermarks.  Note, space available in the watermarks is defined by a heuristic which requires that a number of checkpoint intervals remain before the high watemark is reached.  The goal is to ensure that the sequence numbers being allocated lay towards the middle of the watermarks, otherwise, a race develops between watermark movement and sequence allocation.  There are other similar heuristics about checkpoint garbage collection as well.

## Nodes

The [Nodes](../node.go) is a component which tracks the message received by nodes in the network.  Although the original PBFT (and Mir) allow for out of order message delivery, the implementation can be drastically simplified assuming in order message delivery, so the *Nodes* component enforces this.  For each node in the network, and for each bucket, the *Nodes* component tracks the most recently sent Prepare/Commit or Preprepare/Commit message sent (for non-leader and leader respectively).  If the last Prepare/Commit sequence for a bucket was 7 and 3 respectively, then the next message from that node for that bucket may either be a Prepare for sequence 8, or a Commit for squence 4.  Today, unanticipated messages are simply logged and dropped.  In the future, it should be possible to add a caching component, to allow out of order message delivery and still apply those message in order.  Note however, for typical network links, an unanticipated message indicates a missed/lost message and state transfer or view/epoch change will likely be required.

## Buckets

The [Buckets](../bucket.go) is a component which tracks the progression of the three-phase commit within a bucket.  Because of the invariants guaranteed *Nodes* component (messages arrive in relative order), and the invariants guaranteed below by the *Sequences* component (transitions to Prepared/Committed only occur after sending our own Prepare/Commit), buckets themselves provide another useful guarantee.  For any given bucket, the highest committed sequence number is always less than or equal to the highest prepared sequence number, and all sequences less than or equal to the highest committed or prepared sequence number are themselves committed or prepared, respecitvely.  In other words, buckets never develop holes.

The *Buckets* component maintains a list of *Sequences* components, for each active sequence number between the watermarks. Because *Sequences* components are agnostic to the leadership of the bucket, the *Buckets* component is responsible for producing and applying psuedo-messages to the *Sequences* component.  This includes automatically transitioning from *Digested* to *Verified* when the node is the bucket leader to avoid double validation, as well as applying a pseudo-*Prepare* message as if the leader sent it to avoid additional logical complexity inside the *Sequences* component.

## Sequences

The [Sequences](../sequence.go) is a component which tracks the three-phase commit protocol for a given sequence/bucket/epoch.  Many more details may be found in the [Sequences State Machine document](Sequence.md).
