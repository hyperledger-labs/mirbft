# Node and State Machine

Thie Mir node and state machine are the core components of the MirBFT implementation.  They is responsible for accumulating messages and actions from the network and consumer, translating those into new required actions, forming the heart of the feedback loop between the library consumer and the library itself.

![Node and State Machine](http://yuml.me/diagram/plain/activity/(Node)->(StateMachine),(StateMachine)->(CheckpointTracker)->(NodeBuffer),(StateMachine)->(ClientWindows)->(NodeBuffer),(StateMachine)->(EpochTracker)->(NodeBuffer))

## Serializer

The [Serializer](../serializer.go) is the point of contact between the *Node* component and the rest of the state machine.  The *Serializer* is created with a single dedicated go routine in a infinite loop, receiving requests from the *Node* and dispatching them to the *State Machine*, then aggregating results from the state machine for retreival via the *Node*.

When a new consensus message arrives from the *Network Ingress* component, it is sent in through the `stepC` channel, and applied to the state machine.  When a new message is to be ordered, the *Data Ingress* component sends it to the `PropC` channel.

For the *Consumer* component, there are two channels to interact with.  The first, is the `readyC` channel.  This channel will return actions requested by the state machine for the consumer to perform.  The second, is the `resultsC` channel, which the consumer returns the results of applying the actions from the `readyC`.  The serializer maintains a set of outstanding `*consumer.Actions`, which is appended to every time the serializer handles a request from `resultsC`, `propC`, or `stepC`, which is copied to the consumer and cleared whenever `readyC` is read from.  Finally, the consumer is expected to periodically send a message on `tickC`, to help correct unbalanced bucket distribution and or failed nodes.

The serializer additionally exposes a `StatusC` which allows for the caller to request a dump of the current state of the state machine, either in a console loggable or JSON format.  The status feature is useful for demonstration and debugging, but is not expected to be called with frequency during normal execution.  If the caller needs access to specific information with high frequency, we should consider adding dedicated lighter APIs.

## EpochTracker

The [EpochTracker](../epoch_tracker.go) is responsible for handling epoch related messages.  This includes the epoch change related messages before an epoch becomes active, as well as the three-phase commit messages once an epoch becomes active.  The EpochTracker generally delegates message handling down into an EpochTarget, which, if active further delegates messages into an EpochActive.  Messages which the state machine is not yet prepared to handle but may potentially be valid in the future are passed into the NodeBuffer component for potential future processing.

![Epoch Tracker](http://yuml.me/diagram/plain/activity/(EpochTracker)->(CurrentEpochTarget),(EpochTracker)->(PendingEpochTargets)->(NodeBuffer),(CurrentEpochTarget)->(EpochActive)->(Sequences),(EpochActive)->(NodeBuffer),(EpochTracker)->(NodeBuffer),(EpochActive)->(OutstandingReqs),(EpochActive)->(Proposer))

The EpochActive validates that the sequences, embedding batches of client requests, occur in an appropriate order.  In particular, client requests must pre-prepare in order for a particular bucket, e.g. in a network with 4 buckets, bucket 0 must see client request from client 0 in the order 0, 4, 8, 12, etc.  This consistency checking is done via the OutstandingReqs component.  Additionally, sequences are marked allocated, but not validated until a weak quorum cert attests that the client requests are themselves valid.

The EpochActive calls into the proposer component if the nodeID is responsible for any buckets.  The proposer component assembles a stream of batches for a bucket compliant with the rules defined for the outstanding reqs and in accordance with the configured node batching rules.

## NodeBuffers

TODO, WIP.  This is currently not consolidated, but in short, there is a bounded buffer available for each node to store messages which are not currently ready to be applied.  This is the most common cause for instability in PBFT-like networks, where a node receives bursts of messages faster than the application can accomodate them, leading to messages about the watermarks or outside the current epoch to be discarded.  By having a centralized bounded buffer, we can generally accomodate and smooth these bursts.

## Sequences

The [Sequences](../sequence.go) is a component which tracks the three-phase commit protocol for a given sequence/bucket/epoch.  Many more details may be found in the [Sequences State Machine document](Sequence.md).
