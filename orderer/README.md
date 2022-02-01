# _Orderer_

The _Orderer_ module implements the actual ordering of _Batches_, i.e., committing new _Entries_ to the _Log_.
The _Orderer_ listens to the _Manager_ for new _Segments_.
Whenever the _Manager_ issues a new _Segment_,
the _Orderer_ creates a new instance of the ordering protocol that proposes and agrees on _Request_ _Batches_ -
one _Batch_ for each _SN_ that is part of the _Segment_.
When a _Batch_ has been agreed upon for a particular _SN_,
the _Orderer_ announces the (_SN_, _Batch_) pair as an _Entry_, indicating that it can be committed to the _Log_.
We now describe the interface of an _Orderer_ in terms of its behavior.

## Execution of the _Orderer_

The _Orderer_ module starts executing when its `Start` method is called by the system.
The implementation of the `Start` method must:

- Read _Segments_ from the attached _Manager_ (see below on initializing the _Orderer_).
- For each obtained _Segment_, order the _Segment_ (possibly in parallel with ordering other _Segments_).

We define ordering a _Segment_ `segment` as calling `announcer.Announce(logEntry)`
exactly once for each sequence number returned by `segment.SNs`.
The `logEntry` parameter is of type `*log.Entry` and represents an _Entry_.
It must contain one of the sequence numbers returned by `segment.SNs` along with a _Batch_ of _Requests_. 

Ordering a segment is subject to the following restrictions:
- The set of `logEntry` arguments passed to invocations of `announcer.Announce` must be the same across all correct
 peers. (The order in which those _Log_ _Entries_ are announced, however, need not be the same.)
- All _Entries_ must contain _Batches_ exclusively from the _Segment_'s _Bucket_.
  - The leader of the protocol obtains a _Batch_ using the `segment.bucket.CutBatch` method.
- The leader must not call `segment.bucket.CutBatch` before a particular sequence number has been committed.
 This sequence number is defined by the _Segment_ and accessible through the `segment.StartsAfter` method.
  - The `log` package provides a convenience method `log.WaitForEntry`
  that blocks until a specific _Entry_ is committed.

## Implementing an _Orderer_

To implement an _Orderer_, one must implement the `orderer.Orderer` interface.
The `Init`, `HandleMessage`, and `Start` methods serve for integration with the rest of the system
and their signature is described in the comments in [orderer.go](orderer.go).
Here we describe how these methods need to be implemented for obtaining a working _Orderer_.

For an example implementation of a dummy _Orderer_ that can be used as a skeleton for real _Orderer_ implementations,
see [orderer/dummyorderer.go](../orderer/dummyorderer.go).

### Initializing the _Orderer_

Initialization needs to be implemented in the `Init` method.
In addition to initializing whatever internal data structures the particular _Orderer_ implementation needs,
the _Orderer_ must subscribe to _Segments_ the provided _Manager_ issues
(the _Manager_ is provided as an argument to `Init`).
Subscribing to _Segments_ will most likely be a one-liner calling the _Manager_'s `SubscribeOrderer` method.
This method returns a channel that the _Orderer_ needs to save in its local state.
When started, the _Orderer_ will read _Segments_ from the channel and order these _Segments_.

The `Init` method must be called before the _Orderer_ is started (using _Start_).
After `Init` returns, the _Orderer_ must be ready to process incoming messages
(even if the handler blocks on the first one), while not producing any outgoing messages.
(Outgoing messages can be produced only after `Start` is called.)
This is crucial, as the program that uses the _Orderer_ firs initializes it
and then starts the messaging subsystem (the _Messenger_).
The _Messenger_ needs an initialized _Orderer_ for the messages the _Messenger_ delivers.
However, it is only after the _Messenger_ starts, when it is safe for the _Orderer_ to produce outgoing messages.

### Protocol messages

As the _Orderer_ is supposed to implement an ordering protocol, it will need to exchange messages.
Each message used by the _Orderer_ is wrapped in a protocol message of type `*pb.OrdererMsg`,
defined in [protobufs/messenger.proto](../protobufs/messenger.proto).
A `*pb.OrdererMsg` only has one field (called `Msg`) of the protocol buffers "oneof" type, containing the actual message
of the ordering protocol.
The _Orderer_ implementation must define its own message types (e.g. `PrePrepare`, `Prepare`, `Commit`, etc. for PBFT)
to be used in this field and add these message types as possible values of the `msg` field in
the definition of `OrdererMsg` in [protobufs/messenger.proto](../protobufs/messenger.proto). 

#### Sending messages

To send a message `m` to a peer with ID `id`,
the _Orderer_ implementation only needs to call the `messenger.EnqueueMessage(m, id)`.
The message `m` needs to be of type `*pb.ProtocolMessage` with `senderId` set to the own ID
(accessible through `membership.OwnID`) and `Msg` containing a (properly wrapped) message of type `*pb.OrdererMsg`.
The `OrdererMsg`, in turn, contains the (properly wrapped) actual message in its own (nested) `Msg` field.

For example, the _DummyOrderer_ defines a `DummyOrdererMsg`, an instance of which is created as follows.
```go
m := &pb.ProtocolMessage{
    SenderId: membership.OwnID,
    Msg: &pb.ProtocolMessage_Orderer{Orderer: &pb.OrdererMsg{
        Msg: &pb.OrdererMsg_Dummy{Dummy:&pb.DummyOrdererMsg{
            ProtocolSpedificField1: protocolSpecificValue1
            ProtocolSpedificField2: protocolSpecificValue2
            //...
        }},
    }},
}
```

#### Handling messages

To handle incoming messages, the `HandleMessage` method of the _Orderer_ must be implemented.
This method is called by the _Messenger_ whenever the peer receives a message that belongs to the ordering protocol.
Note that `HandleMessage` will potentially be called by multiple goroutines concurrently
and its implementation needs to deal with it.

In addition to the received message, `HandleMessage` receives he ID of the sending peer as an additional argument.
Depending on the implementation of _Messenger_, thes messages might or might not be authenticated.
