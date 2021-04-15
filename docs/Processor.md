# Processor design

The *Processor* is where the bulk of the application logic lives.  It is responsible for performing the actions requested by the state machine, such as hashing, sending to the network, persisting state to disk, and finally, processing commits from the state machine (the actual application logic).

## Builtin Processors

There are two builtin processors which are suitable for many applications.  Both processors work under the assumption that the application provides five components:

* `RequestStore` which can be used to persistently store requests as well as retreive them to forward to other nodes in the network.
* `WAL` which can be used to persistently store the state machine critical operations log.
* `Transport` which can be used to securely route messages to other nodes on the network. (Note, securely here implies that the other nodes can authenticate the messages, and may be done with point-to-point secured links, or potentially with other schemes, such as a signed gossip message).
* `Hash` a cryptographically secure hashing implementation
* `Log` an application log for committed transactions and state transfer.


There is the [SerialProcessor](https://github.com/hyperledger-labs/mirbft/blob/master/processor.go) which performs operations serially (and therefore simply and deterministically), as well as the [ParallelProcessor](https://github.com/hyperledger-labs/mirbft/blob/master/processor.go) which is capable of significantly faster execution.

## Writing your own processor

Ultimately, it is the processor's responsibility to take the set of `Actions` provided by the state machine and to execute those actions.  If those actions have results, such as the `HashResult` or `CheckpointResult`, then the processor must inject those results back into the state machine.

In general, it is safe to continue to poll actions while the previous set of actions is processing, but this is almost always unnecessary.  Throughput should be constrained by thread contention with the serializer, and with throughput of the processor.  Therefore, instead, the processor should maintain exclusive read access to new actions and poll only for new actions after the current set of actions completes.

Before any network sends occur, it is critical that the requested WAL entries are persisted to disk in a safe manner (for instance, by performing an fsync at the end).  Similarly, it is important that requests are persisted before performing network sends.

Hashing may occur in parallel at any point and has no dependency on persistence.

Commits and checkpointing may safely begin immediately, as a commit implies that we have already broadcast our preprepare and prepare messages for that sequence number and it has been previously recorded into the WAL.

