# Sequence State Machine

*Note this is seriously out of date, and needs to be updated*.

The most fundamental building block of the Mir state machine is the sequence number.  The sequence number transitions along the 3-phase commit, first from *Preprepared*, then to *Prepared*, and finally to *Committed*.  In the MirBFT library we add two additional states as part of the state machine transition, *Digested* and *Validated*.  These additional states provides an interim step which allows the heavy hashing and validation procedures to be performed outside of the state machine.  The validation, and other heavyweight activities like IO can be performed in parallel while the state machine itself is manipulated in a single-threaded fashion.

![A representation of the sequence state machine](http://yuml.me/diagram/plain/activity/(start)->(Preprepared)->(Digested)->(Validated)->(Prepared)->(Committed),(Digested)->(Invalid),(Validated))

At each state, messages from the rest of the network may acrue with or without causing a state transition.  However, each state triggers a new request to be propogated back to the caller, whose response may trigger the transition to the next state.

## Uninitialized -> Preprepared

The sequence state machine begins in an uninitialized state, and may only transition to a *Preprepared* state when receiving a _Preprepare_ message.  Although other messages such as _Prepare_ and even _Commit_ messages could arrive at this stage, they are simply collected but do not affect the state transition.  When the state transitions to *Preprepared*, the state machine returns a _DigestRequest_ to the caller which must be serviced, returning a digest for the batch included in the _Preprepare_.

## Preprepared -> Digested
The *Preprepared* state can only be transitioned to *Digested* when the caller fo the library returns a *DigestResult* in a call to `AddResults`.  When the state transitions to *Digested*, the state machine returns a _ValidationRequest_ to the caller which must be serviced, and the results returned to the state machine.

## Digested -> Validated

The *Preprepared* state can only be transition to *Validated* when the caller of the library invokes `AddResults` with a _ValidationResult_ corresponding to the _ValidationRequest_ return by the transition to *Digested*.  The _ValidationResponse_ will contain whether the validation succeeded and, a computed digest for the batch.  If the validation did not succeed, the state machine transitions to an *Invalid* state and the network will require a view-change to make progress on this sequence number.  When the state machine transitions to *Validated* it returns a _Prepare_ message to be sent to the rest of the cluster (and itself).

## Validated -> Prepared

The *Validated* state transitions to the *Prepared* state once this node has received _2f_ prepares from the network (including its own).  Note that his transition only occurs after the node has processed its own  _Prepare_ message.  This is to avoid the possibility (due to network asynchrony) that a later sequence number prepares before a prior one.

## Prepared -> Committed

The *Prepared* state transitions to *Committed* once the node has received _2f+1_ _Commit_ messages from the cluster, including its own.  At this point, the caller may safely apply the batches in the log.  Note, this state transition may only occur after the node's own commit has been applied, for the same safety reasons as the prepared transition.
