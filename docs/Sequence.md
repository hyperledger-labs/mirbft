# Sequence State Machine

The most fundamental building block of the Mir state machine is the sequence number.  The sequence number transitions along the 3-phase commit, first from *Preprepared*, then to *Prepared*, and finally to *Committed*.  In the MirBFT library we add one additional state as part of the state machine transition.  This state is *Validated*.  This additional state provides an interim step which allows the heavy validation procedures to be performed outside of the state machine.  The validation, and other heavyweight activities like IO can be performed in parallel while the state machine itself is manipulated in a single-threaded fashion.

![A representation of the sequence state machine](http://yuml.me/diagram/plain/activity/(start)->(Preprepared)->(Validated)->(Prepared)->(Committed),(Preprepared)->(Invalid),(Validated)->(Timeout),(Prepared)->(Timeout))

At each state, messages from the rest of the network may acrue with or without causing a state transition.  However, each state (other than committed) triggers a new request to be propogated back to the caller, whose response may trigger the transition to the next state.

## Uninitialized -> Preprepared

The sequence state machine begins in an uninitialized state, and may only transition to a *Preprepared* state when receiving a _Preprepare_ message.  Although other messages such as _Prepare_ and even _Commit_ messages could arrive at this stage, they are simply collected but do not affect the state transition.  When the state transitions to *Preprepared*, the state machine returns a _ValidationRequest_ to the caller which must be serviced, and the results returned to the state machine.

## Preprepared -> Validated

The *Preprepared* state can only be transition to *Validated* when the caller of the library invokes _Advance_ with a _ValidationResult_ corresponding to the _ValidationRequest_ return by the transition to *Preprepared*.  The _ValidationResponse_ will contain whether the validation succeeded and, a computed digest for the batch.  If the validation did not succeed, the state machine transitions to an *Invalid* state and the network will require a view-change to make progress on this sequence number.  When the state machine transitions to *Validated* it returns a _Prepare_ message to be sent to the rest of the cluster (and itself).

## Validated -> Prepared

The *Validated* state transitions to the *Prepared* state once this node has received _2f_ prepares from the network (including its own).  Note that his transition only occurs when the node processes a _Prepare_ message, however, this is safe as the transition to *Validated* returns such a message which must be reprocessed by this node.  When the state transitions to *Prepared*, the state machine returns a _Commit_ message to be sent to the cluster (including itself).

## Prepared -> Committed

The *Prepared* state transitions to *Committed* once the node has received _2f+1_ _Commit_ messages from the cluster, including its own.  At this point, the caller may safely apply the batches in the log.
