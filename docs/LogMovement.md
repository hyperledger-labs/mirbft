# Log Movement

For those looking to understand the internal workings fo the MirBFT state machine, perhaps no other detail is more important than the write-ahead-log (WAL) movement.  The WAL durably records important state transitions within the MirBFT state machine so that the node may safely crash, and resume operation without inducing potentially byzantine faults.

Given the BFT nature of MirBFT, a natural question is "Why we even care about inducing byzantine faults during a crash?".  The answer lies in the fact that in a real world system, simultaenous crash faults exceeding the byzantine assumptions are not unusual, or unexpected.  Without a WAL, if all nodes are deployed into the same datacenter (or, in testing scenarios onto the same VM), a single crash outage can induce sufficient byzantine faults to break safety in the system.

## WAL entry types

The WAL contains entries for tracking the current epoch configuration, the current network state, and the current state of the three-phase commit for the in-window sequence numbers.  The types and their roles follow.

* `PEntry` - As in the original PBFT paper, a PEntry is appended to the log whenever a sequence prepares.  This s when any replica sends a Commit message.  Additionally, when any replica sends a NewEpochReady message the sequences referenced in the new epoch configuration have PEntries appended to the log.
* `QEntry` - As in the original PBFT paper, a QEntry is appended to the log whenever a sequence preprepares.  This could be as a leader sending the Preprepare message for a sequence, or as a follower when replying with a Prepare message.  Additionally, when a leader sends its NewEpoch message, or a follower replies with a NewEpochEcho the sequences referenced in the new epoch configuration have QEntries appended to the log.
* `CEntry` - Once the sequences for a checkpoint interval have committed, and the application has responded with the checkpoint value, and any pending configuration changes, a CEntry with the checkpoint sequence, value, current network state, and pending configurations is appended to the log.  The CEntry is critical for WAL truncation.
* `NEntry` - An NEntry indicates that new sequences have been allocated.  An NEntry has a sequence number corresponding to the beginning of a checkpoint interval, and also contains the epoch configuration under which the sequences may commit.
* `FEntry` - An FEntry indicates that the previous checkpoint is both stable and corresponds to the end of an epoch.  This is used for graceful epoch rotation as well as reconfiguration.
* `ECEntry` - Indicates that an epoch change message has been sent, generated by the previously recorded entries in the log.
* `TEntry` - Indicates that state transfer is required and in progress.

## WAL Truncation

The WAL has two sorts of operations, append and truncate.  The bulk of the data written to the WAL is related to requests, `PEntry` and `QEntry` entries which correspond to a sequence, and batch of requests, but these entries can be largely ignored with respect to watermark movement.  Instead, we care about two types of entries, those of type checkpoint entry (`CEntry`) and those of type allocation entry (either `NEntry` or `FEntry`).

The WAL _always_ contains at least `CEntry` and one `NEntry` or `FEntry`.  The `NEntry` indicates that new sequences have been allocated under a given epoch configuration and always has a sequence number less than the next `CEntry`. On the other hand, an `FEntry` indicates that new sequences have been deliberately not allocated, because of a pending graceful epoch change or reconfiguration. To begin, let's focus on normal WAL movement which requires only `NEntry`s.

### Normal case movement

During normal operation, new `NEntry` entries are appended to the log after corresponding `CEntry` entries.  When `CEntry` for sequence `s` is appended, if the epoch is active (not ending because of graceful or ungraceful epoch change, or reconfiguration), then an `NEntry` is appended for sequence `s+k+1` where `k` is the checkpoint interval.  When a checkpoint for sequence `r` becomes stable, the log is truncated to the `NEntry` with sequence `r+1`.  Since the `NEntry` with sequence `r+1` was allocated in response to the checkpoint with sequence `(r+1) = s+k+1` we know that `s = r-k`, the checkpoint before the latest stable one.  Consequently, we know that the `NEntry` with sequence `r+1` always appears _before_ the `CEntry` with sequence `r`.  Consider the following diagram of normal log movement for a log with a checkpoint interval of 5, where `C_{seqno1}` and `N_{seqno2}` indicate a `CEntry` and `NEntry` with sequence seqno1 and seqno2 respectively.

```
N_21 ... C_20 N_26 ... C_25 N_31 ... C_30                    # C_25 becomes stable
              N_26 ... C_25 N_31 ... C_30 N_36 ...           # C_30 becomes stable
                            N_31 ... C_30 N_36 ...           # More commits
                            N_31 ... C_30 N_36 ... C_40 N_41 # C_40 generated
```

But, it's always possible that the epoch is interrupted during normal operation by an ungraceful epoch change.  When this occurs, we record an `ECEntry` into the log, indicating that, based on the log entries up to the ECEntry, an EpochChange message was computed and disseminated.  At this point, it is critical that no truncations occurs until a new epoch becomes active.  This is because in the event of a crash, the node must re-compute an identical epoch change and any log truncation would impact this.

It was considered during development that we take an approach closer to the one in the PBFT paper -- writing a large full epoch change entry, containing the existing PSet and QSet, then immediately truncate the log.  However, this has a few drawbacks.  First, it requires writing duplicative entries to the log at every epoch change; the PSet and QSet are already in the log, there is no need to rewrite it.  Secondly, truncating the log to the epoch change point also truncates CEntries, which contain network state.  So, this network state would also need to be duplicated and embedded into the entry containing the epoch change.  It would further require more sophisticated parsing of the log for components to identify checkpoints occurring either embedded within epoch changes or within `CEntry`s.  All things considered, it seemed simpler to simply check that some epoch is active before truncating.

Consider the following example where `EC_{number}` indicates an epoch change sent for epoch `number` and the node is currently in epoch 1.

```
N_21 ... C_20 N_26 ... EC_2                      # C_20 is stable, EpochChange computed
N_21 ... C_20 N_26 ... EC_2 C_25                 # Checkpoint C_25 is computed and applied
N_21 ... C_20 N_26 ... EC_2 C_25                 # Checkpoint C_25 becomes stable but no GC
N_21 ... C_20 N_26 ... EC_2 C_25 EC_3            # Epoch 2 does not start, EpochChange computed
              N_26 ... EC_2 C_25 EC_3 N_31       # Epoch 3 starts, log is immediately truncated
```


### Epoch ending movement

Epochs can end gracefully for one of two reasons.  Firstly, an epoch may simply reach its planned expiration.  This occurs to prevent a byzantine replica from censoring requests in a particular bucket indefinitely, and depending on configuration may occur frequently, or infrequently.  Secondly, an epoch will end gracefully any time a configuration change commits.  This could be the addition of a new client, tweaking of the number of buckets, adding a new node, etc.

In either case, the state machine handles the epoch ending in the same way.  We require that when an epoch ends gracefully, that it ends on a checkpoint boundary (therefore planned epoch expiration must always land on an expiration boundary, and configuration only applies on checkpoint boundaries).  If an epoch is set to end at sequence `s`, then when checkpoint `s-k` (where `k` is the checkpoint interval) is appended to the log, we do not append an `NEntry` as we would in the normal operation case.  Instead, we simply wait for checkpoint `s` to become stable, at which point we append an `FEntry` to the log, and truncate the log to `CEntry` `s`.  If on startup, the log contains an `FEntry` but has not been truncated, we perform the truncation before initializing.

Once the log contains only the `CEntry` for `s` and the `FEntry` (note, no `PEntry`, `QEntry`, or `NEntry` may appear between the `CEntry` and `FEntry`), we append an epoch change for the next epoch to our logs, and wait for the new primary to start the next epoch.  At this point, we append the new `NEntry` as normal, and proceed with standard watermark movement.

Note, the `FEntry` is necessary because we can only conclude that the epoch ended normally if we observe a strong quorum for the checkpoint.  Once there is a strong quorum for the checkpoint we are guaranteed that even if the new epoch does not start gracefully, we will not need to reference any of the previous epoch `PEntry` or `QEntry` values in our epoch change computation.

## Computing the EpochChange

Given the log structure, computing the epoch change message actually becomes trivial and can be done in a deterministic way, and we can even get away without persisting the generated message.  If our log starts with an `NEntry` (as in the normal case), we simply concatenate all of the `CEntry` sequence and value pairs to form the CSet, then we iterate over the `QEntry` values using the epoch context from the `NEntry` to form the QSet.  Because the PSet should not contain duplicate entries, we first determine the unique sequences, and then construct the PSet through iteration just like the QSet.  Note, that because the WAL is never truncated while no epoch is active, it's actually possible to compute any epoch change since the last active epoch ended simply by specifying an epoch number and ignoring future epoch entries.  In the case that the log contains no `NEntry` and only an `FEntry` the CSet is the sole `CEntry` and the PSet and QSet are empty.

## State Transfer

The watermark movement works naturally, so long as a replica stays in sync with the rest of the network.  In general, once a replica has been offline for any significant period of time, when it starts again, it will need to perform state transfer to the latest state before it can begin consenting.  It's also possible during new epoch computation that the starting checkpoint is unreachable from the replica's current state.  In either case, we introduce a new type log entry, a `TEntry` indicating that state transfer has been requested.

At startup, if the last entry in the log is a state transfer entry, the replica will attempt to reinitialize state transfer.  If state transfer fails, a new state target will be chosen, and a new `TEntry` will be appended to the log.  A log may contain 1 or more `TEntry`s in a row, but the first non-`TEntry` log record will be a `CEntry` corresponding to the most recent `TEntry`'s checkpoint value, and containing the current network state.

While a replica is in state transfer, it continues to buffer messages, ideally so that once state transfer is complete, the replica can rapidly catch up to the current state of the network, by playing forward these buffers.  If the buffers are exhausted, then multiple rounds of state transfer may be necessary before the replica catches up.

## Advanced Topics

Although at the beginning of this section we asked the question "Why we even care about inducing byzantine faults during a crash?", the answer assumes certain 'real world' conditions of a deployment.  If your deployment makes different assumptions, it may be desirable to implement a custom WAL which either does not give strong sync characteristics, or delibarely skips persisting certain entries (in particular `PEntry` and `QEntry` entries).  Depending on workload, and risk tolerance, the performance benefits to such an optimization may be worthwhile -- although we expect most users will want to operate with a WAL in its standard configuration.