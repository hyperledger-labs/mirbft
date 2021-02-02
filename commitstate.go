/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"

	pb "github.com/IBM/mirbft/mirbftpb"

	"google.golang.org/protobuf/proto"
)

// commitState represents our state, as reflected within our log watermarks.
// The mir network state only changes at checkpoint boundaries, and it
// is not possible for two different sets of network configuration state
// to exist within the same state structure.  Once the network state config
// commits, we wait for a strong checkpoint, then perform an epoch change
// and restart the state machine with the new network state as starting state.
// When a checkpoint result returns, it becomes the new activeState, the upperHalf
// of the committed sequences becomes the lowerHalf.
type commitState struct {
	persisted     *persisted
	clientTracker *clientTracker
	logger        Logger

	lowWatermark      uint64
	lastAppliedCommit uint64
	highestCommit     uint64
	stopAtSeqNo       uint64
	activeState       *pb.NetworkState
	lowerHalfCommits  []*pb.QEntry
	upperHalfCommits  []*pb.QEntry
	checkpointPending bool
	transferring      bool
}

func newCommitState(persisted *persisted, clientTracker *clientTracker, logger Logger) *commitState {
	cs := &commitState{
		clientTracker: clientTracker,
		persisted:     persisted,
		logger:        logger,
	}

	return cs
}

func (cs *commitState) reinitialize() *Actions {
	var lastCEntry, secondToLastCEntry *pb.CEntry
	var lastTEntry *pb.TEntry

	cs.persisted.iterate(logIterator{
		onCEntry: func(cEntry *pb.CEntry) {
			lastCEntry, secondToLastCEntry = cEntry, lastCEntry
		},
		onTEntry: func(tEntry *pb.TEntry) {
			lastTEntry = tEntry
		},
	})

	if secondToLastCEntry == nil || len(secondToLastCEntry.NetworkState.PendingReconfigurations) == 0 {
		cs.activeState = lastCEntry.NetworkState
		cs.lowWatermark = lastCEntry.SeqNo
	} else {
		cs.activeState = secondToLastCEntry.NetworkState
		cs.lowWatermark = secondToLastCEntry.SeqNo
	}

	ci := uint64(cs.activeState.Config.CheckpointInterval)
	if len(cs.activeState.PendingReconfigurations) == 0 {
		cs.stopAtSeqNo = lastCEntry.SeqNo + 2*ci
	} else {
		cs.stopAtSeqNo = lastCEntry.SeqNo + ci
	}

	cs.lastAppliedCommit = lastCEntry.SeqNo
	cs.highestCommit = lastCEntry.SeqNo

	cs.lowerHalfCommits = make([]*pb.QEntry, ci)
	cs.upperHalfCommits = make([]*pb.QEntry, ci)

	if lastTEntry == nil || lastCEntry.SeqNo >= lastTEntry.SeqNo {
		cs.logger.Log(LevelDebug, "reinitialized commit-state", "low_watermark", cs.lowWatermark, "stop_at_seq_no", cs.stopAtSeqNo, "len(pending_reconfigurations)", len(cs.activeState.PendingReconfigurations), "last_checkpoint_seq_no", lastCEntry.SeqNo)
		cs.transferring = false
		return &Actions{}
	}

	cs.logger.Log(LevelInfo, "reinitialized commit-state detected crash during state transfer", "target_seq_no", lastTEntry.SeqNo, "target_value", lastTEntry.Value)

	// We crashed during a state transfer
	cs.transferring = true
	return &Actions{
		StateTransfer: &StateTarget{
			SeqNo: lastTEntry.SeqNo,
			Value: lastTEntry.Value,
		},
	}
}

func (cs *commitState) transferTo(seqNo uint64, value []byte) *Actions {
	cs.logger.Log(LevelDebug, "initiating state transfer", "target_seq_no", seqNo, "target_value", value)
	assertEqual(cs.transferring, false, "multiple state transfers are not supported concurrently")
	cs.transferring = true
	return cs.persisted.addTEntry(&pb.TEntry{
		SeqNo: seqNo,
		Value: value,
	}).concat(&Actions{
		StateTransfer: &StateTarget{
			SeqNo: seqNo,
			Value: value,
		},
	})
}

func (cs *commitState) applyCheckpointResult(epochConfig *pb.EpochConfig, result *pb.CheckpointResult) *Actions {
	cs.logger.Log(LevelDebug, "applying checkpoint result", "seq_no", result.SeqNo, "value", result.Value)
	ci := uint64(cs.activeState.Config.CheckpointInterval)

	if cs.transferring {
		return &Actions{}
	}

	if result.SeqNo != cs.lowWatermark+ci {
		panic("dev sanity test -- this panic is helpful for dev, but needs to be removed as we could get stale checkpoint results")
	}

	if len(result.NetworkState.PendingReconfigurations) == 0 {
		cs.stopAtSeqNo = result.SeqNo + 2*ci
	} else {
		cs.logger.Log(LevelDebug, "checkpoint result has pending reconfigurations, not extending stop", "stop_at_seq_no", cs.stopAtSeqNo)
	}

	cs.activeState = result.NetworkState
	cs.lowerHalfCommits = cs.upperHalfCommits
	cs.upperHalfCommits = make([]*pb.QEntry, ci)
	cs.lowWatermark = result.SeqNo
	cs.checkpointPending = false

	return cs.persisted.addCEntry(&pb.CEntry{
		SeqNo:           result.SeqNo,
		CheckpointValue: result.Value,
		NetworkState:    result.NetworkState,
	}).send(
		cs.activeState.Config.Nodes,
		&pb.Msg{
			Type: &pb.Msg_Checkpoint{
				Checkpoint: &pb.Checkpoint{
					SeqNo: result.SeqNo,
					Value: result.Value,
				},
			},
		},
	)
}

func (cs *commitState) commit(qEntry *pb.QEntry) {
	assertEqual(cs.transferring, false, "we should never commit during state transfer")
	assertGreaterThanOrEqual(cs.stopAtSeqNo, qEntry.SeqNo, "commit sequence exceeds stop sequence")

	if qEntry.SeqNo <= cs.lowWatermark {
		// During an epoch change, we may be asked to
		// commit seqnos which we have already committed
		// (and cannot check), so ignore.
		return
	}

	if cs.highestCommit < qEntry.SeqNo {
		assertEqual(cs.highestCommit+1, qEntry.SeqNo, "next commit should always be exactly one greater than the highest")
		cs.highestCommit = qEntry.SeqNo
	}

	ci := uint64(cs.activeState.Config.CheckpointInterval)
	upper := qEntry.SeqNo-cs.lowWatermark > ci
	offset := int((qEntry.SeqNo - (cs.lowWatermark + 1)) % ci)
	var commits []*pb.QEntry
	if upper {
		commits = cs.upperHalfCommits
	} else {
		commits = cs.lowerHalfCommits
	}

	if commits[offset] != nil {
		assertTruef(bytes.Equal(commits[offset].Digest, qEntry.Digest), "previously committed %x but now have %x for seq_no=%d", commits[offset].Digest, qEntry.Digest, qEntry.SeqNo)
	} else {
		commits[offset] = qEntry
	}
}

func nextNetworkConfig(startingState *pb.NetworkState, clientConfigs []*pb.NetworkState_Client) (*pb.NetworkState_Config, []*pb.NetworkState_Client) {
	if len(startingState.PendingReconfigurations) == 0 {
		return startingState.Config, clientConfigs
	}

	nextConfig := proto.Clone(startingState.Config).(*pb.NetworkState_Config)
	nextClients := append([]*pb.NetworkState_Client{}, clientConfigs...)
	for _, reconfig := range startingState.PendingReconfigurations {
		switch rc := reconfig.Type.(type) {
		case *pb.Reconfiguration_NewClient_:
			clientConfigs = append(clientConfigs, &pb.NetworkState_Client{
				Id:    rc.NewClient.Id,
				Width: rc.NewClient.Width,
			})
		case *pb.Reconfiguration_RemoveClient:
			found := false
			for i, clientConfig := range nextClients {
				if clientConfig.Id != rc.RemoveClient {
					continue
				}

				found = true
				nextClients = append(nextClients[:i], nextClients[i+1:]...)
				break
			}

			// TODO, heavy handed, back off to a warning
			assertTruef(found, "asked to remove client %d which doesn't exist", rc.RemoveClient)
		case *pb.Reconfiguration_NewConfig:
			nextConfig = rc.NewConfig
		}
	}

	return nextConfig, clientConfigs
}

// drain returns all available Commits (including checkpoint requests)
func (cs *commitState) drain() []*Commit {
	ci := uint64(cs.activeState.Config.CheckpointInterval)

	var result []*Commit
	for cs.lastAppliedCommit < cs.lowWatermark+2*ci {
		if cs.lastAppliedCommit == cs.lowWatermark+ci && !cs.checkpointPending {
			clientState := cs.clientTracker.computeNewClientStates(cs.lastAppliedCommit)
			networkConfig, clientConfigs := nextNetworkConfig(cs.activeState, clientState)
			result = append(result, &Commit{
				Checkpoint: &Checkpoint{
					SeqNo:         cs.lastAppliedCommit,
					NetworkConfig: networkConfig,
					ClientsState:  clientConfigs,
				},
			})

			cs.checkpointPending = true
			cs.logger.Log(LevelDebug, "all previous sequences has committed, requesting checkpoint", "seq_no", cs.lastAppliedCommit)

		}

		nextCommit := cs.lastAppliedCommit + 1
		upper := nextCommit-cs.lowWatermark > ci
		offset := int((nextCommit - (cs.lowWatermark + 1)) % ci)
		var commits []*pb.QEntry
		if upper {
			commits = cs.upperHalfCommits
		} else {
			commits = cs.lowerHalfCommits
		}
		commit := commits[offset]
		if commit == nil {
			break
		}

		assertEqual(commit.SeqNo, nextCommit, "attempted out of order commit")

		result = append(result, &Commit{
			Batch: commit,
		})
		cs.clientTracker.markCommitted(commit)
		cs.lastAppliedCommit = nextCommit
	}

	return result
}
