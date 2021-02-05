/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"

	pb "github.com/IBM/mirbft/mirbftpb"
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
	persisted         *persisted
	committingClients map[uint64]*committingClient
	logger            Logger

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

func newCommitState(persisted *persisted, logger Logger) *commitState {
	cs := &commitState{
		persisted: persisted,
		logger:    logger,
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

	cs.committingClients = map[uint64]*committingClient{}
	for _, clientState := range lastCEntry.NetworkState.Clients {
		cs.committingClients[clientState.Id] = newCommittingClient(lastCEntry.SeqNo, clientState)
	}

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

func nextNetworkConfig(startingState *pb.NetworkState, committingClients map[uint64]*committingClient) (*pb.NetworkState_Config, []*pb.NetworkState_Client) {
	nextConfig := startingState.Config

	nextClients := make([]*pb.NetworkState_Client, len(startingState.Clients))
	for i, oldClientState := range startingState.Clients {
		cc, ok := committingClients[oldClientState.Id]
		assertTrue(ok, "must have a committing client instance all client states")
		nextClients[i] = cc.createCheckpointState()
	}

	for _, reconfig := range startingState.PendingReconfigurations {
		switch rc := reconfig.Type.(type) {
		case *pb.Reconfiguration_NewClient_:
			nextClients = append(nextClients, &pb.NetworkState_Client{
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

	return nextConfig, nextClients
}

// drain returns all available Commits (including checkpoint requests)
func (cs *commitState) drain() []*Commit {
	ci := uint64(cs.activeState.Config.CheckpointInterval)

	var result []*Commit
	for cs.lastAppliedCommit < cs.lowWatermark+2*ci {
		if cs.lastAppliedCommit == cs.lowWatermark+ci && !cs.checkpointPending {
			networkConfig, clientConfigs := nextNetworkConfig(cs.activeState, cs.committingClients)

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

		for _, req := range commit.Requests {
			cs.committingClients[req.ClientId].markCommitted(commit.SeqNo, req.ReqNo)
		}

		cs.lastAppliedCommit = nextCommit
	}

	return result
}

type committingClient struct {
	lastState                    *pb.NetworkState_Client
	committedSinceLastCheckpoint []*uint64
}

func newCommittingClient(seqNo uint64, clientState *pb.NetworkState_Client) *committingClient {
	committedSinceLastCheckpoint := make([]*uint64, clientState.Width)
	mask := bitmask(clientState.CommittedMask)
	for i := 0; i < mask.bits(); i++ {
		if !mask.isBitSet(i) {
			continue
		}
		committedSinceLastCheckpoint[i] = &seqNo
	}

	return &committingClient{
		lastState:                    clientState,
		committedSinceLastCheckpoint: committedSinceLastCheckpoint,
	}

}

func (cc *committingClient) markCommitted(seqNo, reqNo uint64) {
	offset := reqNo - cc.lastState.LowWatermark
	cc.committedSinceLastCheckpoint[offset] = &seqNo
}

func (cc *committingClient) createCheckpointState() (newState *pb.NetworkState_Client) {
	defer func() {
		cc.lastState = newState
	}()

	var firstUncommitted, lastCommitted *uint64

	for i, seqNoPtr := range cc.committedSinceLastCheckpoint {
		reqNo := cc.lastState.LowWatermark + uint64(i)
		if seqNoPtr != nil {
			lastCommitted = &reqNo
			continue
		}
		if firstUncommitted == nil {
			firstUncommitted = &reqNo
		}
	}

	if lastCommitted == nil {
		return &pb.NetworkState_Client{
			Id:                          cc.lastState.Id,
			Width:                       cc.lastState.Width,
			WidthConsumedLastCheckpoint: 0,
			LowWatermark:                cc.lastState.LowWatermark,
		}
	}

	if firstUncommitted == nil {
		highWatermark := cc.lastState.LowWatermark + uint64(cc.lastState.Width) - uint64(cc.lastState.WidthConsumedLastCheckpoint) - 1
		assertEqual(*lastCommitted, highWatermark, "if no client reqs are uncommitted, then all though the high watermark should be committed")

		cc.committedSinceLastCheckpoint = []*uint64{}
		return &pb.NetworkState_Client{
			Id:                          cc.lastState.Id,
			Width:                       cc.lastState.Width,
			WidthConsumedLastCheckpoint: cc.lastState.Width,
			LowWatermark:                *lastCommitted + 1,
		}
	}

	widthConsumed := int(*firstUncommitted - cc.lastState.LowWatermark)
	cc.committedSinceLastCheckpoint = cc.committedSinceLastCheckpoint[widthConsumed:]
	cc.committedSinceLastCheckpoint = append(cc.committedSinceLastCheckpoint, make([]*uint64, int(cc.lastState.Width)-widthConsumed)...)

	var mask bitmask
	if *lastCommitted != *firstUncommitted {
		mask = bitmask(make([]byte, int(*lastCommitted-*firstUncommitted)/8+1))
		for i := 0; i <= int(*lastCommitted-*firstUncommitted); i++ {
			if cc.committedSinceLastCheckpoint[i] == nil {
				continue
			}

			assertNotEqualf(i, 0, "the first uncommitted cannot be marked committed: firstUncommitted=%d, lastCommitted=%d slice=%+v", *firstUncommitted, *lastCommitted, cc.committedSinceLastCheckpoint)

			mask.setBit(i)
		}
	}

	return &pb.NetworkState_Client{
		Id:                          cc.lastState.Id,
		Width:                       cc.lastState.Width,
		LowWatermark:                *firstUncommitted,
		WidthConsumedLastCheckpoint: uint32(widthConsumed),
		CommittedMask:               mask,
	}
}
