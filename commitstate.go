/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"

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

	lowWatermark      uint64
	lastAppliedCommit uint64
	highestCommit     uint64
	stopAtSeqNo       uint64
	activeState       *pb.NetworkState
	lowerHalfCommits  []*pb.QEntry
	upperHalfCommits  []*pb.QEntry
	checkpointPending bool
}

func newCommitState(persisted *persisted, clientTracker *clientTracker) *commitState {
	cs := &commitState{
		clientTracker: clientTracker,
		persisted:     persisted,
	}

	return cs
}

func (cs *commitState) reinitialize() {
	var lastCEntry, secondToLastCEntry *pb.CEntry

	cs.persisted.iterate(logIterator{
		onCEntry: func(cEntry *pb.CEntry) {
			lastCEntry, secondToLastCEntry = cEntry, lastCEntry
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

	// fmt.Printf("JKY: initialized stopAtSeqNo to %d -- lowWatermark=%d pc=%d lastCEntry=%d\n", cs.stopAtSeqNo, cs.lowWatermark, len(cs.activeState.PendingReconfigurations), lastCEntry.SeqNo)
}

func (cs *commitState) applyCheckpointResult(epochConfig *pb.EpochConfig, result *pb.CheckpointResult) *Actions {
	// fmt.Printf("JKY: Applying checkpoint result for seqNo=%d\n", result.SeqNo)
	ci := uint64(cs.activeState.Config.CheckpointInterval)

	if result.SeqNo != cs.lowWatermark+ci {
		panic("dev sanity test -- should remove as we could get stale checkpoint results")
	}

	if len(result.NetworkState.PendingReconfigurations) == 0 {
		cs.stopAtSeqNo = result.SeqNo + 2*ci
		// fmt.Printf("JKY: increasing stop at seqno to seqNo=%d\n", cs.stopAtSeqNo)
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
	).concat(cs.clientTracker.drain())
}

func (cs *commitState) commit(qEntry *pb.QEntry) {
	if qEntry.SeqNo > cs.stopAtSeqNo {
		panic(fmt.Sprintf("dev sanity test -- asked to commit %d, but we asked to stop at %d", qEntry.SeqNo, cs.stopAtSeqNo))
	}

	if cs.highestCommit < qEntry.SeqNo {
		if cs.highestCommit+1 != qEntry.SeqNo {
			panic(fmt.Sprintf("dev sanity test -- asked to commit %d, but highest commit was %d", qEntry.SeqNo, cs.highestCommit))
		}
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
		if !bytes.Equal(commits[offset].Digest, qEntry.Digest) {
			panic(fmt.Sprintf("dev sanity check -- previously committed %x but now have %x for seqNo=%d", commits[offset].Digest, qEntry.Digest, qEntry.SeqNo))
		}
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

			if !found {
				panic("asked to remove client which doesn't exist") // TODO, heavy handed, back off to a warning
			}
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
			clientState := cs.clientTracker.commitsCompletedForCheckpointWindow(cs.lastAppliedCommit)
			networkConfig, clientConfigs := nextNetworkConfig(cs.activeState, clientState)
			result = append(result, &Commit{
				Checkpoint: &Checkpoint{
					SeqNo:         cs.lastAppliedCommit,
					NetworkConfig: networkConfig,
					ClientsState:  clientConfigs,
				},
			})

			cs.checkpointPending = true
			// fmt.Printf("--- JKY: adding checkpoint request for seq_no=%d to action results\n", cs.lastAppliedCommit)

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

		if commit.SeqNo != nextCommit {
			panic(fmt.Sprintf("dev sanity check -- expected seqNo=%d == commit=%d, upper=%v\n", nextCommit, commit.SeqNo, upper))
		}

		result = append(result, &Commit{
			Batch: commit,
		})
		for _, fr := range commit.Requests {
			// fmt.Printf("JKY: Attempting to commit seqNo=%d with req=%d.%d\n", commit.SeqNo, fr.ClientId, fr.ReqNo)
			cw, ok := cs.clientTracker.client(fr.ClientId)
			if !ok {
				panic("we never should have committed this without the client available")
			}
			cw.reqNo(fr.ReqNo).committed = &commit.SeqNo
		}

		cs.lastAppliedCommit = nextCommit
	}

	return result
}
