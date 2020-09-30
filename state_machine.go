/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"

	"google.golang.org/protobuf/proto"
)

// bucketID is the identifier for a bucket.  It is a simple alias to a uint64, but
// is used to help disambiguate function signatures which accept multiple uint64
// values with different meanings.
type bucketID uint64

// nodeID represents the identifier assigned to a node.  It is a simple alias to a uint64, but
// is used to help disambiguate function signatures which accept multiple uint64
// values with different meanings.
type nodeID uint64

type stateMachineState int

const (
	smUninitialized stateMachineState = iota
	smLoadingPersisted
	smInitialized
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
	lowWatermark     uint64
	lastCommit       uint64
	stopAtSeqNo      uint64
	activeState      *pb.NetworkState
	clientTracker    *clientTracker
	lowerHalfCommits []*pb.QEntry
	upperHalfCommits []*pb.QEntry
}

func newCommitState(initialCEntry *pb.CEntry, clientTracker *clientTracker) *commitState {
	ci := int(initialCEntry.NetworkState.Config.CheckpointInterval)

	stopAtSeqNo := initialCEntry.SeqNo + uint64(ci)
	if len(initialCEntry.NetworkState.PendingReconfigurations) == 0 {
		stopAtSeqNo += uint64(ci)
	}

	return &commitState{
		lowWatermark:     initialCEntry.SeqNo,
		lastCommit:       initialCEntry.SeqNo,
		activeState:      initialCEntry.NetworkState,
		clientTracker:    clientTracker,
		lowerHalfCommits: make([]*pb.QEntry, ci),
		upperHalfCommits: make([]*pb.QEntry, ci),
		stopAtSeqNo:      stopAtSeqNo,
	}
}

func (cs *commitState) applyCheckpointResult(result *pb.CheckpointResult) {
	fmt.Printf("JKY: Applying checkpoint result for seqNo=%d\n", result.SeqNo)
	ci := uint64(cs.activeState.Config.CheckpointInterval)

	if result.SeqNo != cs.lowWatermark+ci {
		panic("dev sanity test -- should remove as we could get stale checkpoint results")
	}

	if len(result.NetworkState.PendingReconfigurations) == 0 {
		cs.stopAtSeqNo += ci
		fmt.Printf("JKY: increasing stop at seqno to seqNo=%d\n", cs.stopAtSeqNo)
	}

	cs.activeState = result.NetworkState
	cs.lowerHalfCommits = cs.upperHalfCommits
	cs.lowWatermark = result.SeqNo
}

func (cs *commitState) commit(qEntry *pb.QEntry) {
	if qEntry.SeqNo > cs.stopAtSeqNo {
		panic(fmt.Sprintf("dev sanity test -- asked to commit %d, but we asked to stop at %d", qEntry.SeqNo, cs.stopAtSeqNo))
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
	for _, reconfig := range startingState.PendingReconfigurations {
		switch rc := reconfig.Type.(type) {
		case *pb.Reconfiguration_NewClient:
			clientConfigs = append(clientConfigs, &pb.NetworkState_Client{
				Id:    rc.NewClient,
				Width: 5000, // XXX this needs to be configured either in the new client msg or in the overall config
			})
		case *pb.Reconfiguration_RemoveClient:
			// XXX implement
			panic("unimplemented removing clent -- implement me")
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
	for cs.lastCommit < cs.lowWatermark+2*ci {
		upper := cs.lastCommit-cs.lowWatermark > ci
		offset := int((cs.lastCommit - (cs.lowWatermark + 1)) % ci)
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

		result = append(result, &Commit{
			Batch: commit,
		})

		for _, fr := range commit.Requests {
			cw, ok := cs.clientTracker.client(fr.ClientId)
			if !ok {
				panic("we never should have committed this without the client available")
			}
			cw.reqNo(fr.ReqNo).committed = &commit.SeqNo
		}

		if !upper && cs.lastCommit == cs.lowWatermark+ci {
			networkConfig, clientConfigs := nextNetworkConfig(cs.activeState, cs.clientTracker.clientConfigs())
			result = append(result, &Commit{
				Checkpoint: &Checkpoint{
					SeqNo:         cs.lastCommit,
					NetworkConfig: networkConfig,
					ClientsState:  clientConfigs,
				},
			})
		}

		cs.lastCommit++
	}

	return result
}

// StateMachine contains a deterministic processor for mirbftpb state events.
// This structure should almost never be initialized directly but should instead
// be allocated via StartNode.
type StateMachine struct {
	Logger Logger

	state stateMachineState

	myConfig      *pb.StateEvent_InitialParameters
	commitState   *commitState
	clientTracker *clientTracker

	batchTracker      *batchTracker
	checkpointTracker *checkpointTracker
	epochTracker      *epochTracker
	persisted         *persisted
}

func (sm *StateMachine) initialize(parameters *pb.StateEvent_InitialParameters) {
	if sm.state != smUninitialized {
		panic("state machine has already been initialized")
	}

	sm.myConfig = parameters
	sm.state = smLoadingPersisted
	sm.persisted = newPersisted(sm.Logger)
}

func (sm *StateMachine) applyPersisted(entry *WALEntry) {
	if sm.state != smLoadingPersisted {
		panic("state machine has already finished loading persisted data")
	}

	sm.persisted.appendInitialLoad(entry)
}

func (sm *StateMachine) completeInitialization() {
	if sm.state != smLoadingPersisted {
		panic("state machine has already finished loading persisted data")
	}

	var checkpoints []*pb.CEntry
	for el := sm.persisted.logHead; el != nil; el = el.next {
		cEntryType, ok := el.entry.Type.(*pb.Persistent_CEntry)
		if !ok {
			continue
		}

		checkpoints = append(checkpoints, cEntryType.CEntry)
	}

	if len(checkpoints) == 0 {
		panic("no checkpoints in log")
	}

	if len(checkpoints) > 3 {
		panic("this seems wrong... maybe it's okay, TODO, analyse")
	}

	sm.checkpointTracker = newCheckpointTracker(sm.persisted, sm.myConfig, sm.Logger)
	sm.clientTracker = newClientWindows(sm.persisted, sm.myConfig, sm.Logger)

	sm.commitState = newCommitState(checkpoints[len(checkpoints)-1], sm.clientTracker)

	sm.batchTracker = newBatchTracker(sm.persisted)

	sm.epochTracker = newEpochTracker(
		sm.persisted,
		sm.commitState,
		sm.checkpointTracker.networkConfig,
		sm.Logger,
		sm.myConfig,
		sm.batchTracker,
		sm.clientTracker,
	)

	sm.state = smInitialized
}

func injectCheckpoint(cp *Checkpoint, i int, commits []*Commit) []*Commit {
	lhs, rhs := commits[:i], commits[i:]
	result := make([]*Commit, len(commits)+1)
	copy(result, lhs)
	copy(result[i+1:], rhs)
	result[i] = &Commit{
		Checkpoint: cp,
	}

	return result
}

func (sm *StateMachine) ApplyEvent(stateEvent *pb.StateEvent) *Actions {
	assertInitialized := func() {
		if sm.state != smInitialized {
			panic("cannot apply events to an uninitialized state machine")
		}
	}

	actions := &Actions{}

	switch event := stateEvent.Type.(type) {
	case *pb.StateEvent_Initialize:
		sm.initialize(event.Initialize)
		return &Actions{}
	case *pb.StateEvent_LoadEntry:
		sm.applyPersisted(&WALEntry{
			Index: event.LoadEntry.Index,
			Data:  event.LoadEntry.Data,
		})
		return &Actions{}
	case *pb.StateEvent_CompleteInitialization:
		sm.completeInitialization()
		return &Actions{}
	case *pb.StateEvent_Tick:
		assertInitialized()
		actions.concat(sm.clientTracker.tick())
		actions.concat(sm.epochTracker.tick())
	case *pb.StateEvent_Step:
		assertInitialized()
		actions.concat(sm.step(
			nodeID(event.Step.Source),
			event.Step.Msg,
		))
	case *pb.StateEvent_Propose:
		assertInitialized()
		actions.concat(sm.propose(
			event.Propose.Request,
		))
	case *pb.StateEvent_AddResults:
		assertInitialized()
		actions.concat(sm.processResults(
			event.AddResults,
		))
	case *pb.StateEvent_ActionsReceived:
		// This is a bit odd, in that it's a no-op, but it's harmless
		// and allows for much more insightful playback events (allowing
		// us to tie action results to a particular set of actions)
		return &Actions{}
	default:
		panic(fmt.Sprintf("unknown state event type: %T", stateEvent.Type))
	}

	// A nice guarantee we have, is that for any given event, at most, one watermark movement is
	// required.  It is not possible for the watermarks to move twice, as it would require
	// new checkpoint messages from ourselves, and because of reconfiguration, we can only generate
	// a checkpoint request after the previous checkpoint requests has been returned (because
	// the checkpoint result includes any pending reconfiguration which must be reflected in
	// the next checkpoint.)
	if sm.checkpointTracker.state == cpsGarbageCollectable {
		newLow := sm.checkpointTracker.garbageCollect()
		fmt.Printf("JKY: garbage collecting to %d\n", newLow)

		sm.clientTracker.garbageCollect(newLow)
		if newLow > uint64(sm.checkpointTracker.networkConfig.CheckpointInterval) {
			// Note, we leave an extra checkpoint worth of batches around, to help
			// during epoch change.
			sm.batchTracker.truncate(newLow - uint64(sm.checkpointTracker.networkConfig.CheckpointInterval))
		}
		sm.persisted.truncate(newLow)
		actions.concat(sm.epochTracker.moveLowWatermark(newLow))
	}

	for {
		// We note all of the commits that occured in response to the current event
		// as well as any watermark movement.  Then, based on this information we
		// may continue to iterate the state machine, and do so, so long as
		// attempting to advance the state causes new actions.

		actions.concat(&Actions{
			Commits: sm.commitState.drain(),
		})

		loopActions := sm.epochTracker.advanceState()
		loopActions.concat(sm.clientTracker.advanceState())
		if loopActions.isEmpty() {
			break
		}

		actions.concat(loopActions)
	}

	return actions
}

func (sm *StateMachine) propose(requestData *pb.Request) *Actions {
	data := [][]byte{
		uint64ToBytes(requestData.ClientId),
		uint64ToBytes(requestData.ReqNo),
		requestData.Data,
	}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: data,
				Origin: &pb.HashResult{
					Type: &pb.HashResult_Request_{
						Request: &pb.HashResult_Request{
							Source:  sm.myConfig.Id,
							Request: requestData,
						},
					},
				},
			},
		},
	}
}

func (sm *StateMachine) step(source nodeID, msg *pb.Msg) *Actions {
	actions := &Actions{}
	switch msg.Type.(type) {
	case *pb.Msg_RequestAck:
		return actions.concat(sm.clientTracker.step(source, msg))
	case *pb.Msg_FetchRequest:
		return actions.concat(sm.clientTracker.step(source, msg))
	case *pb.Msg_ForwardRequest:
		return actions.concat(sm.clientTracker.step(source, msg))
	case *pb.Msg_Checkpoint:
		sm.checkpointTracker.step(source, msg)
		return &Actions{}
	case *pb.Msg_FetchBatch:
		// TODO decide if we want some buffering?
		return sm.batchTracker.step(source, msg)
	case *pb.Msg_ForwardBatch:
		// TODO decide if we want some buffering?
		return sm.batchTracker.step(source, msg)
	case *pb.Msg_Suspect:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_EpochChange:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_EpochChangeAck:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_NewEpoch:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_NewEpochEcho:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_NewEpochReady:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_Preprepare:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_Prepare:
		return sm.epochTracker.step(source, msg)
	case *pb.Msg_Commit:
		return sm.epochTracker.step(source, msg)
	default:
		panic(fmt.Sprintf("unexpected bad message type %T", msg.Type))
	}
}

func (sm *StateMachine) processResults(results *pb.StateEvent_ActionResults) *Actions {
	actions := &Actions{}

	for _, checkpointResult := range results.Checkpoints {
		// sm.Logger.Debug("applying checkpoint result", zap.Uint64("SeqNo", checkpointResult.SeqNo))
		sm.commitState.applyCheckpointResult(checkpointResult)

		actions.concat(sm.checkpointTracker.applyCheckpointResult(
			checkpointResult.SeqNo,
			checkpointResult.Value,
			sm.epochTracker.currentEpoch.number,
			checkpointResult.NetworkState,
		))

	}

	for _, hashResult := range results.Digests {
		switch hashType := hashResult.Type.(type) {
		case *pb.HashResult_Batch_:
			batch := hashType.Batch
			sm.batchTracker.addBatch(batch.SeqNo, hashResult.Digest, batch.RequestAcks)
			actions.concat(sm.epochTracker.applyBatchHashResult(batch.Epoch, batch.SeqNo, hashResult.Digest))
		case *pb.HashResult_Request_:
			req := hashType.Request.Request
			actions.concat(sm.clientTracker.applyRequestDigest(
				&pb.RequestAck{
					ClientId: req.ClientId,
					ReqNo:    req.ReqNo,
					Digest:   hashResult.Digest,
				},
				req.Data,
			))
		case *pb.HashResult_VerifyRequest_:
			request := hashType.VerifyRequest
			if !bytes.Equal(request.RequestAck.Digest, hashResult.Digest) {
				panic("byzantine")
				// XXX this should not panic, but put to make dev easier
			}
			actions.concat(sm.clientTracker.applyRequestDigest(
				request.RequestAck,
				request.RequestData,
			))
		case *pb.HashResult_EpochChange_:
			epochChange := hashType.EpochChange
			actions.concat(sm.epochTracker.applyEpochChangeDigest(epochChange, hashResult.Digest))
		case *pb.HashResult_VerifyBatch_:
			verifyBatch := hashType.VerifyBatch
			sm.batchTracker.applyVerifyBatchHashResult(hashResult.Digest, verifyBatch)
			if !sm.batchTracker.hasFetchInFlight() && sm.epochTracker.currentEpoch.state == etFetching {
				actions.concat(sm.epochTracker.currentEpoch.fetchNewEpochState())
			}
		default:
			panic("no hash result type set")
		}
	}

	return actions
}

func (sm *StateMachine) clientWaiter(clientID uint64) *clientWaiter {
	client, ok := sm.clientTracker.client(clientID)
	if !ok {
		return nil
	}

	return client.clientWaiter
}

func (sm *StateMachine) Status() *status.StateMachine {
	if sm.state != smInitialized {
		return &status.StateMachine{}
	}

	clientTrackerStatus := make([]*status.ClientTracker, len(sm.clientTracker.clientIDs))

	for i, id := range sm.clientTracker.clientIDs {
		client := sm.clientTracker.clients[id]
		rws := client.status()
		rws.ClientID = id
		clientTrackerStatus[i] = rws
	}

	nodes := make([]*status.NodeBuffer, len(sm.checkpointTracker.networkConfig.Nodes))
	for i, id := range sm.checkpointTracker.networkConfig.Nodes {
		nodeID := nodeID(id)
		nodes[i] = sm.epochTracker.currentEpoch.nodeMsgs[nodeID].status()
	}

	lowWatermark, highWatermark, bucketStatus := sm.epochTracker.currentEpoch.bucketStatus()

	checkpoints := sm.checkpointTracker.status()

	return &status.StateMachine{
		NodeID:        sm.myConfig.Id,
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochTracker:  sm.epochTracker.status(),
		ClientWindows: clientTrackerStatus,
		Buckets:       bucketStatus,
		Checkpoints:   checkpoints,
		NodeBuffers:   nodes,
	}
}
