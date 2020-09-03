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

// StateMachine contains a deterministic processor for mirbftpb state events.
// This structure should almost never be initialized directly but should instead
// be allocated via StartNode.
type StateMachine struct {
	Logger Logger

	state stateMachineState

	myConfig      *pb.StateEvent_InitialParameters
	networkConfig *pb.NetworkState_Config
	clientWindows *clientWindows

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

func (sm *StateMachine) applyPersisted(entry *pb.Persistent) {
	if sm.state != smLoadingPersisted {
		panic("state machine has already finished loading persisted data")
	}

	sm.persisted.appendLogEntry(entry)
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
		checkpoints = checkpoints[len(checkpoints)-3:]
	}

	sm.persisted.truncate(checkpoints[0].SeqNo)
	sm.persisted.lastCommitted = checkpoints[len(checkpoints)-1].SeqNo

	sm.checkpointTracker = newCheckpointTracker(sm.persisted, sm.myConfig, sm.Logger)
	sm.clientWindows = newClientWindows(sm.persisted, sm.myConfig, sm.Logger)

	sm.networkConfig = sm.checkpointTracker.networkConfig

	sm.batchTracker = newBatchTracker(sm.persisted)

	sm.epochTracker = newEpochTracker(
		sm.persisted,
		sm.networkConfig,
		sm.Logger,
		sm.myConfig,
		sm.batchTracker,
		sm.clientWindows,
	)

	sm.state = smInitialized
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
		sm.applyPersisted(event.LoadEntry.Entry)
		return &Actions{}
	case *pb.StateEvent_CompleteInitialization:
		sm.completeInitialization()
		return &Actions{}
	case *pb.StateEvent_Tick:
		assertInitialized()
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
	// new checkpoint messages from ourselves, which necessarily can only exist after the
	// commits for those sequences have been returned.
	if sm.checkpointTracker.state == cpsGarbageCollectable {
		newLow := sm.checkpointTracker.garbageCollect()

		sm.clientWindows.garbageCollect(newLow)
		if newLow > uint64(sm.networkConfig.CheckpointInterval) {
			// Note, we leave an extra checkpoint worth of batches around, to help
			// during epoch change.
			sm.batchTracker.truncate(newLow - uint64(sm.networkConfig.CheckpointInterval))
		}
		sm.persisted.truncate(newLow)
		actions.concat(sm.epochTracker.moveWatermarks(newLow))
	}

	loopActions := actions
	for {
		// We note all of the commits that occured in response to the current event
		// as well as any watermark movement.  Then, based on this information we
		// may continue to iterate the state machine, and do so, so long as
		// attempting to advance the state causes new actions.

		for _, commit := range loopActions.Commits {
			for _, fr := range commit.QEntry.Requests {
				cw, ok := sm.clientWindows.clientWindow(fr.Request.ClientId)
				if !ok {
					panic("we never should have committed this without the client available")
				}
				cw.request(fr.Request.ReqNo).committed = &commit.QEntry.SeqNo
				// sm.Logger.Info(fmt.Sprintf("Committing %d.%d at seqno=%d", fr.Request.ClientId, fr.Request.ReqNo, commit.QEntry.SeqNo))
			}

			checkpoint := commit.QEntry.SeqNo%uint64(sm.networkConfig.CheckpointInterval) == 0
			// If this commit lands on a checkpoint boundary, we note the committed
			// network state and set the flag.  Otherwise, we will not need to reference
			// the epoch config in the returned result, so we nil it.
			if checkpoint {
				commit.Checkpoint = true
				commit.NetworkState = &pb.NetworkState{
					Clients: sm.clientWindows.clientConfigs(),
					Config:  sm.networkConfig,
				}
			} else {
				commit.EpochConfig = nil
			}

			sm.persisted.setLastCommitted(commit.QEntry.SeqNo)
		}

		loopActions = sm.epochTracker.advanceState()
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
		return actions.concat(sm.clientWindows.step(source, msg))
	case *pb.Msg_FetchRequest:
		return actions.concat(sm.clientWindows.step(source, msg))
	case *pb.Msg_ForwardRequest:
		return actions.concat(sm.clientWindows.step(source, msg))
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
		actions.concat(sm.checkpointTracker.applyCheckpointResult(
			checkpointResult.SeqNo,
			checkpointResult.Value,
			checkpointResult.EpochConfig,
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
			request := hashType.Request
			actions.send(
				sm.networkConfig.Nodes,
				&pb.Msg{
					Type: &pb.Msg_RequestAck{
						RequestAck: &pb.RequestAck{
							ClientId: request.Request.ClientId,
							ReqNo:    request.Request.ReqNo,
							Digest:   hashResult.Digest,
						},
					},
				},
			)
			sm.clientWindows.allocate(request.Request, hashResult.Digest)
		case *pb.HashResult_VerifyRequest_:
			request := hashType.VerifyRequest
			if !bytes.Equal(request.ExpectedDigest, hashResult.Digest) {
				panic("byzantine")
				// XXX this should not panic, but put to make dev easier
			}
			sm.clientWindows.allocate(request.Request, hashResult.Digest)
			if sm.epochTracker.currentEpoch.state == fetching {
				actions.concat(sm.epochTracker.currentEpoch.fetchNewEpochState())
			}
		case *pb.HashResult_EpochChange_:
			epochChange := hashType.EpochChange
			actions.concat(sm.epochTracker.applyEpochChangeDigest(epochChange, hashResult.Digest))
		case *pb.HashResult_VerifyBatch_:
			verifyBatch := hashType.VerifyBatch
			sm.batchTracker.applyVerifyBatchHashResult(hashResult.Digest, verifyBatch)
			if !sm.batchTracker.hasFetchInFlight() && sm.epochTracker.currentEpoch.state == fetching {
				actions.concat(sm.epochTracker.currentEpoch.fetchNewEpochState())
			}
		default:
			panic("no hash result type set")
		}
	}

	return actions
}

func (sm *StateMachine) clientWaiter(clientID uint64) *clientWaiter {
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		return nil
	}

	return clientWindow.clientWaiter
}

func (sm *StateMachine) Status() *status.StateMachine {
	if sm.state != smInitialized {
		return &status.StateMachine{}
	}

	clientWindowsStatus := make([]*status.ClientTracker, len(sm.clientWindows.clients))

	for i, id := range sm.clientWindows.clients {
		clientWindow := sm.clientWindows.windows[id]
		rws := clientWindow.status()
		rws.ClientID = id
		clientWindowsStatus[i] = rws
	}

	nodes := make([]*status.NodeBuffer, len(sm.networkConfig.Nodes))
	for i, id := range sm.networkConfig.Nodes {
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
		ClientWindows: clientWindowsStatus,
		Buckets:       bucketStatus,
		Checkpoints:   checkpoints,
		NodeBuffers:   nodes,
	}
}
