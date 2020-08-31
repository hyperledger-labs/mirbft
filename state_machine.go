/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type StateMachineState int

const (
	smUninitialized StateMachineState = iota
	smLoadingPersisted
	smInitialized
)

// StateMachine contains a deterministic processor for mirbftpb state events.
// This structure should almost never be initialized directly but should instead
// be allocated via StartNode.
type StateMachine struct {
	// Logger XXX this is a weird place/way to initialize the logger, since
	// we go and reference it through myConfig at the moment, but, it's the
	// only non-serializable part of the config.
	Logger Logger

	state StateMachineState

	myConfig      *pb.StateEvent_InitialParameters
	networkConfig *pb.NetworkState_Config
	nodeMsgs      map[NodeID]*nodeMsgs
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

	oddities := &oddities{
		logger: sm.Logger,
	}

	sm.checkpointTracker = newCheckpointTracker(sm.persisted, sm.myConfig)
	sm.clientWindows = newClientWindows(sm.persisted, sm.Logger)

	sm.networkConfig = sm.checkpointTracker.networkConfig

	sm.nodeMsgs = map[NodeID]*nodeMsgs{}
	for _, id := range sm.networkConfig.Nodes {
		sm.nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), sm.networkConfig, sm.Logger, sm.myConfig, sm.clientWindows, oddities)
	}

	sm.batchTracker = newBatchTracker(sm.persisted)

	sm.epochTracker = newEpochChanger(
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
		return sm.epochTracker.tick()
	case *pb.StateEvent_Step:
		assertInitialized()
		actions.concat(sm.step(
			NodeID(event.Step.Source),
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
	default:
		panic(fmt.Sprintf("unknown state event type: %T", stateEvent.Type))
	}

	if sm.epochTracker.currentEpoch.activeEpoch == nil {
		return actions
	}

	actions.concat(sm.epochTracker.currentEpoch.activeEpoch.outstandingReqs.advanceRequests())
	return actions.concat(sm.epochTracker.currentEpoch.activeEpoch.drainProposer())

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

func (sm *StateMachine) step(source NodeID, outerMsg *pb.Msg) *Actions {
	nodeMsgs, ok := sm.nodeMsgs[source]
	if !ok {
		sm.Logger.Panic("received a message from a node ID that does not exist", zap.Int("source", int(source)))
	}

	nodeMsgs.ingest(outerMsg)

	return sm.advance(sm.drainNodeMsgs())
}

func (sm *StateMachine) advance(actions *Actions) *Actions {
	for _, commit := range actions.Commits {
		for _, fr := range commit.QEntry.Requests {
			cw, ok := sm.clientWindows.clientWindow(fr.Request.ClientId)
			if !ok {
				panic("we never should have committed this without the client available")
			}
			cw.request(fr.Request.ReqNo).committed = &commit.QEntry.SeqNo
		}

		checkpoint := commit.QEntry.SeqNo%uint64(sm.networkConfig.CheckpointInterval) == 0

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

	if sm.epochTracker.currentEpoch.state != inProgress {
		return actions
	}

	for _, nodeMsgs := range sm.nodeMsgs {
		if nodeMsgs.epochMsgs == nil { // TODO, remove this hack
			nodeMsgs.setActiveEpoch(sm.epochTracker.currentEpoch.activeEpoch)
		}
	}

	return actions
}

func (sm *StateMachine) drainNodeMsgs() *Actions {
	actions := &Actions{}

	for {
		moreActions := false
		for source, nodeMsgs := range sm.nodeMsgs {
			msg := nodeMsgs.next()
			if msg == nil {
				continue
			}
			moreActions = true

			switch innerMsg := msg.Type.(type) {
			case *pb.Msg_Preprepare:
				msg := innerMsg.Preprepare
				actions.concat(sm.epochTracker.applyPreprepareMsg(source, msg.Epoch, msg.SeqNo, msg.Batch))
			case *pb.Msg_Prepare:
				msg := innerMsg.Prepare
				actions.concat(sm.epochTracker.applyPrepareMsg(source, msg.Epoch, msg.SeqNo, msg.Digest))
			case *pb.Msg_Commit:
				msg := innerMsg.Commit
				actions.concat(sm.epochTracker.applyCommitMsg(source, msg.Epoch, msg.SeqNo, msg.Digest))
			case *pb.Msg_Checkpoint:
				msg := innerMsg.Checkpoint
				actions.concat(sm.checkpointMsg(source, msg.SeqNo, msg.Value))
			case *pb.Msg_RequestAck:
				// TODO, make sure nodeMsgs ignores this if client is not defined
				msg := innerMsg.RequestAck
				sm.clientWindows.ack(source, msg)
			case *pb.Msg_FetchBatch:
				msg := innerMsg.FetchBatch
				actions.concat(sm.batchTracker.replyFetchBatch(uint64(source), msg.SeqNo, msg.Digest))
			case *pb.Msg_FetchRequest:
				msg := innerMsg.FetchRequest
				actions.concat(sm.clientWindows.replyFetchRequest(source, msg.ClientId, msg.ReqNo, msg.Digest))
			case *pb.Msg_ForwardBatch:
				msg := innerMsg.ForwardBatch
				actions.concat(sm.batchTracker.applyForwardBatchMsg(source, msg.SeqNo, msg.Digest, msg.RequestAcks))
			case *pb.Msg_ForwardRequest:
				if source == NodeID(sm.myConfig.Id) {
					// We've already pre-processed this
					// TODO, once we implement unicasting to only those
					// who don't know this should go away.
					continue
				}
				actions.concat(sm.clientWindows.applyForwardRequest(source, innerMsg.ForwardRequest))
			case *pb.Msg_Suspect:
				sm.applySuspectMsg(source, innerMsg.Suspect.Epoch)
			case *pb.Msg_EpochChange:
				actions.concat(sm.epochTracker.applyEpochChangeMsg(source, innerMsg.EpochChange))
			case *pb.Msg_EpochChangeAck:
				actions.concat(sm.epochTracker.applyEpochChangeAckMsg(source, innerMsg.EpochChangeAck))
			case *pb.Msg_NewEpoch:
				actions.concat(sm.epochTracker.applyNewEpochMsg(innerMsg.NewEpoch))
			case *pb.Msg_NewEpochEcho:
				actions.concat(sm.epochTracker.applyNewEpochEchoMsg(source, innerMsg.NewEpochEcho))
			case *pb.Msg_NewEpochReady:
				actions.concat(sm.epochTracker.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady))
			default:
				// This should be unreachable, as the nodeMsgs filters based on type as well
				panic(fmt.Sprintf("unexpected bad message type %T, should have been detected earlier", msg.Type))
			}
		}

		if !moreActions {
			return actions
		}
	}
}

func (sm *StateMachine) applySuspectMsg(source NodeID, epoch uint64) *Actions {
	epochChange := sm.epochTracker.applySuspectMsg(source, epoch)
	if epochChange == nil {
		return &Actions{}
	}

	for _, nodeMsgs := range sm.nodeMsgs {
		nodeMsgs.setActiveEpoch(nil)
	}

	actions := sm.persisted.addEpochChange(epochChange) // TODO, this is an awkward spot

	return actions.send(
		sm.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_EpochChange{
				EpochChange: epochChange,
			},
		},
	)
}

func (sm *StateMachine) checkpointMsg(source NodeID, seqNo uint64, value []byte) *Actions {
	sm.checkpointTracker.applyCheckpointMsg(source, seqNo, value)

	if sm.checkpointTracker.state != cpsGarbageCollectable {
		return &Actions{}
	}

	newLow := sm.checkpointTracker.garbageCollect()

	sm.clientWindows.garbageCollect(newLow)
	if newLow > uint64(sm.networkConfig.CheckpointInterval) {
		// Note, we leave an extra checkpoint worth of batches around, to help
		// during epoch change.
		sm.batchTracker.truncate(newLow - uint64(sm.networkConfig.CheckpointInterval))
	}
	sm.persisted.truncate(newLow)
	actions := sm.epochTracker.currentEpoch.activeEpoch.moveWatermarks(newLow)
	return actions.concat(sm.drainNodeMsgs())
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

	actions.concat(sm.drainNodeMsgs())

	return sm.advance(actions)
}

func (sm *StateMachine) clientWaiter(clientID uint64) *clientWaiter {
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		return nil
	}

	return clientWindow.clientWaiter
}

func (sm *StateMachine) Status() *Status {
	if sm.state != smInitialized {
		return &Status{}
	}

	clientWindowsStatus := make([]*ClientWindowStatus, len(sm.clientWindows.clients))

	for i, id := range sm.clientWindows.clients {
		clientWindow := sm.clientWindows.windows[id]
		rws := clientWindow.status()
		rws.ClientID = id
		clientWindowsStatus[i] = rws
	}

	nodes := make([]*NodeStatus, len(sm.networkConfig.Nodes))
	for i, nodeID := range sm.networkConfig.Nodes {
		nodeID := NodeID(nodeID)
		nodes[i] = sm.nodeMsgs[nodeID].status()
	}

	checkpoints := sm.checkpointTracker.status()

	var buckets []*BucketStatus
	var lowWatermark, highWatermark uint64

	if sm.epochTracker.currentEpoch.activeEpoch != nil {
		epoch := sm.epochTracker.currentEpoch.activeEpoch

		buckets = epoch.status()

		lowWatermark = epoch.lowWatermark() - 1
		highWatermark = epoch.highWatermark()
	} else {
		buckets = make([]*BucketStatus, sm.networkConfig.NumberOfBuckets)
		for i := range buckets {
			buckets[i] = &BucketStatus{ID: uint64(i)}
		}
	}

	return &Status{
		NodeID:        sm.myConfig.Id,
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochChanger:  sm.epochTracker.status(),
		ClientWindows: clientWindowsStatus,
		Buckets:       buckets,
		Checkpoints:   checkpoints,
		Nodes:         nodes,
	}
}
