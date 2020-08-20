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
	state StateMachineState

	myConfig      *Config
	networkConfig *pb.NetworkState_Config
	nodeMsgs      map[NodeID]*nodeMsgs
	clientWindows *clientWindows

	activeEpoch       *epoch
	batchTracker      *batchTracker
	checkpointTracker *checkpointTracker
	epochChanger      *epochChanger
	persisted         *persisted
}

func (sm *StateMachine) initialize(myConfig *Config) {
	if sm.state != smUninitialized {
		panic("state machine has already been initialized")
	}

	sm.myConfig = myConfig
	sm.state = smLoadingPersisted
	sm.persisted = newPersisted(myConfig)
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
		logger: sm.myConfig.Logger,
	}

	sm.checkpointTracker = newCheckpointTracker(sm.persisted, sm.myConfig)
	sm.clientWindows = newClientWindows(sm.persisted, sm.myConfig)

	sm.networkConfig = sm.checkpointTracker.networkConfig

	sm.nodeMsgs = map[NodeID]*nodeMsgs{}
	for _, id := range sm.networkConfig.Nodes {
		sm.nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), sm.networkConfig, sm.myConfig, sm.clientWindows, oddities)
	}

	sm.batchTracker = newBatchTracker(sm.persisted)

	sm.epochChanger = newEpochChanger(
		sm.persisted,
		sm.networkConfig,
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

	switch event := stateEvent.Type.(type) {
	case *pb.StateEvent_LoadEntry:
		sm.applyPersisted(event.LoadEntry.Entry)
	case *pb.StateEvent_CompleteInitialization:
		sm.completeInitialization()
	case *pb.StateEvent_Tick:
		assertInitialized()
		return sm.tick()
	case *pb.StateEvent_Step:
		assertInitialized()
		return sm.step(
			NodeID(event.Step.Source),
			event.Step.Msg,
		)
	case *pb.StateEvent_Propose:
		assertInitialized()
		return sm.propose(
			event.Propose.Request,
		)
	case *pb.StateEvent_AddResults:
		assertInitialized()
		return sm.processResults(
			event.AddResults,
		)
	default:
		panic(fmt.Sprintf("unknown state event type: %T", stateEvent.Type))
	}

	return &Actions{}
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
							Source:  sm.myConfig.ID,
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
		sm.myConfig.Logger.Panic("received a message from a node ID that does not exist", zap.Int("source", int(source)))
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

	if sm.epochChanger.activeEpoch.state != ready {
		return actions
	}

	sm.activeEpoch = newEpoch(sm.persisted, sm.clientWindows, sm.myConfig)
	actions.Append(sm.activeEpoch.drainProposer())
	sm.epochChanger.activeEpoch.state = inProgress
	for _, nodeMsgs := range sm.nodeMsgs {
		nodeMsgs.setActiveEpoch(sm.activeEpoch)
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
				actions.Append(sm.applyPreprepareMsg(source, msg))
			case *pb.Msg_Prepare:
				msg := innerMsg.Prepare
				actions.Append(sm.activeEpoch.applyPrepareMsg(source, msg.SeqNo, msg.Digest))
			case *pb.Msg_Commit:
				msg := innerMsg.Commit
				actions.Append(sm.activeEpoch.applyCommitMsg(source, msg.SeqNo, msg.Digest))
			case *pb.Msg_Checkpoint:
				msg := innerMsg.Checkpoint
				actions.Append(sm.checkpointMsg(source, msg.SeqNo, msg.Value))
			case *pb.Msg_RequestAck:
				msg := innerMsg.RequestAck
				actions.Append(sm.applyRequestAckMsg(source, msg))
			case *pb.Msg_FetchBatch:
				msg := innerMsg.FetchBatch
				actions.Append(sm.batchTracker.replyFetchBatch(msg.SeqNo, msg.Digest))
			case *pb.Msg_FetchRequest:
				msg := innerMsg.FetchRequest
				actions.Append(sm.clientWindows.replyFetchRequest(source, msg.ClientId, msg.ReqNo, msg.Digest))
			case *pb.Msg_ForwardBatch:
				msg := innerMsg.ForwardBatch
				actions.Append(sm.batchTracker.applyForwardBatchMsg(source, msg.SeqNo, msg.Digest, msg.RequestAcks))
			case *pb.Msg_ForwardRequest:
				if source == NodeID(sm.myConfig.ID) {
					// We've already pre-processed this
					// TODO, once we implement unicasting to only those
					// who don't know this should go away.
					continue
				}
				actions.Append(sm.clientWindows.applyForwardRequest(source, innerMsg.ForwardRequest))
			case *pb.Msg_Suspect:
				sm.applySuspectMsg(source, innerMsg.Suspect.Epoch)
			case *pb.Msg_EpochChange:
				actions.Append(sm.epochChanger.applyEpochChangeMsg(source, innerMsg.EpochChange))
			case *pb.Msg_EpochChangeAck:
				actions.Append(sm.epochChanger.applyEpochChangeAckMsg(source, innerMsg.EpochChangeAck))
			case *pb.Msg_NewEpoch:
				actions.Append(sm.epochChanger.applyNewEpochMsg(innerMsg.NewEpoch))
			case *pb.Msg_NewEpochEcho:
				actions.Append(sm.epochChanger.applyNewEpochEchoMsg(source, innerMsg.NewEpochEcho))
			case *pb.Msg_NewEpochReady:
				actions.Append(sm.epochChanger.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady))
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

func (sm *StateMachine) applyPreprepareMsg(source NodeID, msg *pb.Preprepare) *Actions {
	return sm.activeEpoch.applyPreprepareMsg(source, msg.SeqNo, msg.Batch)
}

func (sm *StateMachine) applySuspectMsg(source NodeID, epoch uint64) *Actions {
	epochChange := sm.epochChanger.applySuspectMsg(source, epoch)
	if epochChange == nil {
		return &Actions{}
	}

	for _, nodeMsgs := range sm.nodeMsgs {
		nodeMsgs.setActiveEpoch(nil)
	}
	sm.activeEpoch = nil

	actions := sm.persisted.addEpochChange(epochChange) // TODO, this is an awkward spot

	actions.Append(&Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: epochChange,
				},
			},
		},
	})

	return actions
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
	actions := sm.activeEpoch.moveWatermarks(newLow)
	actions.Append(sm.drainNodeMsgs())
	return actions
}

func (sm *StateMachine) processResults(results *pb.StateEvent_ActionResults) *Actions {
	actions := &Actions{}

	for _, checkpointResult := range results.Checkpoints {
		// sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.checkpointTracker.applyCheckpointResult(
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

			if sm.activeEpoch != nil && batch.Epoch != sm.activeEpoch.epochConfig.Number {
				continue
			}

			// sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
			seqNo := batch.SeqNo
			// TODO, rename applyProcessResult to something better
			actions.Append(sm.activeEpoch.applyProcessResult(seqNo, hashResult.Digest))
		case *pb.HashResult_Request_:
			request := hashType.Request
			actions.Broadcast = append(actions.Broadcast, &pb.Msg{
				Type: &pb.Msg_RequestAck{
					RequestAck: &pb.RequestAck{
						ClientId: request.Request.ClientId,
						ReqNo:    request.Request.ReqNo,
						Digest:   hashResult.Digest,
					},
				},
			})
			actions.Append(sm.applyDigestedValidRequest(hashResult.Digest, request.Request))
		case *pb.HashResult_VerifyRequest_:
			request := hashType.VerifyRequest
			if !bytes.Equal(request.ExpectedDigest, hashResult.Digest) {
				panic("byzantine")
				// XXX this should not panic, but put to make dev easier
			}
			actions.Append(sm.applyDigestedValidRequest(hashResult.Digest, request.Request))
			if sm.epochChanger.activeEpoch.state == fetching {
				actions.Append(sm.epochChanger.activeEpoch.fetchNewEpochState())
			}
		case *pb.HashResult_EpochChange_:
			epochChange := hashType.EpochChange
			actions.Append(sm.epochChanger.applyEpochChangeDigest(epochChange, hashResult.Digest))
		case *pb.HashResult_VerifyBatch_:
			verifyBatch := hashType.VerifyBatch
			sm.batchTracker.applyVerifyBatchHashResult(hashResult.Digest, verifyBatch)
			if !sm.batchTracker.hasFetchInFlight() && sm.epochChanger.activeEpoch.state == fetching {
				actions.Append(sm.epochChanger.activeEpoch.fetchNewEpochState())
			}
		default:
			panic("no hash result type set")
		}
	}

	actions.Append(sm.drainNodeMsgs())

	return sm.advance(actions)
}

func (sm *StateMachine) applyRequestAckMsg(source NodeID, ack *pb.RequestAck) *Actions {
	// TODO, make sure nodeMsgs ignores this if client is not defined

	sm.clientWindows.ack(source, ack)

	if sm.activeEpoch == nil {
		return &Actions{}
	}

	actions := sm.activeEpoch.outstandingReqs.advanceRequests()
	actions.Append(sm.activeEpoch.drainProposer())
	return actions
}

func (sm *StateMachine) applyDigestedValidRequest(digest []byte, requestData *pb.Request) *Actions {
	sm.clientWindows.allocate(requestData, digest)

	if sm.activeEpoch == nil {
		return &Actions{}
	}

	actions := sm.activeEpoch.outstandingReqs.advanceRequests()
	actions.Append(sm.activeEpoch.drainProposer())
	return actions
}

func (sm *StateMachine) clientWaiter(clientID uint64) *clientWaiter {
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		return nil
	}

	return clientWindow.clientWaiter
}

func (sm *StateMachine) tick() *Actions {
	actions := &Actions{}

	if sm.activeEpoch != nil {
		actions.Append(sm.activeEpoch.tick())
	}

	actions.Append(sm.epochChanger.tick())

	return actions
}

func (sm *StateMachine) Status() *Status {
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

	if sm.activeEpoch != nil {
		epoch := sm.activeEpoch

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
		NodeID:        sm.myConfig.ID,
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochChanger:  sm.epochChanger.status(),
		ClientWindows: clientWindowsStatus,
		Buckets:       buckets,
		Checkpoints:   checkpoints,
		Nodes:         nodes,
	}
}
