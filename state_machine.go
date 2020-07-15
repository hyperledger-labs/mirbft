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

type stateMachine struct {
	myConfig      *Config
	networkConfig *pb.NetworkConfig
	nodeMsgs      map[NodeID]*nodeMsgs
	clientWindows *clientWindows

	activeEpoch       *epoch
	batchTracker      *batchTracker
	checkpointTracker *checkpointTracker
	epochChanger      *epochChanger
	persisted         *persisted
}

func newStateMachine(myConfig *Config, persisted *persisted) *stateMachine {
	oddities := &oddities{
		logger: myConfig.Logger,
	}

	networkConfig := persisted.checkpoints[0].NetworkConfig

	clientWindows := &clientWindows{
		windows:       map[uint64]*clientWindow{},
		networkConfig: networkConfig,
		myConfig:      myConfig,
	}

	clientWindowWidth := uint64(100) // XXX this should be configurable

	for _, client := range networkConfig.Clients {
		clientWindow := newClientWindow(client.LowWatermark+1, client.LowWatermark+clientWindowWidth, networkConfig, myConfig)
		clientWindows.insert(client.Id, clientWindow)
	}

	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range networkConfig.Nodes {
		nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), networkConfig, myConfig, clientWindows, oddities)
	}

	checkpointTracker := newCheckpointTracker(networkConfig, persisted, myConfig)
	batchTracker := newBatchTracker() // TODO, populate batch tracker from persisted

	checkpoints := []*pb.Checkpoint{}
	for _, cEntry := range persisted.checkpoints {
		if cEntry == nil {
			break
		}
		checkpoints = append(checkpoints, &pb.Checkpoint{
			SeqNo: cEntry.SeqNo,
			Value: cEntry.CheckpointValue,
		})
	}
	epochChange, err := newParsedEpochChange(&pb.EpochChange{
		Checkpoints: checkpoints,
	})
	if err != nil {
		panic(err)
	}

	epochChanger := &epochChanger{
		persisted:     persisted,
		myConfig:      myConfig,
		networkConfig: networkConfig,
		targets:       map[uint64]*epochTarget{},
		batchTracker:  batchTracker,
		clientWindows: clientWindows,
	}

	target := epochChanger.target(0)
	target.myEpochChange = epochChange
	target.myLeaderChoice = networkConfig.Nodes
	epochChanger.pendingEpochTarget = target

	return &stateMachine{
		myConfig:          myConfig,
		networkConfig:     networkConfig,
		epochChanger:      epochChanger,
		batchTracker:      batchTracker,
		checkpointTracker: checkpointTracker,
		nodeMsgs:          nodeMsgs,
		clientWindows:     clientWindows,
		persisted:         persisted,
	}
}

func (sm *stateMachine) propose(requestData *pb.Request) *Actions {
	data := [][]byte{
		uint64ToBytes(requestData.ClientId),
		uint64ToBytes(requestData.ReqNo),
		requestData.Data,
	}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: data,
				Request: &Request{
					Source:  sm.myConfig.ID,
					Request: requestData,
				},
			},
		},
	}
}

func (sm *stateMachine) step(source NodeID, outerMsg *pb.Msg) *Actions {
	nodeMsgs, ok := sm.nodeMsgs[source]
	if !ok {
		sm.myConfig.Logger.Panic("received a message from a node ID that does not exist", zap.Int("source", int(source)))
	}

	nodeMsgs.ingest(outerMsg)

	return sm.drainNodeMsgs()
}

func (sm *stateMachine) drainNodeMsgs() *Actions {
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
				actions.Append(sm.applyRequestAckMsg(source, msg.ClientId, msg.ReqNo, msg.Digest))
			case *pb.Msg_FetchBatch:
				msg := innerMsg.FetchBatch
				actions.Append(sm.batchTracker.replyFetchBatch(msg.SeqNo, msg.Digest))
			case *pb.Msg_FetchRequest:
				msg := innerMsg.FetchRequest
				actions.Append(sm.replyFetchRequest(msg.ClientId, msg.ReqNo, msg.Digest))
			case *pb.Msg_ForwardBatch:
				msg := innerMsg.ForwardBatch
				actions.Append(sm.batchTracker.applyForwardBatchMsg(source, msg.SeqNo, msg.Digest, msg.RequestAcks))
			case *pb.Msg_ForwardRequest:
				if source == NodeID(sm.myConfig.ID) {
					// We've already pre-processed this
					continue
				}
				msg := innerMsg.ForwardRequest
				cw, ok := sm.clientWindows.clientWindow(msg.Request.ClientId)
				if ok {
					// TODO, make sure that we only allow one vote per replica for a reqno
					if cr := cw.request(msg.Request.ReqNo); cr != nil {
						req, ok := cr.digests[string(msg.Digest)]
						req.agreements[source] = struct{}{}
						if ok && req.data != nil {
							continue
						}
					}
				}

				actions.Hash = append(actions.Hash, &HashRequest{
					Data: [][]byte{
						uint64ToBytes(msg.Request.ClientId),
						uint64ToBytes(msg.Request.ReqNo),
						msg.Request.Data,
					},
					VerifyRequest: &VerifyRequest{
						Source:         uint64(source),
						Request:        msg.Request,
						ExpectedDigest: msg.Digest,
					},
				})
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
				actions.Append(sm.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady))
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

func (sm *stateMachine) applyPreprepareMsg(source NodeID, msg *pb.Preprepare) *Actions {
	return sm.activeEpoch.applyPreprepareMsg(source, msg.SeqNo, msg.Batch)
}

func (sm *stateMachine) replyFetchRequest(clientID uint64, reqNo uint64, digest []byte) *Actions {
	cw, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		return &Actions{}
	}

	if !cw.inWatermarks(reqNo) {
		return &Actions{}
	}

	creq := cw.request(reqNo)
	data, ok := creq.digests[string(digest)]
	if !ok {
		return &Actions{}
	}

	if data.data == nil {
		return &Actions{}
	}

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_ForwardRequest{
					ForwardRequest: &pb.ForwardRequest{
						Request: data.data,
						Digest:  digest,
					},
				},
			},
		},
	}
}

func (sm *stateMachine) applySuspectMsg(source NodeID, epoch uint64) *Actions {
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

func (sm *stateMachine) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	actions := sm.epochChanger.applyNewEpochReadyMsg(source, msg)

	if sm.epochChanger.pendingEpochTarget.state != ready {
		return actions
	}

	sm.activeEpoch = newEpoch(sm.persisted, sm.clientWindows, sm.myConfig)
	actions.Append(sm.activeEpoch.drainProposer())
	sm.epochChanger.pendingEpochTarget.state = idle
	sm.epochChanger.lastActiveEpoch = sm.epochChanger.pendingEpochTarget.number
	for _, nodeMsgs := range sm.nodeMsgs {
		nodeMsgs.setActiveEpoch(sm.activeEpoch)
	}

	return actions
}

func (sm *stateMachine) checkpointMsg(source NodeID, seqNo uint64, value []byte) *Actions {
	if !sm.checkpointTracker.applyCheckpointMsg(source, seqNo, value) {
		return &Actions{}
	}

	cwi := sm.clientWindows.iterator()
	for _, cw := cwi.next(); cw != nil; _, cw = cwi.next() {
		// oldLowReqNo := cw.lowWatermark
		cw.garbageCollect(seqNo)
		// sm.myConfig.Logger.Debug("move client watermarks", zap.Binary("ClientID", cid), zap.Uint64("Old", oldLowReqNo), zap.Uint64("New", cw.lowWatermark))
	}
	if seqNo > uint64(sm.networkConfig.CheckpointInterval) {
		// Note, we leave an extra checkpoint worth of batches around, to help
		// during epoch change.
		sm.batchTracker.truncate(seqNo - uint64(sm.networkConfig.CheckpointInterval))
	}
	sm.persisted.truncate(seqNo)
	sm.checkpointTracker.truncate(seqNo)
	actions := sm.activeEpoch.moveWatermarks(seqNo)
	actions.Append(sm.drainNodeMsgs())
	return actions
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}

	for _, checkpointResult := range results.Checkpoints {
		// sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.checkpointTracker.applyCheckpointResult(checkpointResult.Commit.QEntry.SeqNo, checkpointResult.Value))
		// TODO, maybe push this into the checkpoint tracker?
		sm.persisted.addCEntry(&pb.CEntry{
			SeqNo:           checkpointResult.Commit.QEntry.SeqNo,
			CheckpointValue: checkpointResult.Value,
			NetworkConfig:   checkpointResult.Commit.NetworkConfig,
			EpochConfig:     checkpointResult.Commit.EpochConfig,
		})
	}

	for _, hashResult := range results.Digests {
		request := hashResult.Request
		switch {
		case request.Batch != nil:
			batch := request.Batch
			sm.batchTracker.addBatch(batch.SeqNo, hashResult.Digest, batch.RequestAcks)

			if sm.activeEpoch != nil && batch.Epoch != sm.activeEpoch.epochConfig.Number {
				continue
			}

			// sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
			seqNo := batch.SeqNo
			// TODO, rename applyProcessResult to something better
			actions.Append(sm.activeEpoch.applyProcessResult(seqNo, hashResult.Digest))
		case request.Request != nil:
			request := request.Request
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
		case request.VerifyRequest != nil:
			request := request.VerifyRequest
			if !bytes.Equal(request.ExpectedDigest, hashResult.Digest) {
				panic("byzantine")
				// XXX this should not panic, but put to make dev easier
			}
			actions.Append(sm.applyDigestedValidRequest(hashResult.Digest, request.Request))
			if sm.epochChanger.pendingEpochTarget.state == fetching {
				actions.Append(sm.epochChanger.pendingEpochTarget.fetchNewEpochState())
			}
		case request.EpochChange != nil:
			epochChange := request.EpochChange
			actions.Append(sm.epochChanger.applyEpochChangeDigest(epochChange, hashResult.Digest))
		case request.VerifyBatch != nil:
			verifyBatch := request.VerifyBatch
			sm.batchTracker.applyVerifyBatchHashResult(hashResult.Digest, verifyBatch)
			if !sm.batchTracker.hasFetchInFlight() && sm.epochChanger.pendingEpochTarget.state == fetching {
				actions.Append(sm.epochChanger.pendingEpochTarget.fetchNewEpochState())
			}
		default:
			panic("no hash result type set")
		}
	}

	actions.Append(sm.drainNodeMsgs())

	return actions
}

func (sm *stateMachine) applyRequestAckMsg(source NodeID, clientID uint64, reqNo uint64, digest []byte) *Actions {
	// TODO, make sure nodeMsgs ignores this if client is not defined

	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		panic("unexpected unknown client")
	}
	clientWindow.ack(source, reqNo, digest)

	if sm.activeEpoch == nil {
		return &Actions{}
	}

	sm.activeEpoch.proposer.stepClientWindow(clientID)
	return sm.activeEpoch.drainProposer()
}

func (sm *stateMachine) applyDigestedValidRequest(digest []byte, requestData *pb.Request) *Actions {
	clientID := requestData.ClientId
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		panic("unexpected unknown client")
	}

	clientWindow.allocate(requestData, digest)

	if sm.activeEpoch == nil {
		return &Actions{}
	}

	sm.activeEpoch.proposer.stepClientWindow(clientID)
	return sm.activeEpoch.drainProposer()
}

func (sm *stateMachine) clientWaiter(clientID uint64) *clientWaiter {
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		return nil
	}

	return clientWindow.clientWaiter
}

func (sm *stateMachine) tick() *Actions {
	actions := &Actions{}

	if sm.activeEpoch != nil {
		actions.Append(sm.activeEpoch.tick())
	}

	actions.Append(sm.epochChanger.tick())

	return actions
}

func (sm *stateMachine) status() *Status {
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
