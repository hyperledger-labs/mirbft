/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
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
	checkpointTracker *checkpointTracker
	epochChanger      *epochChanger
}

func newStateMachine(networkConfig *pb.NetworkConfig, myConfig *Config) *stateMachine {
	oddities := &oddities{
		logger: myConfig.Logger,
	}

	fakeCheckpoint := &pb.Checkpoint{
		SeqNo: 0,
		Value: []byte("TODO, get from state"),
	}

	nodeMsgs := map[NodeID]*nodeMsgs{}
	clientWindows := &clientWindows{
		windows: map[string]*clientWindow{},
	}
	for _, id := range networkConfig.Nodes {
		nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), networkConfig, myConfig, clientWindows, oddities)
	}

	checkpointTracker := newCheckpointTracker(networkConfig, myConfig)

	epochChange, err := newEpochChange(
		&pb.EpochChange{
			Checkpoints: []*pb.Checkpoint{fakeCheckpoint},
		},
	)

	if err != nil {
		panic(err)
	}

	epochChanger := &epochChanger{
		myConfig:      myConfig,
		networkConfig: networkConfig,
		targets:       map[uint64]*epochTarget{},
	}

	target := epochChanger.target(0)
	target.changes[NodeID(myConfig.ID)] = epochChange
	target.myEpochChange = epochChange
	epochChanger.pendingEpochTarget = target

	return &stateMachine{
		myConfig:          myConfig,
		networkConfig:     networkConfig,
		epochChanger:      epochChanger,
		checkpointTracker: checkpointTracker,
		nodeMsgs:          nodeMsgs,
		clientWindows:     clientWindows,
	}
}

func (sm *stateMachine) propose(requestData *pb.RequestData) *Actions {
	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_Forward{
					Forward: &pb.Forward{
						RequestData: requestData,
					},
				},
			},
		},
		Preprocess: []*Request{
			{
				Source:        sm.myConfig.ID,
				ClientRequest: requestData,
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
			case *pb.Msg_Forward:
				if source == NodeID(sm.myConfig.ID) {
					// We've already pre-processed this
					continue
				}
				msg := innerMsg.Forward
				actions.Preprocess = append(actions.Preprocess, &Request{
					Source:        uint64(source),
					ClientRequest: msg.RequestData,
				})
			case *pb.Msg_Suspect:
				sm.applySuspectMsg(source, innerMsg.Suspect.Epoch)
			case *pb.Msg_EpochChange:
				actions.Append(sm.epochChanger.applyEpochChangeMsg(source, innerMsg.EpochChange))
			case *pb.Msg_NewEpoch:
				actions.Append(sm.epochChanger.applyNewEpochMsg(innerMsg.NewEpoch))
			case *pb.Msg_NewEpochEcho:
				actions.Append(sm.epochChanger.applyNewEpochEchoMsg(source, innerMsg.NewEpochEcho))
			case *pb.Msg_NewEpochReady:
				actions.Append(sm.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady))
			default:
				// This should be unreachable, as the nodeMsgs filters based on type as well
				panic("unexpected bad message type, should have been detected earlier")
			}
		}

		if !moreActions {
			return actions
		}
	}
}

func (sm *stateMachine) applyPreprepareMsg(source NodeID, msg *pb.Preprepare) *Actions {
	requests := make([]*request, len(msg.Batch))

	for i, batchEntry := range msg.Batch {
		clientWindow, ok := sm.clientWindows.clientWindow(batchEntry.ClientId)
		if !ok {
			panic(fmt.Sprintf("got preprepare including a client which we don't know about"))
		}

		request := clientWindow.request(batchEntry.ReqNo)
		if request == nil {
			panic(fmt.Sprintf("could not find reqno=%d for batch entry from %d, this should have been hackily handled by the nodemsgs stuff", batchEntry.ReqNo, source))
		}
		requests[i] = request
	}

	return sm.activeEpoch.applyPreprepareMsg(source, msg.SeqNo, requests)
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

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: epochChange,
				},
			},
		},
	}
}

func (sm *stateMachine) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	actions := sm.epochChanger.applyNewEpochReadyMsg(source, msg)

	if sm.epochChanger.state == ready {
		sm.activeEpoch = newEpoch(sm.epochChanger.pendingEpochTarget.leaderNewEpoch, sm.checkpointTracker, sm.clientWindows, sm.epochChanger.lastActiveEpoch, sm.networkConfig, sm.myConfig)
		for _, sequence := range sm.activeEpoch.sequences {
			if sequence.state >= Prepared {
				actions.Broadcast = append(actions.Broadcast, &pb.Msg{
					Type: &pb.Msg_Commit{
						Commit: &pb.Commit{
							SeqNo:  sequence.seqNo,
							Epoch:  sequence.epoch,
							Digest: sequence.digest,
						},
					},
				})
			}
		}
		actions.Append(sm.activeEpoch.advanceUncommitted())
		actions.Append(sm.activeEpoch.drainProposer())
		sm.epochChanger.state = idle
		sm.epochChanger.lastActiveEpoch = sm.activeEpoch
		for _, nodeMsgs := range sm.nodeMsgs {
			nodeMsgs.setActiveEpoch(sm.activeEpoch)
		}
	}

	return actions
}

func (sm *stateMachine) checkpointMsg(source NodeID, seqNo uint64, value []byte) *Actions {
	if !sm.checkpointTracker.applyCheckpointMsg(source, seqNo, value) {
		return &Actions{}
	}

	cwi := sm.clientWindows.iterator()
	for cw := cwi.next(); cw != nil; cw = cwi.next() {
		cw.garbageCollect(seqNo)
	}
	actions := sm.activeEpoch.moveWatermarks()
	actions.Append(sm.drainNodeMsgs())
	return actions
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}

	for _, preprocessResult := range results.Preprocessed {
		// sm.myConfig.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.applyPreprocessResult(preprocessResult))
	}

	for _, checkpointResult := range results.Checkpoints {
		// sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.checkpointTracker.applyCheckpointResult(checkpointResult.SeqNo, checkpointResult.Value))
	}

	if sm.activeEpoch == nil {
		// TODO, this is a little heavy handed, we should probably
		// work with the persistence so we don't redo the effort.
		return actions
	}

	for _, processResult := range results.Processed {
		// sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
		seqNo := processResult.SeqNo
		// XXX we need to verify that the epoch matches the expected one
		actions.Append(sm.activeEpoch.applyProcessResult(seqNo, processResult.Digest))
	}

	return actions
}

func (sm *stateMachine) applyPreprocessResult(preprocessResult *PreprocessResult) *Actions {
	clientID := preprocessResult.RequestData.ClientId
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		clientWindow = newRequestWindow(1, 100) // XXX this should be configurable
		sm.clientWindows.insert(clientID, clientWindow)
	}

	clientWindow.allocate(preprocessResult.RequestData, preprocessResult.Digest)

	actions := &Actions{}

	if sm.activeEpoch != nil {
		sm.activeEpoch.proposer.stepRequestWindow(clientID)
		actions.Append(sm.activeEpoch.drainProposer())
	}

	return actions
}

func (sm *stateMachine) clientWaiter(clientID []byte) *clientWaiter {
	clientWindow, ok := sm.clientWindows.clientWindow(clientID)
	if !ok {
		clientWindow = newRequestWindow(1, 100) // XXX this should be configurable
		sm.clientWindows.insert(clientID, clientWindow)
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
	clientWindowsStatus := make([]*RequestWindowStatus, len(sm.clientWindows.clients))

	for i, id := range sm.clientWindows.clients {
		clientWindow := sm.clientWindows.windows[id]
		rws := clientWindow.status()
		rws.ClientID = []byte(id)
		clientWindowsStatus[i] = rws
	}

	nodes := make([]*NodeStatus, len(sm.networkConfig.Nodes))
	for i, nodeID := range sm.networkConfig.Nodes {
		nodeID := NodeID(nodeID)
		nodes[i] = sm.nodeMsgs[nodeID].status()
	}

	checkpoints := []*CheckpointStatus{}
	var buckets []*BucketStatus
	var lowWatermark, highWatermark uint64

	if sm.epochChanger.lastActiveEpoch != nil {
		epoch := sm.epochChanger.lastActiveEpoch
		for _, cw := range epoch.checkpoints {
			checkpoints = append(checkpoints, cw.status())
		}

		buckets = epoch.status()

		lowWatermark = epoch.baseCheckpoint.SeqNo

		if epoch != nil && len(epoch.checkpoints) > 0 {
			highWatermark = epoch.checkpoints[len(epoch.checkpoints)-1].end
		} else {
			highWatermark = lowWatermark
		}
	}

	return &Status{
		NodeID:         sm.myConfig.ID,
		LowWatermark:   lowWatermark,
		HighWatermark:  highWatermark,
		EpochChanger:   sm.epochChanger.status(),
		RequestWindows: clientWindowsStatus,
		Buckets:        buckets,
		Checkpoints:    checkpoints,
		Nodes:          nodes,
	}
}
