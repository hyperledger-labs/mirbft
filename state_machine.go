/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"
	"math"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

type stateMachine struct {
	myConfig       *Config
	networkConfig  *pb.NetworkConfig
	nodeMsgs       map[NodeID]*nodeMsgs
	requestWindows map[string]*requestWindow

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
	requestWindows := map[string]*requestWindow{}
	for _, id := range networkConfig.Nodes {
		requestWindow := newRequestWindow(fakeCheckpoint.SeqNo+1, fakeCheckpoint.SeqNo+2*uint64(networkConfig.CheckpointInterval))
		nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), networkConfig, requestWindow, myConfig, oddities)
		requestWindows[string(uint64ToBytes(id))] = requestWindow
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
	target.myEpochChange = epochChange.underlying
	epochChanger.pendingEpochTarget = target

	return &stateMachine{
		myConfig:          myConfig,
		networkConfig:     networkConfig,
		epochChanger:      epochChanger,
		checkpointTracker: checkpointTracker,
		nodeMsgs:          nodeMsgs,
		requestWindows:    requestWindows,
	}
}

func (sm *stateMachine) propose(data []byte) *Actions {
	reqNo := sm.requestWindows[string(uint64ToBytes(sm.myConfig.ID))].allocateNext()
	requestData := &pb.RequestData{
		ClientId: uint64ToBytes(sm.myConfig.ID),
		ReqNo:    reqNo,
		Data:     data,
	}
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
		requestWindow, ok := sm.requestWindows[string(batchEntry.ClientId)]
		if !ok {
			panic("unexpected")
		}

		request := requestWindow.request(batchEntry.ReqNo)
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
		sm.activeEpoch = newEpoch(sm.epochChanger.pendingEpochTarget.leaderNewEpoch, sm.checkpointTracker, sm.requestWindows, sm.epochChanger.lastActiveEpoch, sm.networkConfig, sm.myConfig)
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

	for _, requestWindow := range sm.requestWindows {
		requestWindow.garbageCollect(seqNo)
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
		seqNo := processResult.Batch.SeqNo
		actions.Append(sm.activeEpoch.applyProcessResult(seqNo, processResult.Digest))
	}

	return actions
}

func (sm *stateMachine) applyPreprocessResult(preprocessResult *PreprocessResult) *Actions {
	requestWindow, ok := sm.requestWindows[string(preprocessResult.RequestData.ClientId)]
	if !ok {
		panic("unexpected")
	}

	requestWindow.allocate(preprocessResult.RequestData, preprocessResult.Digest)

	actions := &Actions{}

	if sm.activeEpoch != nil {
		sm.activeEpoch.proposer.stepRequestWindow(string(preprocessResult.RequestData.ClientId))
		actions.Append(sm.activeEpoch.drainProposer())
	}

	return actions
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
	requestWindowsStatus := make([]*RequestWindowStatus, len(sm.requestWindows))

	nodes := make([]*NodeStatus, len(sm.networkConfig.Nodes))
	for i, nodeID := range sm.networkConfig.Nodes {
		nodeID := NodeID(nodeID)
		nodes[i] = sm.nodeMsgs[nodeID].status()
		requestWindowsStatus[i] = sm.requestWindows[string(uint64ToBytes(uint64(nodeID)))].status()
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

		lowWatermark = epoch.baseCheckpoint.SeqNo / uint64(len(buckets))

		if epoch != nil && len(epoch.checkpoints) > 0 {
			highWatermark = epoch.checkpoints[len(epoch.checkpoints)-1].end / uint64(len(buckets))
		} else {
			highWatermark = lowWatermark
		}
	}

	return &Status{
		NodeID:         sm.myConfig.ID,
		LowWatermark:   lowWatermark,
		HighWatermark:  highWatermark,
		EpochChanger:   sm.epochChanger.status(),
		RequestWindows: requestWindowsStatus,
		Buckets:        buckets,
		Checkpoints:    checkpoints,
		Nodes:          nodes,
	}
}

type Status struct {
	NodeID         uint64
	LowWatermark   uint64
	HighWatermark  uint64
	EpochChanger   *EpochChangerStatus
	Nodes          []*NodeStatus
	Buckets        []*BucketStatus
	Checkpoints    []*CheckpointStatus
	RequestWindows []*RequestWindowStatus
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("===========================================\n"))
	buffer.WriteString(fmt.Sprintf("NodeID=%d, LowWatermark=%d, HighWatermark=%d, Epoch=%d\n", s.NodeID, s.LowWatermark, s.HighWatermark, s.EpochChanger.LastActiveEpoch))
	buffer.WriteString(fmt.Sprintf("===========================================\n\n"))

	buffer.WriteString("=== Epoch Changer ===\n")
	buffer.WriteString(fmt.Sprintf("Change is in state: %d, last active epoch %d\n", s.EpochChanger.State, s.EpochChanger.LastActiveEpoch))
	for _, et := range s.EpochChanger.EpochTargets {
		buffer.WriteString(fmt.Sprintf("Target Epoch %d:\n", et.Number))
		buffer.WriteString(fmt.Sprintf("  EpochChanges: %v\n", et.EpochChanges))
		buffer.WriteString(fmt.Sprintf("  Echos: %v\n", et.Echos))
		buffer.WriteString(fmt.Sprintf("  Readies: %v\n", et.Readies))
		buffer.WriteString(fmt.Sprintf("  Suspicions: %v\n", et.Suspicions))
	}
	buffer.WriteString("\n")
	buffer.WriteString("=====================\n")
	buffer.WriteString("\n")

	hRule := func() {
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString("--")
		}
	}

	for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
		magnitude := math.Pow10(i - 1)
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString(fmt.Sprintf(" %d", seqNo/uint64(magnitude)%10))
		}
		buffer.WriteString("\n")
	}

	if s.LowWatermark == s.HighWatermark {
		buffer.WriteString("=== Empty Watermarks ===\n")
		return buffer.String()
	}

	for _, nodeStatus := range s.Nodes {
		hRule()
		buffer.WriteString(fmt.Sprintf("- === Node %d === \n", nodeStatus.ID))
		for bucket, bucketStatus := range nodeStatus.BucketStatuses {
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
				if seqNo == nodeStatus.LastCheckpoint {
					buffer.WriteString("|X")
					continue
				}

				if seqNo == bucketStatus.LastCommit {
					buffer.WriteString("|C")
					continue
				}

				if seqNo == bucketStatus.LastPrepare {
					buffer.WriteString("|P")
					continue
				}
				buffer.WriteString("| ")
			}

			if bucketStatus.IsLeader {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d (Leader)\n", bucket))
			} else {
				buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucket))
			}
		}
	}

	hRule()
	buffer.WriteString("- === Buckets ===\n")

	for _, bucketStatus := range s.Buckets {
		buffer.WriteString("| ")
		for _, state := range bucketStatus.Sequences {
			switch state {
			case Uninitialized:
				buffer.WriteString("| ")
			case Allocated:
				buffer.WriteString("|A")
			case Invalid:
				buffer.WriteString("|I")
			case Preprepared:
				buffer.WriteString("|Q")
			case Prepared:
				buffer.WriteString("|P")
			case Committed:
				buffer.WriteString("|C")
			}
		}
		if bucketStatus.Leader {
			buffer.WriteString(fmt.Sprintf("| Bucket=%d (LocalLeader)\n", bucketStatus.ID))
		} else {
			buffer.WriteString(fmt.Sprintf("| Bucket=%d\n", bucketStatus.ID))
		}
	}

	hRule()
	buffer.WriteString("- === Checkpoints ===\n")
	i := 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		if len(s.Checkpoints) > i {
			checkpoint := s.Checkpoints[i]
			if seqNo == checkpoint.SeqNo/uint64(len(s.Buckets)) {
				buffer.WriteString(fmt.Sprintf("|%d", checkpoint.MaxAgreements))
				i++
				continue
			}
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Max Agreements\n")
	i = 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		if len(s.Checkpoints) > i {
			checkpoint := s.Checkpoints[i]
			if seqNo == s.Checkpoints[i].SeqNo/uint64(len(s.Buckets)) {
				switch {
				case checkpoint.NetQuorum && !checkpoint.LocalDecision:
					buffer.WriteString("|N")
				case checkpoint.NetQuorum && checkpoint.LocalDecision:
					buffer.WriteString("|G")
				case !checkpoint.NetQuorum && checkpoint.LocalDecision:
					buffer.WriteString("|M")
				default:
					buffer.WriteString("|P")
				}
				i++
				continue
			}
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Status\n")

	hRule()
	buffer.WriteString("-\n")

	buffer.WriteString("\n\n Request Windows\n")
	hRule()
	for i, rws := range s.RequestWindows {
		buffer.WriteString(fmt.Sprintf("\nNode %d L/H %d/%d : %v\n", i, rws.LowWatermark, rws.HighWatermark, rws.Allocated))
		hRule()
	}

	return buffer.String()
}
