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
	myConfig      *Config
	networkConfig *pb.NetworkConfig
	nodeMsgs      map[NodeID]*nodeMsgs

	activeEpoch       *epoch
	checkpointTracker *checkpointTracker
	epochChanger      *epochChanger
}

func newStateMachine(networkConfig *pb.NetworkConfig, myConfig *Config) *stateMachine {
	oddities := &oddities{
		logger: myConfig.Logger,
	}
	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range networkConfig.Nodes {
		nodeMsgs[NodeID(id)] = newNodeMsgs(NodeID(id), networkConfig, myConfig, oddities)
	}

	fakeCheckpoint := &pb.Checkpoint{
		SeqNo: 0,
		Value: []byte("TODO, get from state"),
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
	}
}

func (sm *stateMachine) propose(data []byte) *Actions {
	return &Actions{
		Preprocess: []Proposal{
			{
				Source: sm.myConfig.ID,
				Data:   data,
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

	actions := &Actions{}

	for {
		msg := nodeMsgs.next()
		if msg == nil {
			break
		}

		switch innerMsg := msg.Type.(type) {
		case *pb.Msg_Preprepare:
			msg := innerMsg.Preprepare
			actions.Append(sm.activeEpoch.applyPreprepareMsg(source, msg.SeqNo, msg.Batch))
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
			msg := innerMsg.Forward
			// TODO should we have a separate validate step here?  How do we prevent
			// forwarded messages with bad data from poisoning our batch?
			actions.Append(&Actions{
				Preprocess: []Proposal{
					{
						Source: uint64(source),
						Data:   msg.Data,
					},
				},
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

	return actions
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
		sm.activeEpoch = newEpoch(sm.epochChanger.pendingEpochTarget.leaderNewEpoch, sm.checkpointTracker, sm.epochChanger.lastActiveEpoch, sm.networkConfig, sm.myConfig)
		for _, sequence := range sm.activeEpoch.sequences {
			if sequence.state >= Prepared {
				actions.Broadcast = append(actions.Broadcast, &pb.Msg{
					Type: &pb.Msg_Commit{
						Commit: &pb.Commit{
							SeqNo:  sequence.entry.SeqNo,
							Epoch:  sequence.entry.Epoch,
							Digest: sequence.digest,
						},
					},
				})
			}
		}
		actions.Append(sm.activeEpoch.advanceUncommitted())
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

	return sm.activeEpoch.moveWatermarks()
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}

	if sm.activeEpoch == nil {
		// TODO, this is a little heavy handed, we should probably
		// work with the persistence so we don't redo the effort.
		return actions
	}

	for _, preprocessResult := range results.Preprocesses {
		// sm.myConfig.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.applyPreprocessResult(preprocessResult))
	}

	for _, digestResult := range results.Digests {
		// sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
		seqNo := digestResult.Entry.SeqNo
		actions.Append(sm.activeEpoch.applyDigestResult(seqNo, digestResult.Digest))
	}

	for _, validateResult := range results.Validations {
		// sm.myConfig.Logger.Debug("applying validate result", zap.Int("index", i))
		seqNo := validateResult.Entry.SeqNo
		actions.Append(sm.activeEpoch.applyValidateResult(seqNo, validateResult.Valid))
	}

	for _, checkpointResult := range results.Checkpoints {
		// sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.checkpointTracker.applyCheckpointResult(checkpointResult.SeqNo, checkpointResult.Value))
	}

	return actions
}

func (sm *stateMachine) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
	// TODO we should probably have some sophisticated logic here for graceful epoch
	// rotations.  We should prefer finalizing the old epoch and populating the new.
	// We should also handle the case that we are out of space in the current active
	// epoch.
	return sm.activeEpoch.applyPreprocessResult(preprocessResult)
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
	var suspicions, epochChanges []NodeID

	nodes := make([]*NodeStatus, len(sm.networkConfig.Nodes))
	for i, nodeID := range sm.networkConfig.Nodes {
		nodeID := NodeID(nodeID)
		nodes[i] = sm.nodeMsgs[nodeID].status()

		if _, ok := sm.epochChanger.pendingEpochTarget.suspicions[nodeID]; ok {
			suspicions = append(suspicions, nodeID)
		}
		if _, ok := sm.epochChanger.pendingEpochTarget.changes[nodeID]; ok {
			epochChanges = append(epochChanges, nodeID)
		}
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
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochNumber:   sm.epochChanger.highestObservedCorrectEpoch,
		Suspicions:    suspicions,
		EpochChanges:  epochChanges,
		EpochState:    int(sm.epochChanger.state),
		Buckets:       buckets,
		Checkpoints:   checkpoints,
		Nodes:         nodes,
	}
}

type Status struct {
	LowWatermark  uint64
	HighWatermark uint64
	EpochNumber   uint64
	Suspicions    []NodeID
	EpochChanges  []NodeID
	EpochState    int
	Nodes         []*NodeStatus
	Buckets       []*BucketStatus
	Checkpoints   []*CheckpointStatus
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("===========================================\n"))
	buffer.WriteString(fmt.Sprintf("LowWatermark=%d, HighWatermark=%d, Epoch=%d\n", s.LowWatermark, s.HighWatermark, s.EpochNumber))
	buffer.WriteString(fmt.Sprintf("===========================================\n\n"))

	buffer.WriteString("=== Epoch Changes ===\n")
	buffer.WriteString(fmt.Sprintf("In state: %d\n", s.EpochState))
	buffer.WriteString("Suspicions:")
	for _, nodeID := range s.Suspicions {
		buffer.WriteString(fmt.Sprintf(" %d", nodeID))
	}
	buffer.WriteString("\n")
	buffer.WriteString("EpochChanges:")
	for _, nodeID := range s.EpochChanges {
		buffer.WriteString(fmt.Sprintf(" %d", nodeID))
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
			case Preprepared:
				buffer.WriteString("|Q")
			case Digested:
				buffer.WriteString("|D")
			case InvalidBatch:
				buffer.WriteString("|I")
			case Validated:
				buffer.WriteString("|V")
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
				case checkpoint.NetQuorum && !checkpoint.LocalAgreement:
					buffer.WriteString("|N")
				case checkpoint.NetQuorum && checkpoint.LocalAgreement:
					buffer.WriteString("|G")
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

	return buffer.String()
}
