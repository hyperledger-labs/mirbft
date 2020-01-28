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
	myConfig *Config
	nodeMsgs map[NodeID]*nodeMsgs
	proposer *proposer
	ticks    uint64

	epochs            []*epoch
	checkpointWindows []*checkpointWindow
}

func newStateMachine(config *epochConfig) *stateMachine {
	oddities := &oddities{
		logger: config.myConfig.Logger,
	}
	nodeMsgs := map[NodeID]*nodeMsgs{}
	for _, id := range config.nodes {
		nodeMsgs[id] = newNodeMsgs(id, config, oddities)
	}

	checkpointWindows := []*checkpointWindow{}
	lastEnd := SeqNo(0)
	for i := 0; i < 2; i++ {
		newLastEnd := lastEnd + config.checkpointInterval
		cw := newCheckpointWindow(lastEnd+1, newLastEnd, config)
		checkpointWindows = append(checkpointWindows, cw)
		lastEnd = newLastEnd
	}

	proposer := newProposer(config)
	proposer.maxAssignable = config.checkpointInterval
	// TODO collapse this logic with the new checkpoint allocation logic

	return &stateMachine{
		myConfig: config.myConfig,
		epochs: []*epoch{
			{
				config:            config,
				suspicions:        map[NodeID]struct{}{},
				changes:           map[NodeID]*pb.EpochChange{},
				checkpointWindows: append([]*checkpointWindow{}, checkpointWindows...), // (copy)
			},
		},
		nodeMsgs:          nodeMsgs,
		checkpointWindows: checkpointWindows,
		proposer:          proposer,
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

func (sm *stateMachine) checkpointWindowForSeqNo(seqNo SeqNo) *checkpointWindow {
	var currentEpochConfig *epochConfig
	for _, ec := range sm.epochs {
		if ec.config.plannedExpiration >= seqNo {
			currentEpochConfig = ec.config
			break
		}
	}

	if currentEpochConfig == nil {
		return nil
	}

	offset := seqNo - SeqNo(sm.checkpointWindows[0].start)
	index := offset / SeqNo(currentEpochConfig.checkpointInterval)
	if int(index) >= len(sm.checkpointWindows) {
		return nil
	}
	return sm.checkpointWindows[index]
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
			actions.Append(sm.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyPreprepareMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Batch))
		case *pb.Msg_Prepare:
			msg := innerMsg.Prepare
			actions.Append(sm.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyPrepareMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest))
		case *pb.Msg_Commit:
			msg := innerMsg.Commit
			actions.Append(sm.checkpointWindowForSeqNo(SeqNo(msg.SeqNo)).applyCommitMsg(source, SeqNo(msg.SeqNo), BucketID(msg.Bucket), msg.Digest))
		case *pb.Msg_Checkpoint:
			msg := innerMsg.Checkpoint
			actions.Append(sm.checkpointMsg(source, SeqNo(msg.SeqNo), msg.Value))
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
			sm.suspectMsg(source, innerMsg.Suspect.Epoch)
		case *pb.Msg_EpochChange:
			sm.epochChangeMsg(source, innerMsg.EpochChange)
		default:
			// This should be unreachable, as the nodeMsgs filters based on type as well
			panic("unexpected bad message type, should have been detected earlier")
		}
	}

	return actions
}

func (sm *stateMachine) suspectMsg(source NodeID, epoch uint64) {
	for _, ew := range sm.epochs {
		if ew.config.number == epoch {
			ew.suspicions[source] = struct{}{}
		}
	}
}

func (sm *stateMachine) epochChangeMsg(source NodeID, msg *pb.EpochChange) {
	for _, ew := range sm.epochs {
		if ew.config.number == msg.Epoch {
			ew.changes[source] = msg
		}
	}
}

func (sm *stateMachine) checkpointMsg(source NodeID, seqNo SeqNo, value []byte) *Actions {
	cw := sm.checkpointWindowForSeqNo(seqNo)

	actions := cw.applyCheckpointMsg(source, value)

	lastCW := sm.checkpointWindows[len(sm.checkpointWindows)-1]
	secondToLastCW := sm.checkpointWindows[len(sm.checkpointWindows)-2]

	var nextCWEpochConfig *epochConfig

	if lastCW.epochConfig.plannedExpiration == lastCW.end {
		// The next checkpoint window will belong to a new epoch

		for _, ec := range sm.epochs {
			if ec.config.plannedExpiration > lastCW.end {
				nextCWEpochConfig = ec.config
				break
			}
		}

		if nextCWEpochConfig == nil {
			// We must still be waiting for the next epoch config from the leader

			panic("unhandled")
		}
	} else {
		nextCWEpochConfig = lastCW.epochConfig
	}

	if secondToLastCW.garbageCollectible {
		sm.proposer.maxAssignable = lastCW.end
		sm.checkpointWindows = append(
			sm.checkpointWindows,
			newCheckpointWindow(
				lastCW.end+1,
				lastCW.end+nextCWEpochConfig.checkpointInterval,
				nextCWEpochConfig,
			),
		)
	}

	actions.Append(sm.proposer.drainQueue())
	for len(sm.checkpointWindows) > 2 && (sm.checkpointWindows[0].obsolete || sm.checkpointWindows[1].garbageCollectible) {
		sm.checkpointWindows = sm.checkpointWindows[1:]
	}

	for _, node := range sm.nodeMsgs {
		node.moveWatermarks()
	}

	return actions
}

func (sm *stateMachine) processResults(results ActionResults) *Actions {
	actions := &Actions{}
	for i, preprocessResult := range results.Preprocesses {
		sm.myConfig.Logger.Debug("applying preprocess result", zap.Int("index", i))
		actions.Append(sm.applyPreprocessResult(preprocessResult))
	}

	for i, digestResult := range results.Digests {
		sm.myConfig.Logger.Debug("applying digest result", zap.Int("index", i))
		seqNo := digestResult.Entry.SeqNo
		actions.Append(sm.checkpointWindowForSeqNo(SeqNo(seqNo)).applyDigestResult(SeqNo(seqNo), BucketID(digestResult.Entry.BucketID), digestResult.Digest))
	}

	for i, validateResult := range results.Validations {
		sm.myConfig.Logger.Debug("applying validate result", zap.Int("index", i))
		seqNo := validateResult.Entry.SeqNo
		actions.Append(sm.checkpointWindowForSeqNo(SeqNo(seqNo)).applyValidateResult(SeqNo(seqNo), BucketID(validateResult.Entry.BucketID), validateResult.Valid))
	}

	for i, checkpointResult := range results.Checkpoints {
		sm.myConfig.Logger.Debug("applying checkpoint result", zap.Int("index", i))
		actions.Append(sm.applyCheckpointResult(SeqNo(checkpointResult.SeqNo), checkpointResult.Value))
	}

	return actions
}

func (sm *stateMachine) applyCheckpointResult(seqNo SeqNo, value []byte) *Actions {
	cw := sm.checkpointWindowForSeqNo(seqNo)
	if cw == nil {
		panic("received an unexpected checkpoint result")
	}
	return cw.applyCheckpointResult(value)
}

func (sm *stateMachine) applyPreprocessResult(preprocessResult PreprocessResult) *Actions {
	// XXX it's tricky dealing with this when we are mid-epoch change, as we need to
	// decide which epoch config to use to assign it a bucket.  The immediate scheme that
	// comes to mind, is to evaluate it first, using the old epoch config, if we own it,
	// great propose it.  If we don't, check using the next epoch config, if we own it,
	// also propose it.  The tricky part comes when evaluating it on the other side, today
	// we don't enforce that the message really belongs in the assigned bucket.  In fact,
	// we deliberately distribute messages round robin across all buckets a node owns.
	// Since there's nuance to this though, ignoring for now, and always using the most recent
	// epoch config when deciding if 'we own' or not.  This will likely be a bug as soon
	// as we implement planned epoch rotation.
	lastCW := sm.checkpointWindows[len(sm.checkpointWindows)-1]

	currentEpochConfig := lastCW.epochConfig
	bucketID := BucketID(preprocessResult.Cup % uint64(len(currentEpochConfig.buckets)))
	nodeID := currentEpochConfig.buckets[bucketID]
	if nodeID == NodeID(sm.myConfig.ID) {
		return sm.proposer.propose(preprocessResult.Proposal.Data)
	}

	if preprocessResult.Proposal.Source == sm.myConfig.ID {
		// I originated this proposal, but someone else leads this bucket,
		// forward the message to them
		return &Actions{
			Unicast: []Unicast{
				{
					Target: uint64(nodeID),
					Msg: &pb.Msg{
						Type: &pb.Msg_Forward{
							Forward: &pb.Forward{
								Epoch:  currentEpochConfig.number,
								Bucket: uint64(bucketID),
								Data:   preprocessResult.Proposal.Data,
							},
						},
					},
				},
			},
		}
	}

	// Someone forwarded me this proposal, but I'm not responsible for it's bucket
	// TODO, log oddity? Assign it to the wrong bucket? Forward it again?
	return &Actions{}
}

func (sm *stateMachine) tick() *Actions {
	actions := &Actions{}

	sm.ticks++
	if sm.myConfig.HeartbeatTicks != 0 && sm.ticks%uint64(sm.myConfig.HeartbeatTicks) == 0 {
		actions.Append(sm.proposer.noopAdvance())
	}

	for _, cw := range sm.checkpointWindows {
		actions.Append(cw.tick())
	}

	return actions
}

func (sm *stateMachine) status() *Status {
	epochConfig := sm.epochs[0].config

	nodes := make([]*NodeStatus, len(epochConfig.nodes))
	for i, nodeID := range epochConfig.nodes {
		nodes[i] = sm.nodeMsgs[nodeID].status()
	}

	var buckets []*BucketStatus
	checkpoints := []*CheckpointStatus{}

	for _, cw := range sm.checkpointWindows {
		checkpoints = append(checkpoints, cw.status())
		if buckets == nil {
			buckets = make([]*BucketStatus, len(cw.buckets))
			for i := BucketID(0); i < BucketID(len(sm.checkpointWindows[0].buckets)); i++ {
				buckets[int(i)] = cw.buckets[i].status()
			}
		} else {
			for i := BucketID(0); i < BucketID(len(sm.checkpointWindows[0].buckets)); i++ {
				buckets[int(i)].Sequences = append(buckets[int(i)].Sequences, cw.buckets[i].status().Sequences...)
			}
		}
	}

	lowWatermark := sm.checkpointWindows[0].start
	highWatermark := sm.checkpointWindows[len(sm.checkpointWindows)-1].end

	return &Status{
		LowWatermark:  lowWatermark,
		HighWatermark: highWatermark,
		EpochNumber:   epochConfig.number,

		Buckets:     buckets,
		Checkpoints: checkpoints,
	}
}

type Status struct {
	LowWatermark  SeqNo
	HighWatermark SeqNo
	EpochNumber   uint64
	Nodes         []*NodeStatus
	Buckets       []*BucketStatus
	Checkpoints   []*CheckpointStatus
}

func (s *Status) Pretty() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("LowWatermark=%d, HighWatermark=%d, Epoch=%d\n\n", s.LowWatermark, s.HighWatermark, s.EpochNumber))

	hRule := func() {
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString("--")
		}
	}

	for i := len(fmt.Sprintf("%d", s.HighWatermark)); i > 0; i-- {
		magnitude := SeqNo(math.Pow10(i - 1))
		for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
			buffer.WriteString(fmt.Sprintf(" %d", seqNo/magnitude%10))
		}
		buffer.WriteString("\n")
	}

	if s.LowWatermark == s.HighWatermark {
		buffer.WriteString("Empty Watermarks\n")
		return buffer.String()
	}

	for _, nodeStatus := range s.Nodes {

		fmt.Printf("%+v\n\n\n", nodeStatus)

		hRule()
		buffer.WriteString(fmt.Sprintf("- === Node %d === \n", nodeStatus.ID))
		for bucket, bucketStatus := range nodeStatus.BucketStatuses {
			for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
				if seqNo == SeqNo(bucketStatus.LastCheckpoint) {
					buffer.WriteString("|X")
					continue
				}

				if seqNo == SeqNo(bucketStatus.LastCommit) {
					buffer.WriteString("|C")
					continue
				}

				if seqNo == SeqNo(bucketStatus.LastPrepare) {
					if bucketStatus.IsLeader {
						buffer.WriteString("|Q")
					} else {
						buffer.WriteString("|P")
					}
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
			if seqNo == SeqNo(checkpoint.SeqNo) {
				buffer.WriteString(fmt.Sprintf("|%d", checkpoint.PendingCommits))
				i++
				continue
			}
		}
		buffer.WriteString("| ")
	}
	buffer.WriteString("| Pending Commits\n")
	i = 0
	for seqNo := s.LowWatermark; seqNo <= s.HighWatermark; seqNo++ {
		if len(s.Checkpoints) > i {
			checkpoint := s.Checkpoints[i]
			if seqNo == SeqNo(s.Checkpoints[i].SeqNo) {
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
