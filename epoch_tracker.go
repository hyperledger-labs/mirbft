/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
)

type epochTracker struct {
	currentEpoch           *epochTarget
	persisted              *persisted
	nodeBuffers            *nodeBuffers
	commitState            *commitState
	networkConfig          *pb.NetworkState_Config
	logger                 Logger
	myConfig               *pb.StateEvent_InitialParameters
	batchTracker           *batchTracker
	clientTracker          *clientTracker
	clientHashDisseminator *clientHashDisseminator
	futureMsgs             map[nodeID]*msgBuffer
	targets                map[uint64]*epochTarget
	needsStateTransfer     bool

	maxEpochs              map[nodeID]uint64
	maxCorrectEpoch        uint64
	ticksOutOfCorrectEpoch int
}

func newEpochTracker(
	persisted *persisted,
	nodeBuffers *nodeBuffers,
	commitState *commitState,
	networkConfig *pb.NetworkState_Config,
	logger Logger,
	myConfig *pb.StateEvent_InitialParameters,
	batchTracker *batchTracker,
	clientTracker *clientTracker,
	clientHashDisseminator *clientHashDisseminator,
) *epochTracker {
	return &epochTracker{
		persisted:              persisted,
		nodeBuffers:            nodeBuffers,
		commitState:            commitState,
		myConfig:               myConfig,
		logger:                 logger,
		batchTracker:           batchTracker,
		clientTracker:          clientTracker,
		clientHashDisseminator: clientHashDisseminator,
		targets:                map[uint64]*epochTarget{},
		maxEpochs:              map[nodeID]uint64{},
	}
}

func (et *epochTracker) reinitialize() *Actions {
	et.networkConfig = et.commitState.activeState.Config

	newFutureMsgs := map[nodeID]*msgBuffer{}
	for _, id := range et.networkConfig.Nodes {
		futureMsgs, ok := et.futureMsgs[nodeID(id)]
		if !ok {
			futureMsgs = newMsgBuffer(
				"future-epochs",
				et.nodeBuffers.nodeBuffer(nodeID(id)),
			)
		}
		newFutureMsgs[nodeID(id)] = futureMsgs
	}
	et.futureMsgs = newFutureMsgs

	actions := &Actions{}
	var lastNEntry *pb.NEntry
	var lastECEntry *pb.ECEntry
	var lastFEntry *pb.FEntry
	var highestPreprepared uint64

	et.persisted.iterate(logIterator{
		onNEntry: func(nEntry *pb.NEntry) {
			lastNEntry = nEntry
		},
		onFEntry: func(fEntry *pb.FEntry) {
			lastFEntry = fEntry
		},
		onECEntry: func(ecEntry *pb.ECEntry) {
			lastECEntry = ecEntry
		},
		onQEntry: func(qEntry *pb.QEntry) {
			if qEntry.SeqNo > highestPreprepared {
				highestPreprepared = qEntry.SeqNo
			}
		},
		onCEntry: func(cEntry *pb.CEntry) {
			// In the state transfer case, we may
			// have a CEntry for a seqno we have no QEntry
			if cEntry.SeqNo > highestPreprepared {
				highestPreprepared = cEntry.SeqNo
			}
		},

		// TODO, implement
		onSuspect: func(*pb.Suspect) {},
	})

	var lastEpochConfig *pb.EpochConfig
	graceful := false
	switch {
	case lastNEntry != nil && lastFEntry != nil:
		assertGreaterThan(lastNEntry.EpochConfig.Number, lastFEntry.EndsEpochConfig.Number, "new epoch number must not be less than last terminated epoch")
		lastEpochConfig = lastNEntry.EpochConfig
		graceful = false
	case lastNEntry != nil:
		lastEpochConfig = lastNEntry.EpochConfig
		graceful = false
	case lastFEntry != nil:
		lastEpochConfig = lastFEntry.EndsEpochConfig
		graceful = true
	default:
		panic("no active epoch and no last epoch in log")
	}

	switch {
	case lastNEntry != nil && (lastECEntry == nil || lastECEntry.EpochNumber <= lastNEntry.EpochConfig.Number):
		et.logger.Log(LevelDebug, "reinitializing during a currently active epoch")

		et.currentEpoch = newEpochTarget(
			lastNEntry.EpochConfig.Number,
			et.persisted,
			et.nodeBuffers,
			et.commitState,
			et.clientTracker,
			et.clientHashDisseminator,
			et.batchTracker,
			et.networkConfig,
			et.myConfig,
			et.logger,
		)

		startingSeqNo := highestPreprepared + 1
		for startingSeqNo%uint64(et.networkConfig.CheckpointInterval) != 1 {
			// Advance the starting seqno to the first sequence after
			// some checkpoint.  This ensures we do not start consenting
			// on sequences we have already consented on.  If we have
			// startingSeqNo != highestPreprepared + 1 after this loop,
			// then state transfer will be required, though we do
			// not have a state target yet.
			startingSeqNo++
			et.needsStateTransfer = true
		}
		et.currentEpoch.startingSeqNo = startingSeqNo
		et.currentEpoch.state = etResuming
		suspect := &pb.Suspect{
			Epoch: lastNEntry.EpochConfig.Number,
		}
		actions.concat(et.persisted.addSuspect(suspect))
		actions.send(et.networkConfig.Nodes, &pb.Msg{
			Type: &pb.Msg_Suspect{
				Suspect: suspect,
			},
		})
	case lastFEntry != nil && (lastECEntry == nil || lastECEntry.EpochNumber <= lastFEntry.EndsEpochConfig.Number):
		et.logger.Log(LevelDebug, "reinitializing immediately after graceful epoch end, but before epoch change sent, creating epoch change")
		// An epoch has just gracefully ended, and we have not yet tried to move to the next
		lastECEntry = &pb.ECEntry{
			EpochNumber: lastFEntry.EndsEpochConfig.Number + 1,
		}
		actions.concat(et.persisted.addECEntry(lastECEntry))
		fallthrough
	case lastECEntry != nil:
		// An epoch has ended (ungracefully or otherwise), and we have sent our epoch change

		et.logger.Log(LevelDebug, "reinitializing after epoch change persisted")

		if et.currentEpoch != nil && et.currentEpoch.number == lastECEntry.EpochNumber {
			// We have been reinitialized during an epoch change, no need to start fresh
			return actions.concat(et.currentEpoch.advanceState())
		}

		epochChange := et.persisted.constructEpochChange(lastECEntry.EpochNumber)
		parsedEpochChange, err := newParsedEpochChange(epochChange)
		assertEqualf(err, nil, "could not parse epoch change we generated: %s", err)

		et.currentEpoch = newEpochTarget(
			epochChange.NewEpoch,
			et.persisted,
			et.nodeBuffers,
			et.commitState,
			et.clientTracker,
			et.clientHashDisseminator,
			et.batchTracker,
			et.networkConfig,
			et.myConfig,
			et.logger,
		)

		et.currentEpoch.myEpochChange = parsedEpochChange

		// XXX this leader selection is wrong, but using while we modify the startup.
		// instead base it on the lastEpochConfig and whether that epoch ended gracefully.
		_, _ = lastEpochConfig, graceful
		et.currentEpoch.myLeaderChoice = et.networkConfig.Nodes
	default:
		// There's no active epoch, it did not end gracefully, or ungracefully
		panic("no recorded active epoch, ended epoch, or epoch change in log")
	}

	for _, id := range et.networkConfig.Nodes {
		et.futureMsgs[nodeID(id)].iterate(et.filter, func(source nodeID, msg *pb.Msg) {
			actions.concat(et.applyMsg(source, msg))
		})
	}

	return actions
}

func (et *epochTracker) advanceState() *Actions {
	if et.currentEpoch.state < etDone {
		return et.currentEpoch.advanceState()
	}

	if et.commitState.checkpointPending {
		// It simplifies our lives considerably to wait for checkpoints
		// before initiating epoch change.
		return &Actions{}
	}

	newEpochNumber := et.currentEpoch.number + 1
	if et.maxCorrectEpoch > newEpochNumber {
		newEpochNumber = et.maxCorrectEpoch
	}
	epochChange := et.persisted.constructEpochChange(newEpochNumber)

	myEpochChange, err := newParsedEpochChange(epochChange)
	assertEqualf(err, nil, "could not parse epoch change we generated: %s", err)

	et.currentEpoch = newEpochTarget(
		newEpochNumber,
		et.persisted,
		et.nodeBuffers,
		et.commitState,
		et.clientTracker,
		et.clientHashDisseminator,
		et.batchTracker,
		et.networkConfig,
		et.myConfig,
		et.logger,
	)
	et.currentEpoch.myEpochChange = myEpochChange
	et.currentEpoch.myLeaderChoice = []uint64{et.myConfig.Id} // XXX, wrong

	actions := et.persisted.addECEntry(&pb.ECEntry{
		EpochNumber: newEpochNumber,
	}).send(
		et.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_EpochChange{
				EpochChange: epochChange,
			},
		},
	)

	for _, id := range et.networkConfig.Nodes {
		et.futureMsgs[nodeID(id)].iterate(et.filter, func(source nodeID, msg *pb.Msg) {
			actions.concat(et.applyMsg(source, msg))
		})
	}

	return actions
}

func epochForMsg(msg *pb.Msg) uint64 {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_Preprepare:
		return innerMsg.Preprepare.Epoch
	case *pb.Msg_Prepare:
		return innerMsg.Prepare.Epoch
	case *pb.Msg_Commit:
		return innerMsg.Commit.Epoch
	case *pb.Msg_Suspect:
		return innerMsg.Suspect.Epoch
	case *pb.Msg_EpochChange:
		return innerMsg.EpochChange.NewEpoch
	case *pb.Msg_EpochChangeAck:
		return innerMsg.EpochChangeAck.EpochChange.NewEpoch
	case *pb.Msg_NewEpoch:
		return innerMsg.NewEpoch.NewConfig.Config.Number
	case *pb.Msg_NewEpochEcho:
		return innerMsg.NewEpochEcho.Config.Number
	case *pb.Msg_NewEpochReady:
		return innerMsg.NewEpochReady.Config.Number
	default:
		panic(fmt.Sprintf("unexpected bad epoch message type %T, this indicates a bug", msg.Type))
	}
}

func (et *epochTracker) filter(_ nodeID, msg *pb.Msg) applyable {
	epochNumber := epochForMsg(msg)

	switch {
	case epochNumber < et.currentEpoch.number:
		return past
	case epochNumber > et.currentEpoch.number:
		return future
	default:
		return current
	}
}

func (et *epochTracker) step(source nodeID, msg *pb.Msg) *Actions {
	epochNumber := epochForMsg(msg)

	switch {
	case epochNumber < et.currentEpoch.number:
		// past
		return &Actions{}
	case epochNumber > et.currentEpoch.number:
		// future
		maxEpoch := et.maxEpochs[source]
		if maxEpoch < epochNumber {
			et.maxEpochs[source] = epochNumber
		}
		et.futureMsgs[source].store(msg)
		return &Actions{}
	default:
		// current
		return et.applyMsg(source, msg)
	}
}

func (et *epochTracker) applyMsg(source nodeID, msg *pb.Msg) *Actions {
	target := et.currentEpoch

	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_Preprepare:
		return target.step(source, msg)
	case *pb.Msg_Prepare:
		return target.step(source, msg)
	case *pb.Msg_Commit:
		return target.step(source, msg)
	case *pb.Msg_Suspect:
		target.applySuspectMsg(source)
		return &Actions{}
	case *pb.Msg_EpochChange:
		return target.applyEpochChangeMsg(source, innerMsg.EpochChange)
	case *pb.Msg_EpochChangeAck:
		return target.applyEpochChangeAckMsg(source, nodeID(innerMsg.EpochChangeAck.Originator), innerMsg.EpochChangeAck.EpochChange)
	case *pb.Msg_NewEpoch:
		if innerMsg.NewEpoch.NewConfig.Config.Number%uint64(len(et.networkConfig.Nodes)) != uint64(source) {
			// TODO, log oddity
			return &Actions{}
		}
		return target.applyNewEpochMsg(innerMsg.NewEpoch)
	case *pb.Msg_NewEpochEcho:
		return target.applyNewEpochEchoMsg(source, innerMsg.NewEpochEcho)
	case *pb.Msg_NewEpochReady:
		return target.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady)
	default:
		panic(fmt.Sprintf("unexpected bad epoch message type %T, this indicates a bug", msg.Type))
	}
}

func (et *epochTracker) applyBatchHashResult(epoch, seqNo uint64, digest []byte) *Actions {
	if epoch != et.currentEpoch.number || et.currentEpoch.state != etInProgress {
		// TODO, should we try to see if it applies to the current epoch?
		return &Actions{}
	}

	return et.currentEpoch.activeEpoch.applyBatchHashResult(seqNo, digest)
}

func (et *epochTracker) tick() *Actions {
	for _, maxEpoch := range et.maxEpochs {
		if maxEpoch <= et.maxCorrectEpoch {
			continue
		}
		matches := 1
		for _, matchingEpoch := range et.maxEpochs {
			if matchingEpoch < maxEpoch {
				continue
			}
			matches++
		}

		if matches < someCorrectQuorum(et.networkConfig) {
			continue
		}

		et.maxCorrectEpoch = maxEpoch
	}

	if et.maxCorrectEpoch > et.currentEpoch.number {
		et.ticksOutOfCorrectEpoch++

		// TODO make this configurable
		if et.ticksOutOfCorrectEpoch > 10 {
			et.currentEpoch.state = etDone
		}
	}

	return et.currentEpoch.tick()
}

func (et *epochTracker) moveLowWatermark(seqNo uint64) *Actions {
	return et.currentEpoch.moveLowWatermark(seqNo)
}

func (et *epochTracker) applyEpochChangeDigest(hashResult *pb.HashResult_EpochChange, digest []byte) *Actions {
	targetNumber := hashResult.EpochChange.NewEpoch
	switch {
	case targetNumber < et.currentEpoch.number:
		// This is for an old epoch we no long care about
		return &Actions{}
	case targetNumber > et.currentEpoch.number:
		assertFailed("", "got an epoch change digest for epoch %d we are processing %d", targetNumber, et.currentEpoch.number)

	}
	return et.currentEpoch.applyEpochChangeDigest(hashResult, digest)
}

func (et *epochTracker) status() *status.EpochTracker {
	targets := make([]*status.EpochTarget, 0, len(et.targets))
	for number, target := range et.targets {
		ts := target.status()
		ts.Number = number
		targets = append(targets, ts)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].Number < targets[j].Number
	})

	return &status.EpochTracker{
		LastActiveEpoch: et.currentEpoch.number,
		State:           status.EpochTargetState(et.currentEpoch.state),
		EpochTargets:    targets,
	}
}
