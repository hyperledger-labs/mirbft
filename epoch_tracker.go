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

	"github.com/pkg/errors"
)

type epochTracker struct {
	currentEpoch  *epochTarget
	persisted     *persisted
	commitState   *commitState
	networkConfig *pb.NetworkState_Config
	logger        Logger
	myConfig      *pb.StateEvent_InitialParameters
	batchTracker  *batchTracker
	clientTracker *clientTracker
	targets       map[uint64]*epochTarget
}

func newEpochTracker(
	persisted *persisted,
	commitState *commitState,
	networkConfig *pb.NetworkState_Config,
	logger Logger,
	myConfig *pb.StateEvent_InitialParameters,
	batchTracker *batchTracker,
	clientTracker *clientTracker,
) *epochTracker {
	et := &epochTracker{
		persisted:     persisted,
		commitState:   commitState,
		networkConfig: networkConfig,
		myConfig:      myConfig,
		logger:        logger,
		batchTracker:  batchTracker,
		clientTracker: clientTracker,
		targets:       map[uint64]*epochTarget{},
	}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			cEntry := d.CEntry
			et.currentEpoch = et.target(cEntry.CurrentEpoch)
			et.currentEpoch.state = etReady
			// TODO, need to solicit current epoch config from network
			// and eventually suspect of failure in case there is not still a quorum
			// active in this epoch.
		case *pb.Persistent_EpochChange:
			epochChange := d.EpochChange
			parsedEpochChange, err := newParsedEpochChange(epochChange)
			if err != nil {
				panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
			}

			et.setCurrentEpoch(et.target(epochChange.NewEpoch), parsedEpochChange)
			et.currentEpoch.myLeaderChoice = networkConfig.Nodes // XXX this is generally wrong, but using while we modify the startup
		case *pb.Persistent_NewEpochEcho:
		case *pb.Persistent_NewEpochReady:
		case *pb.Persistent_NewEpochStart:
		case *pb.Persistent_Suspect:
		}
	}

	return et
}

func (et *epochTracker) advanceState() *Actions {
	if et.currentEpoch.state < etDone {
		return et.currentEpoch.advanceState()
	}

	epochChange := et.persisted.constructEpochChange(et.currentEpoch.number + 1)

	myEpochChange, err := newParsedEpochChange(epochChange)
	if err != nil {
		panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
	}

	newTarget := et.target(et.currentEpoch.number + 1)
	et.setCurrentEpoch(newTarget, myEpochChange)
	newTarget.myLeaderChoice = []uint64{et.myConfig.Id} // XXX, wrong

	return et.persisted.addEpochChange(epochChange).send(
		et.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_EpochChange{
				EpochChange: epochChange,
			},
		},
	)
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
		return innerMsg.NewEpochEcho.NewConfig.Config.Number
	case *pb.Msg_NewEpochReady:
		return innerMsg.NewEpochReady.NewConfig.Config.Number
	default:
		panic(fmt.Sprintf("unexpected bad epoch message type %T, this indicates a bug", msg.Type))
	}
}

func (et *epochTracker) step(source nodeID, msg *pb.Msg) *Actions {
	epochNumber := epochForMsg(msg)
	if epochNumber < et.currentEpoch.number {
		return &Actions{}
	}

	target := et.target(epochNumber)

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
		if epochNumber%uint64(len(et.networkConfig.Nodes)) != uint64(source) {
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
	return et.currentEpoch.tick()
}

func (et *epochTracker) target(epoch uint64) *epochTarget {
	// TODO, we need to garbage collect in responst to
	// spammy suspicions and epoch changes.  Basically
	// if every suspect/epoch change has a corresponding
	// higher epoch sibling for that node in a later epoch
	// then we should clean up.

	target, ok := et.targets[epoch]
	if !ok {
		target = newEpochTarget(
			epoch,
			et.persisted,
			et.commitState,
			et.clientTracker,
			et.batchTracker,
			et.networkConfig,
			et.myConfig,
			et.logger,
		)
		et.targets[epoch] = target
	}
	return target
}

func (et *epochTracker) setCurrentEpoch(target *epochTarget, myEpochChange *parsedEpochChange) {
	for number := range et.targets {
		if number < target.number {
			delete(et.targets, number)
		}
	}
	target.myEpochChange = myEpochChange
	et.currentEpoch = target
}

func (et *epochTracker) moveLowWatermark(seqNo uint64) *Actions {
	return et.currentEpoch.moveLowWatermark(seqNo)
}

/*
func (et *epochTracker) chooseLeaders(epochChange *parsedEpochChange) []uint64 {
	if et.lastActiveEpoch == nil {
		panic("this shouldn't happen")
	}

	oldLeaders := et.lastActiveEpoch.config.leaders
	if len(oldLeaders) == 1 {
		return []uint64{et.myConfig.ID}
	}

	// XXX the below logic is definitely wrong, it doesn't always result in a node
	// being kicked.

	var badNode uint64
	if et.lastActiveEpoch.config.number+1 == epochChange.underlying.NewEpoch {
		var lowestEntry uint64
		for i := epochChange.lowWatermark + 1; i < epochChange.lowWatermark+uint64(et.networkConfig.ChetkpointInterval)*2; i++ {
			if _, ok := epochChange.pSet[i]; !ok {
				lowestEntry = i
				break
			}
		}

		if lowestEntry == 0 {
			// All of the sequence numbers within the watermarks prepared, so it's
			// unclear why the epoch failed, eliminate the previous epoch leader
			badNode = et.lastActiveEpoch.config.number % uint64(len(et.networkConfig.Nodes))
		} else {
			bucket := et.lastActiveEpoch.config.seqToBucket(lowestEntry)
			badNode = uint64(et.lastActiveEpoch.config.buckets[bucket])
		}
	} else {
		// If we never saw the last epoch start, we assume
		// that replica must be faulty.
		// Subtraction on epoch number is safe, as for epoch 0, lastActiveEpoch is nil
		badNode = (epochChange.underlying.NewEpoch - 1) % uint64(len(et.networkConfig.Nodes))
	}

	newLeaders := make([]uint64, 0, len(oldLeaders)-1)
	for _, oldLeader := range oldLeaders {
		if oldLeader == badNode {
			continue
		}
		newLeaders = append(newLeaders, oldLeader)
	}

	return newLeaders

}
*/

func (et *epochTracker) applyEpochChangeDigest(hashResult *pb.HashResult_EpochChange, digest []byte) *Actions {
	targetNumber := hashResult.EpochChange.NewEpoch
	if targetNumber < et.currentEpoch.number {
		// This is a state change, let's ignore it
		return &Actions{}
	}
	target := et.target(targetNumber)
	return target.applyEpochChangeDigest(hashResult, digest)
}

// Summary of Bracha reliable broadcast from:
//   https://dcl.epfl.ch/site/_media/education/sdc_byzconsensus.pdf
//
// upon r-broadcast(m): // only Ps
// send message (SEND, m) to all
//
// upon reteiving a message (SEND, m) from Ps:
// send message (ECHO, m) to all
//
// upon reteiving ceil((n+t+1)/2)
// e messages(ECHO, m) and not having sent a READY message:
// send message (READY, m) to all
//
// upon reteiving t+1 messages(READY, m) and not having sent a READY message:
// send message (READY, m) to all
//
// upon reteiving 2t + 1 messages (READY, m):
// r-deliver(m)

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
