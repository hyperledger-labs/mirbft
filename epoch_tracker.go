/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
)

type epochTracker struct {
	currentEpoch  *epochTarget
	persisted     *persisted
	networkConfig *pb.NetworkState_Config
	logger        Logger
	myConfig      *pb.StateEvent_InitialParameters
	batchTracker  *batchTracker
	clientWindows *clientWindows
	targets       map[uint64]*epochTarget
	nodeMsgs      map[NodeID]*nodeMsgs
}

func newEpochChanger(
	persisted *persisted,
	networkConfig *pb.NetworkState_Config,
	logger Logger,
	myConfig *pb.StateEvent_InitialParameters,
	batchTracker *batchTracker,
	clientWindows *clientWindows,
	nodeMsgs map[NodeID]*nodeMsgs,
) *epochTracker {
	et := &epochTracker{
		persisted:     persisted,
		networkConfig: networkConfig,
		myConfig:      myConfig,
		batchTracker:  batchTracker,
		clientWindows: clientWindows,
		targets:       map[uint64]*epochTarget{},
		nodeMsgs:      nodeMsgs, // XXX hack
	}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			cEntry := d.CEntry
			et.currentEpoch = et.target(cEntry.EpochConfig.Number)
			et.currentEpoch.state = ready
		case *pb.Persistent_EpochChange:
			epochChange := d.EpochChange
			parsedEpochChange, err := newParsedEpochChange(epochChange)
			if err != nil {
				panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
			}

			et.currentEpoch = et.target(epochChange.NewEpoch)
			et.currentEpoch.myEpochChange = parsedEpochChange
			et.currentEpoch.myLeaderChoice = networkConfig.Nodes // XXX this is generally wrong, but using while we modify the startup
		case *pb.Persistent_NewEpochEcho:
		case *pb.Persistent_NewEpochReady:
		case *pb.Persistent_NewEpochStart:
		case *pb.Persistent_Suspect:
		}
	}

	return et
}

func (et *epochTracker) step(source NodeID, msg *pb.Msg) *Actions {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_Preprepare:
		msg := innerMsg.Preprepare
		return et.applyPreprepareMsg(source, msg.Epoch, msg.SeqNo, msg.Batch)
	case *pb.Msg_Prepare:
		msg := innerMsg.Prepare
		return et.applyPrepareMsg(source, msg.Epoch, msg.SeqNo, msg.Digest)
	case *pb.Msg_Commit:
		msg := innerMsg.Commit
		return et.applyCommitMsg(source, msg.Epoch, msg.SeqNo, msg.Digest)
	case *pb.Msg_Suspect:
		return et.applySuspectMsg(source, innerMsg.Suspect.Epoch)
	case *pb.Msg_EpochChange:
		return et.applyEpochChangeMsg(source, innerMsg.EpochChange)
	case *pb.Msg_EpochChangeAck:
		return et.applyEpochChangeAckMsg(source, innerMsg.EpochChangeAck)
	case *pb.Msg_NewEpoch:
		return et.applyNewEpochMsg(innerMsg.NewEpoch)
	case *pb.Msg_NewEpochEcho:
		return et.applyNewEpochEchoMsg(source, innerMsg.NewEpochEcho)
	case *pb.Msg_NewEpochReady:
		return et.applyNewEpochReadyMsg(source, innerMsg.NewEpochReady)
	default:
		panic(fmt.Sprintf("unexpected bad epoch message type %T, this indicates a bug", msg.Type))
	}
}

func (et *epochTracker) applyBatchHashResult(epoch, seqNo uint64, digest []byte) *Actions {
	if epoch != et.currentEpoch.number || et.currentEpoch.state != inProgress {
		// TODO, should we try to see if it applies to the current epoch?
		return &Actions{}
	}

	return et.currentEpoch.activeEpoch.applyBatchHashResult(seqNo, digest)
}

func (et *epochTracker) tick() *Actions {
	return et.currentEpoch.tick()
}

func (et *epochTracker) target(epoch uint64) *epochTarget {
	// TODO, we need to garbage collett in responst to
	// spammy suspicions and epoch changes.  Basically
	// if every suspett/epoch change has a corresponding
	// higher epoch sibling for that node in a later epoch
	// then we should clean up.

	target, ok := et.targets[epoch]
	if !ok {
		target = &epochTarget{
			number:        epoch,
			suspicions:    map[NodeID]struct{}{},
			changes:       map[NodeID]*epochChange{},
			strongChanges: map[NodeID]*parsedEpochChange{},
			echos:         map[*pb.NewEpochConfig]map[NodeID]struct{}{},
			readies:       map[*pb.NewEpochConfig]map[NodeID]struct{}{},
			isLeader:      epoch%uint64(len(et.networkConfig.Nodes)) == et.myConfig.Id,
			persisted:     et.persisted,
			networkConfig: et.networkConfig,
			logger:        et.logger,
			myConfig:      et.myConfig,
			batchTracker:  et.batchTracker,
			clientWindows: et.clientWindows,
		}
		et.targets[epoch] = target
	}
	return target
}

func (et *epochTracker) setPendingTarget(target *epochTarget) {
	for number := range et.targets {
		if number < target.number {
			delete(et.targets, number)
		}
	}
	et.currentEpoch = target
}

func (et *epochTracker) applySuspectMsg(source NodeID, epoch uint64) *Actions {
	target := et.target(epoch)
	target.applySuspectMsg(source)
	if target.state < done {
		return &Actions{}
	}

	epochChange := et.persisted.constructEpochChange(epoch + 1)

	newTarget := et.target(epoch + 1)
	et.setPendingTarget(newTarget)
	for _, nodeMsgs := range et.nodeMsgs {
		nodeMsgs.setActiveEpoch(nil)
	}

	var err error
	newTarget.myEpochChange, err = newParsedEpochChange(epochChange)
	if err != nil {
		panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
	}

	newTarget.myLeaderChoice = []uint64{et.myConfig.Id}

	return et.persisted.addEpochChange(epochChange).send(
		et.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_EpochChange{
				EpochChange: epochChange,
			},
		},
	)
}

func (et *epochTracker) applyPreprepareMsg(source NodeID, epoch, seqNo uint64, batch []*pb.RequestAck) *Actions {
	if epoch != et.currentEpoch.number {
		panic("TODO handle me")
	}

	return et.currentEpoch.activeEpoch.applyPreprepareMsg(source, seqNo, batch)
}

func (et *epochTracker) applyPrepareMsg(source NodeID, epoch, seqNo uint64, digest []byte) *Actions {
	if epoch != et.currentEpoch.number {
		panic("TODO handle me")
	}

	return et.currentEpoch.activeEpoch.applyPrepareMsg(source, seqNo, digest)
}

func (et *epochTracker) applyCommitMsg(source NodeID, epoch, seqNo uint64, digest []byte) *Actions {
	if epoch != et.currentEpoch.number {
		panic("TODO handle me")
	}

	return et.currentEpoch.activeEpoch.applyCommitMsg(source, seqNo, digest)
}

func (et *epochTracker) moveWatermarks(seqNo uint64) *Actions {
	if et.currentEpoch.state != inProgress {
		return &Actions{}
	}

	return et.currentEpoch.activeEpoch.moveWatermarks(seqNo)
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

func (et *epochTracker) applyEpochChangeMsg(source NodeID, msg *pb.EpochChange) *Actions {
	actions := &Actions{}
	if source != NodeID(et.myConfig.Id) {
		// We don't want to etho our own EpochChange message,
		// as we already broadcast/rebroadcast it.
		actions.send(
			et.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_EpochChangeAck{
					EpochChangeAck: &pb.EpochChangeAck{
						Originator:  uint64(source),
						EpochChange: msg,
					},
				},
			},
		)
	}

	// TODO, we could get away with one type of message, an 'EpochChange'
	// with an 'Origin', but it's a little less clear reading messages on the wire.
	target := et.target(msg.NewEpoch)
	return actions.concat(target.applyEpochChangeAckMsg(source, source, msg))
}

func (et *epochTracker) applyEpochChangeDigest(epochChange *pb.HashResult_EpochChange, digest []byte) *Actions {
	// TODO, fix all this stuttering and repitition
	target := et.target(epochChange.EpochChange.NewEpoch)
	return target.applyEpochChangeDigest(epochChange, digest)
}

func (et *epochTracker) applyEpochChangeAckMsg(source NodeID, ack *pb.EpochChangeAck) *Actions {
	target := et.target(ack.EpochChange.NewEpoch)
	return target.applyEpochChangeAckMsg(source, NodeID(ack.Originator), ack.EpochChange)
}

func (et *epochTracker) applyNewEpochMsg(msg *pb.NewEpoch) *Actions {
	target := et.target(msg.NewConfig.Config.Number)
	return target.applyNewEpochMsg(msg)
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

func (et *epochTracker) applyNewEpochEchoMsg(source NodeID, msg *pb.NewEpochEcho) *Actions {
	target := et.target(msg.NewConfig.Config.Number)
	return target.applyNewEpochEchoMsg(source, msg)
}

func (et *epochTracker) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	target := et.target(msg.NewConfig.Config.Number)
	oldState := target.state
	actions := target.applyNewEpochReadyMsg(source, msg)

	if oldState != target.state && target.state == inProgress {
		for _, nodeMsgs := range et.nodeMsgs {
			nodeMsgs.setActiveEpoch(target.activeEpoch)
		}
	}

	return actions
}

func (et *epochTracker) status() *EpochChangerStatus {
	targets := make([]*EpochTargetStatus, 0, len(et.targets))
	for number, target := range et.targets {
		ts := target.status()
		ts.Number = number
		targets = append(targets, ts)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].Number < targets[j].Number
	})

	return &EpochChangerStatus{
		LastActiveEpoch: et.currentEpoch.number,
		State:           et.currentEpoch.state,
		EpochTargets:    targets,
	}
}
