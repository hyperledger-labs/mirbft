/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"

	"github.com/pkg/errors"
)

type epochChanger struct {
	activeEpoch   *epochTarget
	persisted     *persisted
	networkConfig *pb.NetworkState_Config
	myConfig      *pb.StateEvent_InitialParameters
	batchTracker  *batchTracker
	clientWindows *clientWindows
	targets       map[uint64]*epochTarget
}

func newEpochChanger(
	persisted *persisted,
	networkConfig *pb.NetworkState_Config,
	myConfig *pb.StateEvent_InitialParameters,
	batchTracker *batchTracker,
	clientWindows *clientWindows,
) *epochChanger {
	ec := &epochChanger{
		persisted:     persisted,
		networkConfig: networkConfig,
		myConfig:      myConfig,
		batchTracker:  batchTracker,
		clientWindows: clientWindows,
		targets:       map[uint64]*epochTarget{},
	}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			cEntry := d.CEntry
			ec.activeEpoch = ec.target(cEntry.EpochConfig.Number)
			ec.activeEpoch.state = ready
		case *pb.Persistent_EpochChange:
			epochChange := d.EpochChange
			parsedEpochChange, err := newParsedEpochChange(epochChange)
			if err != nil {
				panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
			}

			ec.activeEpoch = ec.target(epochChange.NewEpoch)
			ec.activeEpoch.myEpochChange = parsedEpochChange
			ec.activeEpoch.myLeaderChoice = networkConfig.Nodes // XXX this is generally wrong, but using while we modify the startup
		case *pb.Persistent_NewEpochEcho:
		case *pb.Persistent_NewEpochReady:
		case *pb.Persistent_NewEpochStart:
		case *pb.Persistent_Suspect:
		}
	}

	return ec
}

func (ec *epochChanger) tick() *Actions {
	return ec.activeEpoch.tick()
}

func (ec *epochChanger) target(epoch uint64) *epochTarget {
	// TODO, we need to garbage collect in responst to
	// spammy suspicions and epoch changes.  Basically
	// if every suspect/epoch change has a corresponding
	// higher epoch sibling for that node in a later epoch
	// then we should clean up.

	target, ok := ec.targets[epoch]
	if !ok {
		target = &epochTarget{
			number:        epoch,
			suspicions:    map[NodeID]struct{}{},
			changes:       map[NodeID]*epochChange{},
			strongChanges: map[NodeID]*parsedEpochChange{},
			echos:         map[*pb.NewEpochConfig]map[NodeID]struct{}{},
			readies:       map[*pb.NewEpochConfig]map[NodeID]struct{}{},
			isLeader:      epoch%uint64(len(ec.networkConfig.Nodes)) == ec.myConfig.Id,
			persisted:     ec.persisted,
			networkConfig: ec.networkConfig,
			myConfig:      ec.myConfig,
			batchTracker:  ec.batchTracker,
			clientWindows: ec.clientWindows,
		}
		ec.targets[epoch] = target
	}
	return target
}

func (ec *epochChanger) setPendingTarget(target *epochTarget) {
	for number := range ec.targets {
		if number < target.number {
			delete(ec.targets, number)
		}
	}
	ec.activeEpoch = target
}

func (ec *epochChanger) applySuspectMsg(source NodeID, epoch uint64) *pb.EpochChange {
	target := ec.target(epoch)
	target.applySuspectMsg(source)
	if target.state < done {
		return nil
	}

	epochChange := ec.persisted.constructEpochChange(epoch + 1)

	newTarget := ec.target(epoch + 1)
	ec.setPendingTarget(newTarget)
	var err error
	newTarget.myEpochChange, err = newParsedEpochChange(epochChange)
	if err != nil {
		panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
	}

	newTarget.myLeaderChoice = []uint64{ec.myConfig.Id}

	return epochChange
}

/*
func (ec *epochChanger) chooseLeaders(epochChange *parsedEpochChange) []uint64 {
	if ec.lastActiveEpoch == nil {
		panic("this shouldn't happen")
	}

	oldLeaders := ec.lastActiveEpoch.config.leaders
	if len(oldLeaders) == 1 {
		return []uint64{ec.myConfig.ID}
	}

	// XXX the below logic is definitely wrong, it doesn't always result in a node
	// being kicked.

	var badNode uint64
	if ec.lastActiveEpoch.config.number+1 == epochChange.underlying.NewEpoch {
		var lowestEntry uint64
		for i := epochChange.lowWatermark + 1; i < epochChange.lowWatermark+uint64(ec.networkConfig.CheckpointInterval)*2; i++ {
			if _, ok := epochChange.pSet[i]; !ok {
				lowestEntry = i
				break
			}
		}

		if lowestEntry == 0 {
			// All of the sequence numbers within the watermarks prepared, so it's
			// unclear why the epoch failed, eliminate the previous epoch leader
			badNode = ec.lastActiveEpoch.config.number % uint64(len(ec.networkConfig.Nodes))
		} else {
			bucket := ec.lastActiveEpoch.config.seqToBucket(lowestEntry)
			badNode = uint64(ec.lastActiveEpoch.config.buckets[bucket])
		}
	} else {
		// If we never saw the last epoch start, we assume
		// that replica must be faulty.
		// Subtraction on epoch number is safe, as for epoch 0, lastActiveEpoch is nil
		badNode = (epochChange.underlying.NewEpoch - 1) % uint64(len(ec.networkConfig.Nodes))
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

func (ec *epochChanger) applyEpochChangeMsg(source NodeID, msg *pb.EpochChange) *Actions {
	actions := &Actions{}
	if source != NodeID(ec.myConfig.Id) {
		// We don't want to echo our own EpochChange message,
		// as we already broadcast/rebroadcast it.
		actions.send(
			ec.networkConfig.Nodes,
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
	target := ec.target(msg.NewEpoch)
	return actions.concat(target.applyEpochChangeAckMsg(source, source, msg))
}

func (ec *epochChanger) applyEpochChangeDigest(epochChange *pb.HashResult_EpochChange, digest []byte) *Actions {
	// TODO, fix all this stuttering and repitition
	target := ec.target(epochChange.EpochChange.NewEpoch)
	return target.applyEpochChangeDigest(epochChange, digest)
}

func (ec *epochChanger) applyEpochChangeAckMsg(source NodeID, ack *pb.EpochChangeAck) *Actions {
	target := ec.target(ack.EpochChange.NewEpoch)
	return target.applyEpochChangeAckMsg(source, NodeID(ack.Originator), ack.EpochChange)
}

func (ec *epochChanger) applyNewEpochMsg(msg *pb.NewEpoch) *Actions {
	target := ec.target(msg.NewConfig.Config.Number)
	return target.applyNewEpochMsg(msg)
}

// Summary of Bracha reliable broadcast from:
//   https://dcl.epfl.ch/site/_media/education/sdc_byzconsensus.pdf
//
// upon r-broadcast(m): // only Ps
// send message (SEND, m) to all
//
// upon receiving a message (SEND, m) from Ps:
// send message (ECHO, m) to all
//
// upon receiving ceil((n+t+1)/2)
// e messages(ECHO, m) and not having sent a READY message:
// send message (READY, m) to all
//
// upon receiving t+1 messages(READY, m) and not having sent a READY message:
// send message (READY, m) to all
//
// upon receiving 2t + 1 messages (READY, m):
// r-deliver(m)

func (ec *epochChanger) applyNewEpochEchoMsg(source NodeID, msg *pb.NewEpochEcho) *Actions {
	target := ec.target(msg.NewConfig.Config.Number)
	return target.applyNewEpochEchoMsg(source, msg)
}

func (ec *epochChanger) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	target := ec.target(msg.NewConfig.Config.Number)
	return target.applyNewEpochReadyMsg(source, msg)
}

func (ec *epochChanger) status() *EpochChangerStatus {
	targets := make([]*EpochTargetStatus, 0, len(ec.targets))
	for number, target := range ec.targets {
		ts := target.status()
		ts.Number = number
		targets = append(targets, ts)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].Number < targets[j].Number
	})

	return &EpochChangerStatus{
		LastActiveEpoch: ec.activeEpoch.number,
		State:           ec.activeEpoch.state,
		EpochTargets:    targets,
	}
}
