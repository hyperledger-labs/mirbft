/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/golang/protobuf/proto"

	"github.com/pkg/errors"
)

// epochTarget is like an epoch, but this node need not have agreed
// to transition to this target, and may not have information like the
// epoch configuration
type epochTarget struct {
	changes    map[NodeID]*epochChange
	echos      map[*pb.EpochConfig]map[NodeID]struct{}
	readies    map[*pb.EpochConfig]map[NodeID]struct{}
	suspicions map[NodeID]struct{}

	myNewEpoch     *pb.NewEpoch
	myEpochChange  *pb.EpochChange
	leaderNewEpoch *pb.EpochConfig
	isLeader       bool
}

func (et *epochTarget) constructNewEpoch(newLeaders []uint64, nc *pb.NetworkConfig) *pb.NewEpoch {
	config := constructNewEpochConfig(nc, newLeaders, et.changes)
	if config == nil {
		return nil
	}

	remoteChanges := make([]*pb.NewEpoch_RemoteEpochChange, 0, len(et.changes))
	for nodeID, change := range et.changes {
		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId:      uint64(nodeID),
			EpochChange: change.underlying,
		})
	}

	return &pb.NewEpoch{
		Config:       config,
		EpochChanges: remoteChanges,
	}
}

type epochChangeState int

const (
	prepending epochChangeState = iota // Have sent an epoch-change, but waiting for a quorum
	pending                            // Have a quorum of epoch-change messages, waits on new-epoch
	echoing                            // Have received new-epoch, waits for a quorum of echos
	readying                           // Have received a quorum of echos, waits on qourum of ready
	ready                              // New epoch is ready to begin
	idle                               // No pending change
)

type epochChanger struct {
	state                       epochChangeState
	stateTicks                  uint64
	lastActiveEpoch             *epoch
	pendingEpochTarget          *epochTarget
	highestObservedCorrectEpoch uint64
	networkConfig               *pb.NetworkConfig
	myConfig                    *Config
	targets                     map[uint64]*epochTarget
}

func (ec *epochChanger) tick() *Actions {
	ec.stateTicks++

	switch ec.state {
	case prepending:
		return ec.tickPrepending()
	case pending:
		return ec.tickPending()
	default: // case done:
	}

	return &Actions{}
}

func (ec *epochChanger) repeatEpochChangeBroadcast() *Actions {
	if ec.pendingEpochTarget == nil {
		panic("TODO, handle me? Are we guaranteed to have sent an epoch change, I hope so")
	}

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: ec.pendingEpochTarget.myEpochChange,
				},
			},
		},
	}
}

func (ec *epochChanger) tickPrepending() *Actions {
	if ec.pendingEpochTarget == nil {
		panic("we should never be prepending with a nil pending target")
	}

	if ec.pendingEpochTarget.myNewEpoch == nil {
		if ec.stateTicks%uint64(ec.myConfig.NewEpochTimeoutTicks/2) == 0 {
			return ec.repeatEpochChangeBroadcast()
		}

		return &Actions{}
	}

	ec.stateTicks = 0
	ec.state = pending

	if ec.pendingEpochTarget.isLeader {
		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpoch{
						NewEpoch: ec.pendingEpochTarget.myNewEpoch,
					},
				},
			},
		}
	}

	return &Actions{}
}

func (ec *epochChanger) tickPending() *Actions {
	pendingTicks := ec.stateTicks % uint64(ec.myConfig.NewEpochTimeoutTicks)
	if ec.pendingEpochTarget.isLeader {
		// resend the new-view if others perhaps missed it
		if pendingTicks%2 == 0 {
			return &Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_NewEpoch{
							NewEpoch: ec.pendingEpochTarget.myNewEpoch,
						},
					},
				},
			}
		}
	} else {
		if pendingTicks == 0 {
			return &Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Suspect{
							Suspect: &pb.Suspect{
								Epoch: ec.pendingEpochTarget.myNewEpoch.Config.Number,
							},
						},
					},
				},
			}
		}
		if pendingTicks%2 == 0 {
			return ec.repeatEpochChangeBroadcast()
		}
	}
	return &Actions{}
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
			suspicions: map[NodeID]struct{}{},
			changes:    map[NodeID]*epochChange{},
			echos:      map[*pb.EpochConfig]map[NodeID]struct{}{},
			readies:    map[*pb.EpochConfig]map[NodeID]struct{}{},
			isLeader:   epoch%uint64(len(ec.networkConfig.Nodes)) == ec.myConfig.ID,
		}
		ec.targets[epoch] = target
	}
	return target
}

func (ec *epochChanger) updateHighestObservedCorrectEpoch(epoch uint64) {
	for number := range ec.targets {
		if number < epoch {
			delete(ec.targets, number)
		}
	}

	ec.highestObservedCorrectEpoch = epoch
	// TODO, handle if the active epoch is behind
}

func (ec *epochChanger) applySuspectMsg(source NodeID, epoch uint64) *pb.EpochChange {
	target := ec.target(epoch)
	target.suspicions[source] = struct{}{}

	if len(target.suspicions) >= intersectionQuorum(ec.networkConfig) {
		ec.state = prepending
		newTarget := ec.target(epoch + 1)
		ec.pendingEpochTarget = newTarget
		newTarget.myEpochChange = ec.lastActiveEpoch.constructEpochChange(epoch + 1)
		ec.updateHighestObservedCorrectEpoch(epoch + 1)
		return target.myEpochChange
	}

	if len(target.suspicions) >= someCorrectQuorum(ec.networkConfig) &&
		ec.highestObservedCorrectEpoch < epoch {
		ec.highestObservedCorrectEpoch = epoch
		// TODO, end current epoch
	}

	return nil

}

func (ec *epochChanger) applyEpochChangeMsg(source NodeID, epochChange *pb.EpochChange) *Actions {

	change, err := newEpochChange(epochChange)
	if err != nil {
		// TODO, log
		return &Actions{}
	}

	target := ec.target(epochChange.NewEpoch)
	target.changes[source] = change

	if len(target.changes) > someCorrectQuorum(ec.networkConfig) &&
		ec.highestObservedCorrectEpoch < epochChange.NewEpoch {
		ec.updateHighestObservedCorrectEpoch(epochChange.NewEpoch)
	}

	if len(target.changes) < intersectionQuorum(ec.networkConfig) {
		return &Actions{}
	}

	var newLeaders []uint64
	if ec.lastActiveEpoch == nil {
		newLeaders = ec.networkConfig.Nodes
	} else {
		// XXX actually reduce the leader set
		newLeaders = ec.networkConfig.Nodes
	}

	if ec.state == prepending && target.myNewEpoch == nil {
		target.myNewEpoch = target.constructNewEpoch(newLeaders, ec.networkConfig)
	}

	if target.myNewEpoch == nil {

		return &Actions{}
	}

	if target.isLeader {
		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpoch{
						NewEpoch: target.myNewEpoch,
					},
				},
			},
		}
	}

	return &Actions{}
}

func (ec *epochChanger) applyNewEpochMsg(msg *pb.NewEpoch) *Actions {
	if ec.state > pending {
		// TODO log oddity? maybe ensure not possible via nodemsgs
		return &Actions{}
	}

	epochChanges := map[NodeID]*epochChange{}
	for _, remoteEpochChange := range msg.EpochChanges {
		if _, ok := epochChanges[NodeID(remoteEpochChange.NodeId)]; ok {
			// TODO, malformed, log oddity
			return &Actions{}
		}

		helper, err := newEpochChange(remoteEpochChange.EpochChange)
		if err != nil {
			// TODO, log
			return &Actions{}
		}

		epochChanges[NodeID(remoteEpochChange.NodeId)] = helper
	}

	// XXX need to validate the signatures on the epoch changes

	// TODO, do we need to try to validate the leader set?

	newEpochConfig := constructNewEpochConfig(ec.networkConfig, msg.Config.Leaders, epochChanges)

	if !proto.Equal(newEpochConfig, msg.Config) {
		// TODO byzantine, log oddity
		return &Actions{}
	}

	ec.state = echoing

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_NewEpochEcho{
					NewEpochEcho: &pb.NewEpochEcho{
						Config: msg.Config,
					},
				},
			},
		},
	}
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

	target := ec.target(msg.Config.Number) // TODO, handle nil config

	var msgEchos map[NodeID]struct{}

	for config, echos := range target.echos {
		if proto.Equal(config, msg.Config) {
			msgEchos = echos
			break
		}
	}

	if msgEchos == nil {
		msgEchos = map[NodeID]struct{}{}
		target.echos[msg.Config] = msgEchos
	}

	msgEchos[source] = struct{}{}

	if target != ec.pendingEpochTarget && len(msgEchos) >= someCorrectQuorum(ec.networkConfig) {
		ec.updateHighestObservedCorrectEpoch(msg.Config.Number)
	}

	if len(msgEchos) < intersectionQuorum(ec.networkConfig) {
		return &Actions{}
	}

	if ec.state > echoing {
		return &Actions{}
	}

	ec.state = readying

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_NewEpochReady{
					NewEpochReady: &pb.NewEpochReady{
						Config: msg.Config,
					},
				},
			},
		},
	}
}

func (ec *epochChanger) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	if ec.state > readying {
		// We've already accepted the epoch config, move along
		return &Actions{}
	}

	target := ec.target(msg.Config.Number) // TODO, handle nil config

	var msgReadies map[NodeID]struct{}

	for config, readies := range target.readies {
		if proto.Equal(config, msg.Config) {
			msgReadies = readies
			break
		}
	}

	if msgReadies == nil {
		msgReadies = map[NodeID]struct{}{}
		target.readies[msg.Config] = msgReadies
	}

	msgReadies[source] = struct{}{}

	if len(msgReadies) < someCorrectQuorum(ec.networkConfig) {
		return &Actions{}
	}

	if ec.state < readying {
		ec.state = readying

		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpochReady{
						NewEpochReady: &pb.NewEpochReady{
							Config: msg.Config,
						},
					},
				},
			},
		}
	}

	if len(msgReadies) >= intersectionQuorum(ec.networkConfig) {
		ec.state = ready
		target.leaderNewEpoch = msg.Config
	}

	return &Actions{}
}

// TODO, these nested maps are a little tricky to read, might be better to make proper types

type epochChange struct {
	underlying   *pb.EpochChange
	lowWatermark uint64
	pSet         map[uint64]*pb.EpochChange_SetEntry
	qSet         map[uint64]map[uint64][]byte
}

func newEpochChange(underlying *pb.EpochChange) (*epochChange, error) {
	if len(underlying.Checkpoints) == 0 {
		return nil, errors.Errorf("epoch change did not contain any checkpoints")
	}

	lowWatermark := underlying.Checkpoints[0].SeqNo
	checkpoints := map[uint64]*pb.Checkpoint{}

	for _, checkpoint := range underlying.Checkpoints {
		if lowWatermark > checkpoint.SeqNo {
			lowWatermark = checkpoint.SeqNo
		}

		if _, ok := checkpoints[checkpoint.SeqNo]; ok {
			return nil, errors.Errorf("epoch change contained duplicated seqnos for %d", checkpoint.SeqNo)
		}
	}

	// TODO, check pSet and qSet for 'too advanced' views.

	// TODO, check pSet and qSet for entries within log window relative to low watermark

	pSet := map[uint64]*pb.EpochChange_SetEntry{}
	for _, entry := range underlying.PSet {
		if _, ok := pSet[entry.SeqNo]; ok {
			return nil, errors.Errorf("epoch change pSet contained duplicate entries for seqno=%d", entry.SeqNo)
		}

		pSet[entry.SeqNo] = entry
	}

	qSet := map[uint64]map[uint64][]byte{}
	for _, entry := range underlying.QSet {
		views, ok := qSet[entry.SeqNo]
		if !ok {
			views = map[uint64][]byte{}
			qSet[entry.SeqNo] = views
		}

		if _, ok := views[entry.Epoch]; ok {
			return nil, errors.Errorf("epoch change qSet contained duplicate entries for seqno=%d epoch=%d", entry.SeqNo, entry.Epoch)
		}

		views[entry.Epoch] = entry.Digest
	}

	return &epochChange{
		underlying:   underlying,
		lowWatermark: lowWatermark,
		pSet:         pSet,
		qSet:         qSet,
	}, nil
}

func constructNewEpochConfig(config *pb.NetworkConfig, newLeaders []uint64, epochChanges map[NodeID]*epochChange) *pb.EpochConfig {
	type checkpointKey struct {
		SeqNo uint64
		Value string
	}

	checkpoints := map[checkpointKey][]NodeID{}

	var newEpochNumber uint64 // TODO this is super-hacky

	for nodeID, epochChange := range epochChanges {
		newEpochNumber = epochChange.underlying.NewEpoch
		for _, checkpoint := range epochChange.underlying.Checkpoints {

			key := checkpointKey{
				SeqNo: checkpoint.SeqNo,
				Value: string(checkpoint.Value),
			}

			checkpoints[key] = append(checkpoints[key], nodeID)
		}
	}

	var maxCheckpoint *checkpointKey

	for key, supporters := range checkpoints {
		if len(supporters) < someCorrectQuorum(config) {
			continue
		}

		nodesWithLowerWatermark := 0
		for _, epochChange := range epochChanges {
			if epochChange.lowWatermark <= key.SeqNo {
				nodesWithLowerWatermark++
			}
		}

		if nodesWithLowerWatermark < intersectionQuorum(config) {
			continue
		}

		if maxCheckpoint == nil {
			maxCheckpoint = &key
			continue
		}

		if maxCheckpoint.SeqNo > key.SeqNo {
			continue
		}

		if maxCheckpoint.SeqNo == key.SeqNo {
			panic("two correct quorums have different checkpoints for same seqno")
		}

		maxCheckpoint = &key
	}

	if maxCheckpoint == nil {
		return nil
	}

	newEpochConfig := &pb.EpochConfig{
		Number:  newEpochNumber,
		Leaders: newLeaders,
		StartingCheckpoint: &pb.Checkpoint{
			SeqNo: maxCheckpoint.SeqNo,
			Value: []byte(maxCheckpoint.Value),
		},
		FinalPreprepares: make([][]byte, 2*config.CheckpointInterval),
	}

	anyNonNil := false

	for seqNoOffset := range newEpochConfig.FinalPreprepares {
		seqNo := uint64(seqNoOffset) + maxCheckpoint.SeqNo + 1

		for _, nodeID := range config.Nodes {
			nodeID := NodeID(nodeID)
			// Note, it looks like we're re-implementing `range epochChanges` here,
			// and we are, but doing so in a deterministic order.

			epochChange, ok := epochChanges[nodeID]
			if !ok {
				continue
			}

			entry, ok := epochChange.pSet[seqNo]
			if !ok {
				continue
			}

			a1Count := 0
			for _, iEpochChange := range epochChanges {
				if iEpochChange.lowWatermark >= seqNo {
					continue
				}

				iEntry, ok := iEpochChange.pSet[seqNo]
				if !ok || iEntry.Epoch < entry.Epoch {
					a1Count++
					continue
				}

				if iEntry.Epoch > entry.Epoch {
					continue
				}

				// Thus, iEntry.Epoch == entry.Epoch

				if bytes.Equal(entry.Digest, iEntry.Digest) {
					a1Count++
				}
			}

			if a1Count < intersectionQuorum(config) {
				continue
			}

			a2Count := 0
			for _, iEpochChange := range epochChanges {
				epochEntries, ok := iEpochChange.qSet[seqNo]
				if !ok {
					continue
				}

				for epoch, digest := range epochEntries {
					if epoch < entry.Epoch {
						continue
					}

					if !bytes.Equal(entry.Digest, digest) {
						continue
					}

					a2Count++
					break
				}
			}

			if a2Count < someCorrectQuorum(config) {
				continue
			}

			newEpochConfig.FinalPreprepares[seqNoOffset] = entry.Digest
			break
		}

		if newEpochConfig.FinalPreprepares[seqNoOffset] != nil {
			// Some entry from the pSet was selected for this bucketSeq
			anyNonNil = true
			continue
		}

		bCount := 0
		for _, epochChange := range epochChanges {
			if epochChange.lowWatermark >= seqNo {
				continue
			}

			if _, ok := epochChange.pSet[seqNo]; !ok {
				bCount++
			}
		}

		if bCount < intersectionQuorum(config) {
			// We could not satisfy condition A, or B, we need to wait
			return nil
		}
	}

	if !anyNonNil {
		newEpochConfig.FinalPreprepares = nil
	}

	return newEpochConfig
}

func (et *epochTarget) status() *EpochTargetStatus {
	status := &EpochTargetStatus{
		EpochChanges: make([]uint64, 0, len(et.changes)),
		Echos:        make([]uint64, 0, len(et.echos)),
		Readies:      make([]uint64, 0, len(et.readies)),
		Suspicions:   make([]uint64, 0, len(et.suspicions)),
	}

	for node := range et.changes {
		status.EpochChanges = append(status.EpochChanges, uint64(node))
	}
	sort.Slice(status.EpochChanges, func(i, j int) bool {
		return status.EpochChanges[i] < status.EpochChanges[j]
	})

	for _, echoMsgs := range et.echos {
		for node := range echoMsgs {
			status.Echos = append(status.Echos, uint64(node))
		}
	}

	sort.Slice(status.Echos, func(i, j int) bool {
		return status.Echos[i] < status.Echos[j]
	})

	for _, readyMsgs := range et.readies {
		for node := range readyMsgs {
			status.Readies = append(status.Readies, uint64(node))
		}
	}
	sort.Slice(status.Readies, func(i, j int) bool {
		return status.Readies[i] < status.Readies[j]
	})

	for node := range et.suspicions {
		status.Suspicions = append(status.Suspicions, uint64(node))
	}
	sort.Slice(status.Suspicions, func(i, j int) bool {
		return status.Suspicions[i] < status.Suspicions[j]
	})

	return status
}

func (ec *epochChanger) status() *EpochChangerStatus {

	lastActiveEpoch := uint64(0)
	if ec.lastActiveEpoch != nil {
		lastActiveEpoch = ec.lastActiveEpoch.config.number
	}

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
		State:           ec.state,
		LastActiveEpoch: lastActiveEpoch,
		EpochTargets:    targets,
	}
}
