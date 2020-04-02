/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"bytes"
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/golang/protobuf/proto"

	"github.com/pkg/errors"
)

type epochChangeState int

const (
	prepending = iota // Have sent an epoch-change, but waiting for a quorum
	pending           // Have a quorum of epoch-change messages, waits on new-epoch
	echoing           // Have received new-epoch, waits for a quorum of echos
	readying          // Have received a quorum of echos, waits on qourum of ready
	ready             // New epoch is ready to begin
	idle              // No pending change
	done              // We have sent an epoch change, ending this epoch for us
)

// epochTarget is like an epoch, but this node need not have agreed
// to transition to this target, and may not have information like the
// epoch configuration
type epochTarget struct {
	state         epochChangeState
	stateTicks    uint64
	number        uint64
	changes       map[NodeID]*epochChange
	weakChanges   map[NodeID]*epochChange
	strongChanges map[NodeID]*epochChange
	echos         map[*pb.EpochConfig]map[NodeID]struct{}
	readies       map[*pb.EpochConfig]map[NodeID]struct{}
	suspicions    map[NodeID]struct{}

	myNewEpoch      *pb.NewEpoch // The NewEpoch msg we computed from the epoch changes we know of
	myEpochChange   *epochChange
	leaderNewEpoch  *pb.NewEpoch    // The NewEpoch msg we received directly from the leader
	networkNewEpoch *pb.EpochConfig // The NewEpoch msg as received via the bracha broadcast
	isLeader        bool

	networkConfig *pb.NetworkConfig
	myConfig      *Config
}

func (et *epochTarget) constructNewEpoch(newLeaders []uint64, nc *pb.NetworkConfig) *pb.NewEpoch {
	filteredStrongChanges := map[NodeID]*epochChange{}
	for nodeID, change := range et.strongChanges {
		if change.underlying == nil {
			continue
		}
		filteredStrongChanges[nodeID] = change
	}

	if len(filteredStrongChanges) < intersectionQuorum(nc) {
		return nil
	}

	config := constructNewEpochConfig(nc, newLeaders, filteredStrongChanges)
	if config == nil {
		return nil
	}

	remoteChanges := make([]*pb.NewEpoch_RemoteEpochChange, 0, len(et.changes))
	for nodeID, change := range et.strongChanges {
		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId: uint64(nodeID),
			Digest: change.digest,
		})
	}

	return &pb.NewEpoch{
		Config:       config,
		EpochChanges: remoteChanges,
	}
}

func (et *epochTarget) updateCorrectChanges() bool {
	modified := false
	for nodeID, change := range et.changes {
		if change.weakCert {
			if _, ok := et.weakChanges[nodeID]; !ok {
				modified = true
			}
			et.weakChanges[nodeID] = change
		}

		if change.strongCert {
			if _, ok := et.strongChanges[nodeID]; !ok {
				modified = true
			}
			et.strongChanges[nodeID] = change
		}
	}

	return modified
}

func (et *epochTarget) updateNewEpochState() *Actions {
	if et.leaderNewEpoch == nil {
		return &Actions{}
	}

	epochChanges := map[NodeID]*epochChange{}
	for _, remoteEpochChange := range et.leaderNewEpoch.EpochChanges {
		if _, ok := epochChanges[NodeID(remoteEpochChange.NodeId)]; ok {
			// TODO, malformed, log oddity
			return &Actions{}
		}

		change, ok := et.weakChanges[NodeID(remoteEpochChange.NodeId)]
		if !ok || change.underlying == nil {
			panic("we don't handle this yet")
			return &Actions{}
		}

		epochChanges[NodeID(remoteEpochChange.NodeId)] = change
	}

	// TODO, do we need to try to validate the leader set?

	newEpochConfig := constructNewEpochConfig(et.networkConfig, et.leaderNewEpoch.Config.Leaders, epochChanges)

	if !proto.Equal(newEpochConfig, et.leaderNewEpoch.Config) {
		// TODO byzantine, log oddity
		return &Actions{}
	}

	et.state = echoing

	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_NewEpochEcho{
					NewEpochEcho: &pb.NewEpochEcho{
						Config: et.leaderNewEpoch.Config,
					},
				},
			},
		},
	}
}

type epochChanger struct {
	stateTicks                  uint64
	lastActiveEpoch             *epoch
	pendingEpochTarget          *epochTarget
	highestObservedCorrectEpoch uint64
	networkConfig               *pb.NetworkConfig
	myConfig                    *Config
	targets                     map[uint64]*epochTarget
}

func (ec *epochChanger) tick() *Actions {
	return ec.pendingEpochTarget.tick()
}

func (et *epochTarget) tick() *Actions {
	switch et.state {
	case prepending:
		return et.tickPrepending()
	case pending:
		return et.tickPending()
	default: // case done:
	}

	return &Actions{}
}

func (et *epochTarget) repeatEpochChangeBroadcast() *Actions {
	return &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChange{
					EpochChange: et.myEpochChange.underlying,
				},
			},
		},
	}
}

func (et *epochTarget) tickPrepending() *Actions {
	if et.myNewEpoch == nil {
		if et.stateTicks%uint64(et.myConfig.NewEpochTimeoutTicks/2) == 0 {
			return et.repeatEpochChangeBroadcast()
		}

		return &Actions{}
	}

	et.stateTicks = 0
	et.state = pending

	if et.isLeader {
		return &Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_NewEpoch{
						NewEpoch: et.myNewEpoch,
					},
				},
			},
		}
	}

	return &Actions{}
}

func (et *epochTarget) tickPending() *Actions {
	pendingTicks := et.stateTicks % uint64(et.myConfig.NewEpochTimeoutTicks)
	if et.isLeader {
		// resend the new-view if others perhaps missed it
		if pendingTicks%2 == 0 {
			return &Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_NewEpoch{
							NewEpoch: et.myNewEpoch,
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
								Epoch: et.myNewEpoch.Config.Number,
							},
						},
					},
				},
			}
		}
		if pendingTicks%2 == 0 {
			return et.repeatEpochChangeBroadcast()
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
			number:        epoch,
			suspicions:    map[NodeID]struct{}{},
			changes:       map[NodeID]*epochChange{},
			weakChanges:   map[NodeID]*epochChange{},
			strongChanges: map[NodeID]*epochChange{},
			echos:         map[*pb.EpochConfig]map[NodeID]struct{}{},
			readies:       map[*pb.EpochConfig]map[NodeID]struct{}{},
			isLeader:      epoch%uint64(len(ec.networkConfig.Nodes)) == ec.myConfig.ID,
			myEpochChange: &epochChange{
				networkConfig: ec.networkConfig,
			},
			networkConfig: ec.networkConfig,
			myConfig:      ec.myConfig,
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
}

func (ec *epochChanger) applySuspectMsg(source NodeID, epoch uint64) *pb.EpochChange {
	target := ec.target(epoch)
	target.applySuspectMsg(source)
	if target.state < done {
		return nil
	}

	epochChange := ec.lastActiveEpoch.constructEpochChange(epoch + 1)

	newTarget := ec.target(epoch + 1)
	ec.pendingEpochTarget = newTarget
	err := newTarget.myEpochChange.setMsg(epochChange)
	if err != nil {
		panic(errors.WithMessage(err, "could not parse the epoch change I generated"))
	}

	return epochChange
}

func (et *epochTarget) applySuspectMsg(source NodeID) {
	et.suspicions[source] = struct{}{}

	if len(et.suspicions) >= intersectionQuorum(et.networkConfig) {
		et.state = done
	}
}

func epochChangeHashData(epochChange *pb.EpochChange) [][]byte {
	// [new_epoch, checkpoints, pSet, qSet]
	hashData := make([][]byte, 1+len(epochChange.Checkpoints)*2+len(epochChange.PSet)*3+len(epochChange.QSet)*3)
	hashData[0] = uint64ToBytes(epochChange.NewEpoch)

	cpOffset := 1
	for i, cp := range epochChange.Checkpoints {
		hashData[cpOffset+2*i] = uint64ToBytes(cp.SeqNo)
		hashData[cpOffset+2*i+1] = cp.Value
	}

	pEntryOffset := cpOffset + len(epochChange.Checkpoints)*2
	for i, pEntry := range epochChange.PSet {
		hashData[pEntryOffset+3*i] = uint64ToBytes(pEntry.Epoch)
		hashData[pEntryOffset+3*i+1] = uint64ToBytes(pEntry.SeqNo)
		hashData[pEntryOffset+3*i+2] = pEntry.Digest
	}

	qEntryOffset := pEntryOffset + len(epochChange.PSet)*3
	for i, qEntry := range epochChange.QSet {
		hashData[qEntryOffset+3*i] = uint64ToBytes(qEntry.Epoch)
		hashData[qEntryOffset+3*i+1] = uint64ToBytes(qEntry.SeqNo)
		hashData[qEntryOffset+3*i+2] = qEntry.Digest
	}

	if qEntryOffset+len(epochChange.QSet)*3 != len(hashData) {
		panic("TODO, remove me, but this is bad")
	}

	return hashData
}

func (ec *epochChanger) applyEpochChangeMsg(source NodeID, msg *pb.EpochChange) *Actions {
	change := &epochChange{
		networkConfig: ec.networkConfig,
	}
	err := change.setMsg(msg)
	if err != nil {
		// TODO, log
		return &Actions{}
	}

	target := ec.target(msg.NewEpoch)
	// TODO, make sure nodemsgs prevents us from receiving an epoch change twice
	target.changes[source] = change

	hashRequest := &HashRequest{
		Data: epochChangeHashData(msg),
		EpochChange: &EpochChange{
			Source:      uint64(source),
			EpochChange: msg,
		},
	}

	return &Actions{
		Hash: []*HashRequest{hashRequest},
	}
}

func (ec *epochChanger) applyEpochChangeDigest(epochChange *EpochChange, digest []byte) *Actions {
	// TODO, fix all this stuttering and repitition
	target := ec.target(epochChange.EpochChange.NewEpoch)
	pChange := target.changes[NodeID(epochChange.Source)]
	pChange.digest = digest
	actions := &Actions{
		Broadcast: []*pb.Msg{
			{
				Type: &pb.Msg_EpochChangeAck{
					EpochChangeAck: &pb.EpochChangeAck{
						NewEpoch: target.number,
						Sender:   epochChange.Source,
						Digest:   digest,
					},
				},
			},
		},
	}

	modified := pChange.updateAcks()
	if !modified {
		return actions
	}

	modified = target.updateCorrectChanges()
	if !modified {
		return actions
	}

	actions.Append(ec.checkEpochQuorum(target))
	return actions
}

func (ec *epochChanger) applyEpochChangeAckMsg(source NodeID, ack *pb.EpochChangeAck) *Actions {
	target := ec.target(ack.NewEpoch)
	change, ok := target.changes[NodeID(ack.Sender)]
	if !ok {
		change = &epochChange{
			networkConfig: ec.networkConfig,
		}
		target.changes[source] = change
	}

	if change.acks == nil {
		change.acks = map[NodeID][]byte{}
	}
	change.acks[source] = ack.Digest
	modified := change.updateAcks()
	if !modified {
		return &Actions{}
	}

	modified = target.updateCorrectChanges()
	if !modified {
		return &Actions{}
	}

	return ec.checkEpochQuorum(target)
}

func (ec *epochChanger) checkEpochQuorum(target *epochTarget) *Actions {
	if len(target.strongChanges) < intersectionQuorum(ec.networkConfig) || target.myEpochChange == nil {
		return &Actions{}
	}

	var newLeaders []uint64
	if ec.lastActiveEpoch == nil {
		newLeaders = ec.networkConfig.Nodes
	} else {
		oldLeaders := ec.lastActiveEpoch.config.leaders
		if len(oldLeaders) == 1 {
			newLeaders = []uint64{ec.myConfig.ID}
		} else {
			var badNode uint64
			if ec.lastActiveEpoch.config.number+1 == target.myEpochChange.underlying.NewEpoch {
				var lowestEntry uint64
				for i := target.myEpochChange.lowWatermark + 1; i < target.myEpochChange.lowWatermark+uint64(ec.networkConfig.CheckpointInterval)*2; i++ {
					if _, ok := target.myEpochChange.pSet[i]; !ok {
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
				badNode = (target.myEpochChange.underlying.NewEpoch - 1) % uint64(len(ec.networkConfig.Nodes))
			}

			newLeaders = make([]uint64, 0, len(oldLeaders)-1)
			for _, oldLeader := range oldLeaders {
				if oldLeader == badNode {
					continue
				}
				newLeaders = append(newLeaders, oldLeader)
			}
		}
	}

	if target.state == prepending && target.myNewEpoch == nil {
		// TODO, all of the above processing to pick the leaderset should not occur if we've
		// already created a new epoch

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
	target := ec.target(msg.Config.Number)

	if target.state > pending {
		// TODO log oddity? maybe ensure not possible via nodemsgs
		return &Actions{}
	}

	target.leaderNewEpoch = msg
	return target.updateNewEpochState()
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
	return target.applyNewEpochEchoMsg(source, msg)
}

func (et *epochTarget) applyNewEpochEchoMsg(source NodeID, msg *pb.NewEpochEcho) *Actions {
	var msgEchos map[NodeID]struct{}

	for config, echos := range et.echos {
		if proto.Equal(config, msg.Config) {
			msgEchos = echos
			break
		}
	}

	if msgEchos == nil {
		msgEchos = map[NodeID]struct{}{}
		et.echos[msg.Config] = msgEchos
	}

	msgEchos[source] = struct{}{}

	if len(msgEchos) < intersectionQuorum(et.networkConfig) {
		return &Actions{}
	}

	if et.state > echoing {
		return &Actions{}
	}

	et.state = readying

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
	target := ec.target(msg.Config.Number)
	return target.applyNewEpochReadyMsg(source, msg)
}

func (et *epochTarget) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	if et.state > readying {
		// We've already accepted the epoch config, move along
		return &Actions{}
	}

	var msgReadies map[NodeID]struct{}

	for config, readies := range et.readies {
		if proto.Equal(config, msg.Config) {
			msgReadies = readies
			break
		}
	}

	if msgReadies == nil {
		msgReadies = map[NodeID]struct{}{}
		et.readies[msg.Config] = msgReadies
	}

	msgReadies[source] = struct{}{}

	if len(msgReadies) < someCorrectQuorum(et.networkConfig) {
		return &Actions{}
	}

	if et.state < readying {
		et.state = readying

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

	if len(msgReadies) >= intersectionQuorum(et.networkConfig) {
		et.state = ready
		et.networkNewEpoch = msg.Config
	}

	return &Actions{}
}

// TODO, these nested maps are a little tricky to read, might be better to make proper types

type epochChange struct {
	// set at creation
	networkConfig *pb.NetworkConfig

	// set via applyEpochChangeMsg
	underlying *pb.EpochChange
	digest     []byte
	pSet       map[uint64]*pb.EpochChange_SetEntry
	qSet       map[uint64]map[uint64][]byte

	// set via applyEpochChangeAckMsg
	acks map[NodeID][]byte

	// updated via applyEpochChangeMsg and applyEpochChangeAckMsg
	weakCert     bool
	strongCert   bool
	lowWatermark uint64
}

// updateAcks checks to see if there are enough acks to form a strong
// or weak cert.  It returns whether the result changed since last check.
func (ec *epochChange) updateAcks() bool {
	if ec.digest == nil || ec.strongCert {
		return false
	}

	agreements := 0
	for _, ack := range ec.acks {
		if !bytes.Equal(ack, ec.digest) {
			continue
		}
		agreements++
	}

	if agreements >= intersectionQuorum(ec.networkConfig) {
		ec.strongCert = true
		ec.weakCert = true
		return true
	}

	if ec.weakCert {
		return false
	}

	if agreements >= someCorrectQuorum(ec.networkConfig) {
		ec.weakCert = true
	}

	return ec.weakCert
}

func (ec *epochChange) setMsg(underlying *pb.EpochChange) error {
	if len(underlying.Checkpoints) == 0 {
		return errors.Errorf("epoch change did not contain any checkpoints")
	}

	lowWatermark := underlying.Checkpoints[0].SeqNo
	checkpoints := map[uint64]*pb.Checkpoint{}

	for _, checkpoint := range underlying.Checkpoints {
		if lowWatermark > checkpoint.SeqNo {
			lowWatermark = checkpoint.SeqNo
		}

		if _, ok := checkpoints[checkpoint.SeqNo]; ok {
			return errors.Errorf("epoch change contained duplicated seqnos for %d", checkpoint.SeqNo)
		}
	}

	// TODO, check pSet and qSet for 'too advanced' views.

	// TODO, check pSet and qSet for entries within log window relative to low watermark

	pSet := map[uint64]*pb.EpochChange_SetEntry{}
	for _, entry := range underlying.PSet {
		if _, ok := pSet[entry.SeqNo]; ok {
			return errors.Errorf("epoch change pSet contained duplicate entries for seqno=%d", entry.SeqNo)
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
			return errors.Errorf("epoch change qSet contained duplicate entries for seqno=%d epoch=%d", entry.SeqNo, entry.Epoch)
		}

		views[entry.Epoch] = entry.Digest
	}

	ec.underlying = underlying
	ec.lowWatermark = lowWatermark
	ec.pSet = pSet
	ec.qSet = qSet
	return nil
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
		key := key // shadow for when we take the pointer
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
			panic(fmt.Sprintf("two correct quorums have different checkpoints for same seqno %d -- %x != %x", key.SeqNo, []byte(maxCheckpoint.Value), []byte(key.Value)))
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

func (ec *epochChange) status(source uint64) *EpochChangeStatus {
	status := &EpochChangeStatus{
		Source: source,
		Acks:   make([]uint64, 0, len(ec.acks)),
		Digest: ec.digest,
	}

	for node := range ec.acks {
		status.Acks = append(status.Acks, uint64(node))
	}
	sort.Slice(status.Acks, func(i, j int) bool {
		return status.Acks[i] < status.Acks[j]
	})

	return status
}

func (et *epochTarget) status() *EpochTargetStatus {
	status := &EpochTargetStatus{
		EpochChanges: make([]*EpochChangeStatus, 0, len(et.changes)),
		Echos:        make([]uint64, 0, len(et.echos)),
		Readies:      make([]uint64, 0, len(et.readies)),
		Suspicions:   make([]uint64, 0, len(et.suspicions)),
	}

	for node, change := range et.changes {
		status.EpochChanges = append(status.EpochChanges, change.status(uint64(node)))
	}
	sort.Slice(status.EpochChanges, func(i, j int) bool {
		return status.EpochChanges[i].Source < status.EpochChanges[j].Source
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
		State:           ec.pendingEpochTarget.state,
		LastActiveEpoch: lastActiveEpoch,
		EpochTargets:    targets,
	}
}
