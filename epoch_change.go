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

type epochTargetState int

const (
	prepending = iota // Have sent an epoch-change, but waiting for a quorum
	pending           // Have a quorum of epoch-change messages, waits on new-epoch
	verifying         // Have a new view message but it references epoch changes we cannot yet verify
	fetching          // Have received and verified a new epoch messages, and are waiting to get state
	echoing           // Have received and validated a new-epoch, waiting for a quorum of echos
	readying          // Have received a quorum of echos, waiting a on qourum of readies
	ready             // New epoch is ready to begin
	inProgress        // No pending change
	done              // We have sent an epoch change, ending this epoch for us
)

// epochTarget is like an epoch, but this node need not have agreed
// to transition to this target, and may not have information like the
// epoch configuration
type epochTarget struct {
	state         epochTargetState
	stateTicks    uint64
	number        uint64
	changes       map[NodeID]*epochChange
	strongChanges map[NodeID]*parsedEpochChange
	echos         map[*pb.NewEpochConfig]map[NodeID]struct{}
	readies       map[*pb.NewEpochConfig]map[NodeID]struct{}
	suspicions    map[NodeID]struct{}

	persisted       *persisted
	myNewEpoch      *pb.NewEpoch // The NewEpoch msg we computed from the epoch changes we know of
	myEpochChange   *parsedEpochChange
	myLeaderChoice  []uint64           // Set along with myEpochChange
	leaderNewEpoch  *pb.NewEpoch       // The NewEpoch msg we received directly from the leader
	networkNewEpoch *pb.NewEpochConfig // The NewEpoch msg as received via the bracha broadcast
	isLeader        bool

	networkConfig *pb.NetworkState_Config
	myConfig      *pb.StateEvent_InitialParameters
	batchTracker  *batchTracker
	clientWindows *clientWindows
}

func (et *epochTarget) constructNewEpoch(newLeaders []uint64, nc *pb.NetworkState_Config) *pb.NewEpoch {
	filteredStrongChanges := map[NodeID]*parsedEpochChange{}
	for nodeID, change := range et.strongChanges {
		if change.underlying == nil {
			continue
		}
		filteredStrongChanges[nodeID] = change
	}

	if len(filteredStrongChanges) < intersectionQuorum(nc) {
		return nil
	}

	newConfig := constructNewEpochConfig(nc, newLeaders, filteredStrongChanges)
	if newConfig == nil {
		return nil
	}

	remoteChanges := make([]*pb.NewEpoch_RemoteEpochChange, 0, len(et.changes))
	for nodeID := range et.strongChanges {
		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId: uint64(nodeID),
			Digest: et.changes[nodeID].strongCert,
		})
	}

	return &pb.NewEpoch{
		NewConfig:    newConfig,
		EpochChanges: remoteChanges,
	}
}

func (et *epochTarget) verifyNewEpochState() *Actions {
	epochChanges := map[NodeID]*parsedEpochChange{}
	for _, remoteEpochChange := range et.leaderNewEpoch.EpochChanges {
		if _, ok := epochChanges[NodeID(remoteEpochChange.NodeId)]; ok {
			// TODO, references multiple epoch changes from the same node, malformed, log oddity
			return &Actions{}
		}

		change, ok := et.changes[NodeID(remoteEpochChange.NodeId)]
		if !ok {
			// Either the primary is lying, or, we simply don't have enough information yet.
			return &Actions{}
		}

		parsedChange, ok := change.parsedByDigest[string(remoteEpochChange.Digest)]
		if !ok || len(parsedChange.acks) < someCorrectQuorum(et.networkConfig) {
			return &Actions{}
		}

		epochChanges[NodeID(remoteEpochChange.NodeId)] = parsedChange
	}

	// TODO, validate the planned expiration makes sense

	// TODO, do we need to try to validate the leader set?

	newEpochConfig := constructNewEpochConfig(et.networkConfig, et.leaderNewEpoch.NewConfig.Config.Leaders, epochChanges)

	if !proto.Equal(newEpochConfig, et.leaderNewEpoch.NewConfig) {
		// TODO byzantine, log oddity
		return &Actions{}
	}

	et.state = fetching

	return et.advanceState()
}

func (et *epochTarget) fetchNewEpochState() *Actions {
	actions := &Actions{}

	newEpochConfig := et.leaderNewEpoch.NewConfig

	if newEpochConfig.StartingCheckpoint.SeqNo > et.persisted.lastCommitted {
		panic("we need checkpoint state transfer to handle this case")
	}

	fetchPending := false

	for i, digest := range newEpochConfig.FinalPreprepares {
		if len(digest) == 0 {
			continue
		}

		seqNo := uint64(i) + newEpochConfig.StartingCheckpoint.SeqNo + 1

		var sources []uint64
		for _, remoteEpochChange := range et.leaderNewEpoch.EpochChanges {
			// Previous state verified these exist
			change := et.changes[NodeID(remoteEpochChange.NodeId)]
			parsedChange := change.parsedByDigest[string(remoteEpochChange.Digest)]
			for _, qEntryDigest := range parsedChange.qSet[seqNo] {
				if bytes.Equal(qEntryDigest, digest) {
					sources = append(sources, remoteEpochChange.NodeId)
					break
				}
			}
		}

		if len(sources) < someCorrectQuorum(et.networkConfig) {
			panic(fmt.Sprintf("dev only, should never be true, we only found %d sources for seqno=%d with digest=%x", len(sources), seqNo, digest))
		}

		batch, ok := et.batchTracker.getBatch(digest)
		if !ok {
			// TODO, perhaps only ask those who have it?
			actions.concat(et.batchTracker.fetchBatch(seqNo, digest, et.networkConfig.Nodes))
			fetchPending = true
			continue
		}

		batch.observedSequences[seqNo] = struct{}{}

		for _, requestAck := range batch.requestAcks {
			cw, ok := et.clientWindows.clientWindow(requestAck.ClientId)
			if !ok {
				panic("unknown client, we need state transfer to handle this")
			}

			for _, nodeID := range sources {
				cw.ack(NodeID(nodeID), requestAck.ReqNo, requestAck.Digest)
			}

			if cw.request(requestAck.ReqNo).digests[string(requestAck.Digest)].data != nil {
				continue
			}

			// We are missing this request data and must fetch before proceeding
			fetchPending = true
			// TODO, perhaps only ask those who have it?
			actions.send(et.networkConfig.Nodes, &pb.Msg{
				Type: &pb.Msg_FetchRequest{
					FetchRequest: requestAck,
				},
			})
		}
	}

	if fetchPending {
		return actions
	}

	et.state = echoing

	for i, digest := range newEpochConfig.FinalPreprepares {
		seqNo := uint64(i) + newEpochConfig.StartingCheckpoint.SeqNo + 1
		if len(digest) == 0 {
			actions.concat(et.persisted.addQEntry(&pb.QEntry{
				SeqNo: seqNo,
			}))
			continue
		}

		batch, _ := et.batchTracker.getBatch(digest)
		requests := make([]*pb.ForwardRequest, len(batch.requestAcks))

		for j, requestAck := range batch.requestAcks {
			cw, _ := et.clientWindows.clientWindow(requestAck.ClientId)
			r := cw.request(requestAck.ReqNo).digests[string(requestAck.Digest)]
			requests[j] = &pb.ForwardRequest{
				Request: r.data,
				Digest:  requestAck.Digest,
			}
		}

		qEntry := &pb.QEntry{
			SeqNo:    seqNo,
			Digest:   digest,
			Requests: requests,
		}

		actions.concat(et.persisted.addQEntry(qEntry))
	}

	return actions.concat(
		et.persisted.addNewEpochEcho(et.leaderNewEpoch.NewConfig),
	).send(
		et.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_NewEpochEcho{
				NewEpochEcho: &pb.NewEpochEcho{
					NewConfig: et.leaderNewEpoch.NewConfig,
				},
			},
		},
	)
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
	return (&Actions{}).send(
		et.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_EpochChange{
				EpochChange: et.myEpochChange.underlying,
			},
		},
	)
}

func (et *epochTarget) tickPrepending() *Actions {
	if et.myNewEpoch == nil {
		if et.stateTicks%uint64(et.myConfig.NewEpochTimeoutTicks/2) == 0 {
			return et.repeatEpochChangeBroadcast()
		}

		return &Actions{}
	}

	et.stateTicks = 0

	if et.isLeader {
		return (&Actions{}).send(
			et.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_NewEpoch{
					NewEpoch: et.myNewEpoch,
				},
			},
		)
	}

	return &Actions{}
}

func (et *epochTarget) tickPending() *Actions {
	pendingTicks := et.stateTicks % uint64(et.myConfig.NewEpochTimeoutTicks)
	if et.isLeader {
		// resend the new-view if others perhaps missed it
		if pendingTicks%2 == 0 {
			return (&Actions{}).send(
				et.networkConfig.Nodes,
				&pb.Msg{
					Type: &pb.Msg_NewEpoch{
						NewEpoch: et.myNewEpoch,
					},
				},
			)
		}
	} else {
		if pendingTicks == 0 {
			suspect := &pb.Suspect{
				Epoch: et.myNewEpoch.NewConfig.Config.Number,
			}
			return (&Actions{}).send(
				et.networkConfig.Nodes,
				&pb.Msg{
					Type: &pb.Msg_Suspect{
						Suspect: suspect,
					},
				},
			).concat(et.persisted.addSuspect(suspect))
		}
		if pendingTicks%2 == 0 {
			return et.repeatEpochChangeBroadcast()
		}
	}
	return &Actions{}
}

func (et *epochTarget) applyEpochChangeAckMsg(source NodeID, origin NodeID, msg *pb.EpochChange) *Actions {
	// TODO, make sure nodemsgs prevents us from receiving an epoch change twice
	hashRequest := &HashRequest{
		Data: epochChangeHashData(msg),
		Origin: &pb.HashResult{
			Type: &pb.HashResult_EpochChange_{
				EpochChange: &pb.HashResult_EpochChange{
					Source:      uint64(source),
					Origin:      uint64(origin),
					EpochChange: msg,
				},
			},
		},
	}

	return &Actions{
		Hash: []*HashRequest{hashRequest},
	}
}

func (et *epochTarget) applyEpochChangeDigest(processedChange *pb.HashResult_EpochChange, digest []byte) *Actions {
	originNode := NodeID(processedChange.Origin)
	sourceNode := NodeID(processedChange.Source)

	change, ok := et.changes[originNode]
	if !ok {
		change = &epochChange{
			networkConfig: et.networkConfig,
		}
		et.changes[originNode] = change
	}

	change.addMsg(sourceNode, processedChange.EpochChange, digest)

	if change.strongCert == nil {
		return &Actions{}
	}

	if _, alreadyInQuorum := et.strongChanges[originNode]; alreadyInQuorum {
		return &Actions{}
	}

	et.strongChanges[originNode] = change.parsedByDigest[string(change.strongCert)]

	return et.advanceState()
}

func (et *epochTarget) checkEpochQuorum() *Actions {
	if len(et.strongChanges) < intersectionQuorum(et.networkConfig) || et.myEpochChange == nil {
		return &Actions{}
	}

	et.myNewEpoch = et.constructNewEpoch(et.myLeaderChoice, et.networkConfig)
	if et.myNewEpoch == nil {

		return &Actions{}
	}

	et.state = pending

	if et.isLeader {
		return (&Actions{}).send(
			et.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_NewEpoch{
					NewEpoch: et.myNewEpoch,
				},
			},
		)
	}

	return &Actions{}
}

func (et *epochTarget) applyNewEpochMsg(msg *pb.NewEpoch) *Actions {
	et.leaderNewEpoch = msg
	return et.advanceState()
}

func (et *epochTarget) applyNewEpochEchoMsg(source NodeID, msg *pb.NewEpochEcho) *Actions {
	var msgEchos map[NodeID]struct{}

	for config, echos := range et.echos {
		if proto.Equal(config, msg.NewConfig) {
			msgEchos = echos
			break
		}
	}

	if msgEchos == nil {
		msgEchos = map[NodeID]struct{}{}
		et.echos[msg.NewConfig] = msgEchos
	}

	msgEchos[source] = struct{}{}

	return et.advanceState()
}

func (et *epochTarget) checkNewEpochEchoQuorum() *Actions {
	actions := &Actions{}
	for config, msgEchos := range et.echos {
		if len(msgEchos) < intersectionQuorum(et.networkConfig) {
			continue
		}

		et.state = readying

		for i, digest := range config.FinalPreprepares {
			seqNo := uint64(i) + config.StartingCheckpoint.SeqNo + 1
			actions.concat(et.persisted.addPEntry(&pb.PEntry{
				SeqNo:  seqNo,
				Digest: digest,
			}))
		}

		return actions.concat(
			et.persisted.addNewEpochReady(config),
		).send(
			et.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_NewEpochReady{
					NewEpochReady: &pb.NewEpochReady{
						NewConfig: config,
					},
				},
			},
		)
	}

	return actions
}

func (et *epochTarget) applyNewEpochReadyMsg(source NodeID, msg *pb.NewEpochReady) *Actions {
	if et.state > readying {
		// We've already accepted the epoch config, move along
		return &Actions{}
	}

	var msgReadies map[NodeID]struct{}

	for config, readies := range et.readies {
		if proto.Equal(config, msg.NewConfig) {
			msgReadies = readies
			break
		}
	}

	if msgReadies == nil {
		msgReadies = map[NodeID]struct{}{}
		et.readies[msg.NewConfig] = msgReadies
	}

	msgReadies[source] = struct{}{}

	if len(msgReadies) < someCorrectQuorum(et.networkConfig) {
		return &Actions{}
	}

	if et.state < echoing {
		return et.advanceState()
	}

	if et.state < readying {
		et.state = readying

		actions := et.persisted.addNewEpochReady(msg.NewConfig)
		// TODO Pset?

		actions.send(
			et.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_NewEpochReady{
					NewEpochReady: &pb.NewEpochReady{
						NewConfig: msg.NewConfig,
					},
				},
			},
		)

		return actions
	}

	return et.advanceState()
}

func (et *epochTarget) checkNewEpochReadyQuorum() *Actions {
	for config, msgReadies := range et.readies {
		if len(msgReadies) < intersectionQuorum(et.networkConfig) {
			continue
		}

		et.state = ready

		et.networkNewEpoch = config

		commits := make([]*Commit, 0, len(config.FinalPreprepares)-int(et.persisted.lastCommitted-config.StartingCheckpoint.SeqNo))

		currentEpoch := false
		for logEntry := et.persisted.logHead; logEntry != nil; logEntry = logEntry.next {
			switch d := logEntry.entry.Type.(type) {
			case *pb.Persistent_QEntry:
				if !currentEpoch {
					continue
				}

				seqNo := d.QEntry.SeqNo
				if seqNo <= et.persisted.lastCommitted {
					continue
				}

				commits = append(commits, &Commit{
					QEntry:      d.QEntry,
					EpochConfig: config.Config,
				})
			case *pb.Persistent_EpochChange:
				if d.EpochChange.NewEpoch < config.Config.Number {
					continue
				}

				if d.EpochChange.NewEpoch > config.Config.Number {
					panic("dev sanity test")
				}

				currentEpoch = true
			}

		}

		return et.persisted.addNewEpochStart(config.Config).concat(
			&Actions{
				Commits: commits,
			},
		)
	}

	return &Actions{}
}

func (et *epochTarget) advanceState() *Actions {
	actions := &Actions{}
	for {
		oldState := et.state
		switch et.state {
		case prepending: // Have sent an epoch-change, but waiting for a quorum
			actions.concat(et.checkEpochQuorum())
		case pending: // Have a quorum of epoch-change messages, waits on new-epoch
			if et.leaderNewEpoch == nil {
				return actions
			}
			et.state = verifying
		case verifying: // Have a new view message but it references epoch changes we cannot yet verify
			actions.concat(et.verifyNewEpochState())
		case fetching: // Have received and verified a new epoch messages, and are waiting to get state
			actions.concat(et.fetchNewEpochState())
		case echoing: // Have received and validated a new-epoch, waiting for a quorum of echos
			actions.concat(et.checkNewEpochEchoQuorum())
		case readying: // Have received a quorum of echos, waiting a on qourum of readies
			actions.concat(et.checkNewEpochReadyQuorum())
		case ready: // New epoch is ready to begin
		case inProgress: // No pending change
		case done: // We have sent an epoch change, ending this epoch for us
		default:
			panic("dev sanity test")
		}
		if et.state == oldState {
			return actions
		}
	}
}

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

func (et *epochTarget) applySuspectMsg(source NodeID) {
	et.suspicions[source] = struct{}{}

	if len(et.suspicions) >= intersectionQuorum(et.networkConfig) {
		et.state = done
	}
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

type epochChange struct {
	// set at creation
	networkConfig *pb.NetworkState_Config

	// set via setMsg and setDigest
	parsedByDigest map[string]*parsedEpochChange

	// updated via updateAcks
	strongCert []byte
}

func (ec *epochChange) addMsg(source NodeID, msg *pb.EpochChange, digest []byte) {
	if ec.parsedByDigest == nil {
		ec.parsedByDigest = map[string]*parsedEpochChange{}
	}

	parsedChange, ok := ec.parsedByDigest[string(digest)]
	if !ok {
		var err error
		parsedChange, err = newParsedEpochChange(msg)
		if err != nil {
			// TODO, log
			return
		}
		ec.parsedByDigest[string(digest)] = parsedChange
	}

	parsedChange.acks[source] = struct{}{}

	if ec.strongCert != nil || len(parsedChange.acks) < intersectionQuorum(ec.networkConfig) {
		return
	}

	ec.strongCert = digest
}

type parsedEpochChange struct {
	underlying   *pb.EpochChange
	pSet         map[uint64]*pb.EpochChange_SetEntry // TODO, maybe make a real type?
	qSet         map[uint64]map[uint64][]byte        // TODO, maybe make a real type?
	lowWatermark uint64

	acks map[NodeID]struct{}
}

func newParsedEpochChange(underlying *pb.EpochChange) (*parsedEpochChange, error) {
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

	return &parsedEpochChange{
		underlying:   underlying,
		lowWatermark: lowWatermark,
		pSet:         pSet,
		qSet:         qSet,
		acks:         map[NodeID]struct{}{},
	}, nil
}

func (ec *epochChange) status(source uint64) *EpochChangeStatus {
	status := &EpochChangeStatus{
		Source: source,
		Msgs:   make([]*EpochChangeMsgStatus, len(ec.parsedByDigest)),
	}

	i := 0
	for digest, parsedEpochChange := range ec.parsedByDigest {
		status.Msgs[i] = &EpochChangeMsgStatus{
			Digest: []byte(digest),
			Acks:   make([]uint64, len(parsedEpochChange.acks)),
		}

		j := 0
		for acker := range parsedEpochChange.acks {
			status.Msgs[i].Acks[j] = uint64(acker)
			j++
		}

		sort.Slice(status.Msgs[i].Acks, func(k, l int) bool {
			return status.Msgs[i].Acks[k] < status.Msgs[i].Acks[l]
		})

		i++
	}

	sort.Slice(status.Msgs, func(i, j int) bool {
		return string(status.Msgs[i].Digest) < string(status.Msgs[j].Digest)
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
