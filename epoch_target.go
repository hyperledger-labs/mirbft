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
	"google.golang.org/protobuf/proto"
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
	activeEpoch   *activeEpoch
	suspicions    map[NodeID]struct{}

	persisted       *persisted
	myNewEpoch      *pb.NewEpoch // The NewEpoch msg we computed from the epoch changes we know of
	myEpochChange   *parsedEpochChange
	myLeaderChoice  []uint64           // Set along with myEpochChange
	leaderNewEpoch  *pb.NewEpoch       // The NewEpoch msg we received directly from the leader
	networkNewEpoch *pb.NewEpochConfig // The NewEpoch msg as received via the bracha broadcast
	isLeader        bool

	logger        Logger
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
	for _, nodeID := range et.networkConfig.Nodes {
		// Deterministic iteration over strong changes
		_, ok := et.strongChanges[NodeID(nodeID)]
		if !ok {
			continue
		}

		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId: uint64(nodeID),
			Digest: et.changes[NodeID(nodeID)].strongCert,
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
			actions.concat(et.batchTracker.fetchBatch(seqNo, digest, sources))
			fetchPending = true
			continue
		}

		batch.observedSequences[seqNo] = struct{}{}

		for _, requestAck := range batch.requestAcks {
			var cr *clientRequest
			for _, nodeID := range sources {
				cr = et.clientWindows.ack(NodeID(nodeID), requestAck)
			}

			if cr.data != nil {
				continue
			}

			// We are missing this request data and must fetch before proceeding
			fetchPending = true
			actions.send(
				sources,
				&pb.Msg{
					Type: &pb.Msg_FetchRequest{
						FetchRequest: requestAck,
					},
				},
			)
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
			et.activeEpoch = newActiveEpoch(et.persisted, et.clientWindows, et.myConfig, et.logger)
			et.state = inProgress

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

func (et *epochTarget) applySuspectMsg(source NodeID) {
	et.suspicions[source] = struct{}{}

	if len(et.suspicions) >= intersectionQuorum(et.networkConfig) {
		et.state = done
	}
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
