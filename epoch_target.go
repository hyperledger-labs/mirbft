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
	"github.com/IBM/mirbft/status"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type epochTargetState int

const (
	etPrepending = iota // Have sent an epoch-change, but waiting for a quorum
	etPending           // Have a quorum of epoch-change messages, waits on new-epoch
	etVerifying         // Have a new view message but it references epoch changes we cannot yet verify
	etFetching          // Have received and verified a new epoch messages, and are waiting to get state
	etEchoing           // Have received and validated a new-epoch, waiting for a quorum of echos
	etReadying          // Have received a quorum of echos, waiting a on qourum of readies
	etReady             // New epoch is ready to begin
	etInProgress        // No pending change
	etDone              // We have sent an epoch change, ending this epoch for us
)

// epochTarget is like an epoch, but this node need not have agreed
// to transition to this target, and may not have information like the
// epoch configuration
type epochTarget struct {
	state           epochTargetState
	commitState     *commitState
	stateTicks      uint64
	number          uint64
	changes         map[nodeID]*epochChange
	strongChanges   map[nodeID]*parsedEpochChange
	echos           map[*pb.NewEpochConfig]map[nodeID]struct{}
	readies         map[*pb.NewEpochConfig]map[nodeID]struct{}
	activeEpoch     *activeEpoch
	suspicions      map[nodeID]struct{}
	myNewEpoch      *pb.NewEpoch // The NewEpoch msg we computed from the epoch changes we know of
	myEpochChange   *parsedEpochChange
	myLeaderChoice  []uint64           // Set along with myEpochChange
	leaderNewEpoch  *pb.NewEpoch       // The NewEpoch msg we received directly from the leader
	networkNewEpoch *pb.NewEpochConfig // The NewEpoch msg as received via the bracha broadcast
	isLeader        bool

	nodeMsgs      map[nodeID]*nodeMsgs
	persisted     *persisted
	clientTracker *clientTracker
	batchTracker  *batchTracker
	networkConfig *pb.NetworkState_Config
	myConfig      *pb.StateEvent_InitialParameters
	logger        Logger
}

func newEpochTarget(
	number uint64,
	persisted *persisted,
	commitState *commitState,
	clientTracker *clientTracker,
	batchTracker *batchTracker,
	networkConfig *pb.NetworkState_Config,
	myConfig *pb.StateEvent_InitialParameters,
	logger Logger,
) *epochTarget {
	// TODO if we like this pattern, it should get passed down from
	// state machine, but, leaving here for refactor purposes
	oddities := &oddities{
		logger: logger,
	}

	nodeMsgs := map[nodeID]*nodeMsgs{}
	for _, id := range networkConfig.Nodes {
		nodeMsgs[nodeID(id)] = newNodeMsgs(nodeID(id), networkConfig, logger, myConfig, oddities)
	}

	return &epochTarget{
		number:        number,
		commitState:   commitState,
		suspicions:    map[nodeID]struct{}{},
		changes:       map[nodeID]*epochChange{},
		strongChanges: map[nodeID]*parsedEpochChange{},
		echos:         map[*pb.NewEpochConfig]map[nodeID]struct{}{},
		readies:       map[*pb.NewEpochConfig]map[nodeID]struct{}{},
		isLeader:      number%uint64(len(networkConfig.Nodes)) == myConfig.Id,
		nodeMsgs:      nodeMsgs,
		persisted:     persisted,
		clientTracker: clientTracker,
		batchTracker:  batchTracker,
		networkConfig: networkConfig,
		myConfig:      myConfig,
		logger:        logger,
	}
}

func (et *epochTarget) step(source nodeID, msg *pb.Msg) *Actions {
	nodeMsgs, ok := et.nodeMsgs[source]
	if !ok {
		et.logger.Panic("received a message from a node ID that does not exist", zap.Int("source", int(source)))
	}

	nodeMsgs.ingest(msg)

	return et.drainNodeMsgs()
}

func (et *epochTarget) drainNodeMsgs() *Actions {
	actions := &Actions{}

	for {
		moreActions := false
		for _, id := range et.networkConfig.Nodes {
			source := nodeID(id)
			nodeMsgs := et.nodeMsgs[source]
			msg := nodeMsgs.next()
			if msg == nil {
				continue
			}
			moreActions = true

			switch innerMsg := msg.Type.(type) {
			case *pb.Msg_Preprepare:
				msg := innerMsg.Preprepare
				actions.concat(et.activeEpoch.applyPreprepareMsg(source, msg.SeqNo, msg.Batch))
			case *pb.Msg_Prepare:
				msg := innerMsg.Prepare
				actions.concat(et.activeEpoch.applyPrepareMsg(source, msg.SeqNo, msg.Digest))
			case *pb.Msg_Commit:
				msg := innerMsg.Commit
				actions.concat(et.activeEpoch.applyCommitMsg(source, msg.SeqNo, msg.Digest))
			default:
				panic("unexpected type")
			}
		}

		if !moreActions {
			return actions
		}
	}
}

func (et *epochTarget) constructNewEpoch(newLeaders []uint64, nc *pb.NetworkState_Config) *pb.NewEpoch {
	filteredStrongChanges := map[nodeID]*parsedEpochChange{}
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
	for _, id := range et.networkConfig.Nodes {
		// Deterministic iteration over strong changes
		_, ok := et.strongChanges[nodeID(id)]
		if !ok {
			continue
		}

		remoteChanges = append(remoteChanges, &pb.NewEpoch_RemoteEpochChange{
			NodeId: uint64(id),
			Digest: et.changes[nodeID(id)].strongCert,
		})
	}

	return &pb.NewEpoch{
		NewConfig:    newConfig,
		EpochChanges: remoteChanges,
	}
}

func (et *epochTarget) verifyNewEpochState() *Actions {
	epochChanges := map[nodeID]*parsedEpochChange{}
	for _, remoteEpochChange := range et.leaderNewEpoch.EpochChanges {
		if _, ok := epochChanges[nodeID(remoteEpochChange.NodeId)]; ok {
			// TODO, references multiple epoch changes from the same node, malformed, log oddity
			return &Actions{}
		}

		change, ok := et.changes[nodeID(remoteEpochChange.NodeId)]
		if !ok {
			// Either the primary is lying, or, we simply don't have enough information yet.
			return &Actions{}
		}

		parsedChange, ok := change.parsedByDigest[string(remoteEpochChange.Digest)]
		if !ok || len(parsedChange.acks) < someCorrectQuorum(et.networkConfig) {
			return &Actions{}
		}

		epochChanges[nodeID(remoteEpochChange.NodeId)] = parsedChange
	}

	// TODO, validate the planned expiration makes sense

	// TODO, do we need to try to validate the leader set?

	newEpochConfig := constructNewEpochConfig(et.networkConfig, et.leaderNewEpoch.NewConfig.Config.Leaders, epochChanges)

	if !proto.Equal(newEpochConfig, et.leaderNewEpoch.NewConfig) {
		// TODO byzantine, log oddity
		return &Actions{}
	}

	et.state = etFetching

	return et.advanceState()
}

func (et *epochTarget) fetchNewEpochState() *Actions {
	actions := &Actions{}

	newEpochConfig := et.leaderNewEpoch.NewConfig

	if newEpochConfig.StartingCheckpoint.SeqNo > et.commitState.lowWatermark {
		if newEpochConfig.StartingCheckpoint.SeqNo <= et.commitState.lastCommit {
			// We've committed through this checkpoint, but are awaiting the result
			return actions
		}

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
			change := et.changes[nodeID(remoteEpochChange.NodeId)]
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
			for _, id := range sources {
				cr = et.clientTracker.ack(nodeID(id), requestAck)
			}

			if _, ok := cr.agreements[nodeID(et.myConfig.Id)]; ok {
				continue
			}

			// We are missing this request data and must fetch before proceeding
			fetchPending = true
			actions.concat(cr.fetch())
		}
	}

	if fetchPending {
		return actions
	}

	et.state = etEchoing

	for i, digest := range newEpochConfig.FinalPreprepares {
		seqNo := uint64(i) + newEpochConfig.StartingCheckpoint.SeqNo + 1
		if len(digest) == 0 {
			actions.concat(et.persisted.addQEntry(&pb.QEntry{
				SeqNo: seqNo,
			}))
			continue
		}

		batch, _ := et.batchTracker.getBatch(digest)

		qEntry := &pb.QEntry{
			SeqNo:    seqNo,
			Digest:   digest,
			Requests: batch.requestAcks,
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
	et.stateTicks++
	switch et.state {
	case etPrepending:
		return et.tickPrepending()
	case etPending:
		return et.tickPending()
	case etInProgress:
		return et.activeEpoch.tick()
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

func (et *epochTarget) applyEpochChangeMsg(source nodeID, msg *pb.EpochChange) *Actions {
	actions := &Actions{}
	if source != nodeID(et.myConfig.Id) {
		// We don't want to echo our own EpochChange message,
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

	// Automatically apply an ACK from the originator
	return actions.concat(et.applyEpochChangeAckMsg(source, source, msg))
}

func (et *epochTarget) applyEpochChangeAckMsg(source nodeID, origin nodeID, msg *pb.EpochChange) *Actions {
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
	originNode := nodeID(processedChange.Origin)
	sourceNode := nodeID(processedChange.Source)

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

	et.stateTicks = 0
	et.state = etPending

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

func (et *epochTarget) applyNewEpochEchoMsg(source nodeID, msg *pb.NewEpochEcho) *Actions {
	var msgEchos map[nodeID]struct{}

	for config, echos := range et.echos {
		if proto.Equal(config, msg.NewConfig) {
			msgEchos = echos
			break
		}
	}

	if msgEchos == nil {
		msgEchos = map[nodeID]struct{}{}
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

		et.state = etReadying

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

func (et *epochTarget) applyNewEpochReadyMsg(source nodeID, msg *pb.NewEpochReady) *Actions {
	if et.state > etReadying {
		// We've already accepted the epoch config, move along
		return &Actions{}
	}

	var msgReadies map[nodeID]struct{}

	for config, readies := range et.readies {
		if proto.Equal(config, msg.NewConfig) {
			msgReadies = readies
			break
		}
	}

	if msgReadies == nil {
		msgReadies = map[nodeID]struct{}{}
		et.readies[msg.NewConfig] = msgReadies
	}

	msgReadies[source] = struct{}{}

	if len(msgReadies) < someCorrectQuorum(et.networkConfig) {
		return &Actions{}
	}

	if et.state < etEchoing {
		return et.advanceState()
	}

	if et.state < etReadying {
		et.state = etReadying

		actions := et.persisted.addNewEpochReady(msg.NewConfig)

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

		et.state = etReady

		et.networkNewEpoch = config

		currentEpoch := false
		for logEntry := et.persisted.logHead; logEntry != nil; logEntry = logEntry.next {
			switch d := logEntry.entry.Type.(type) {
			case *pb.Persistent_QEntry:
				if !currentEpoch {
					continue
				}

				et.commitState.commit(d.QEntry)
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

		return et.persisted.addNewEpochStart(config.Config)
	}

	return &Actions{}
}

func (et *epochTarget) advanceState() *Actions {
	actions := &Actions{}
	for {
		oldState := et.state
		switch et.state {
		case etPrepending: // Have sent an epoch-change, but waiting for a quorum
			actions.concat(et.checkEpochQuorum())
		case etPending: // Have a quorum of epoch-change messages, waits on new-epoch
			if et.leaderNewEpoch == nil {
				return actions
			}
			et.state = etVerifying
		case etVerifying: // Have a new view message but it references epoch changes we cannot yet verify
			actions.concat(et.verifyNewEpochState())
		case etFetching: // Have received and verified a new epoch messages, and are waiting to get state
			actions.concat(et.fetchNewEpochState())
		case etEchoing: // Have received and validated a new-epoch, waiting for a quorum of echos
			actions.concat(et.checkNewEpochEchoQuorum())
		case etReadying: // Have received a quorum of echos, waiting a on qourum of readies
			actions.concat(et.checkNewEpochReadyQuorum())
		case etReady: // New epoch is ready to begin
			et.activeEpoch = newActiveEpoch(et.networkNewEpoch.Config, et.persisted, et.commitState, et.clientTracker, et.myConfig, et.logger)
			// TODO, handle case where planned epoch expiration is now
			et.state = etInProgress
			// It's important not to step through into the next state transition,
			// as we must commit the seqs proposed by other replicas in previous
			// epochs prior to attempting to propose our own (and potentially
			// re-proposing the same rquests)
			for _, nodeMsgs := range et.nodeMsgs {
				nodeMsgs.setActiveEpoch(et.activeEpoch)
			}
			return actions
		case etInProgress: // No pending change
			actions.concat(et.activeEpoch.outstandingReqs.advanceRequests())
			actions.concat(et.activeEpoch.advance())
			actions.concat(et.drainNodeMsgs())
		case etDone: // We have sent an epoch change, ending this epoch for us
			for _, nodeMsgs := range et.nodeMsgs {
				nodeMsgs.setActiveEpoch(nil)
			}
		default:
			panic("dev sanity test")
		}
		if et.state == oldState {
			return actions
		}
	}
}

func (et *epochTarget) moveLowWatermark(seqNo uint64) *Actions {
	if et.state != etInProgress {
		fmt.Printf("JKY: Ignoring watermark movement because not in progress\n")
		return &Actions{}
	}

	actions, done := et.activeEpoch.moveLowWatermark(seqNo)
	if done {
		et.state = etDone
	}

	for _, nodeMsgs := range et.nodeMsgs {
		nodeMsgs.epochMsgs.moveWatermarks(seqNo)
	}

	return actions.concat(et.drainNodeMsgs())
}

func (et *epochTarget) applySuspectMsg(source nodeID) {
	et.suspicions[source] = struct{}{}

	if len(et.suspicions) >= intersectionQuorum(et.networkConfig) {
		et.state = etDone
	}
}

func (et *epochTarget) bucketStatus() (lowWatermark, highWatermark uint64, bucketStatus []*status.Bucket) {
	if et.activeEpoch != nil {
		bucketStatus = et.activeEpoch.status()
		lowWatermark = et.activeEpoch.lowWatermark()
		highWatermark = et.activeEpoch.highWatermark()
		return
	}

	if et.state <= etFetching {
		lowWatermark = et.myEpochChange.lowWatermark + 1
		highWatermark = lowWatermark + uint64(2*et.networkConfig.CheckpointInterval) - 1
	} else {
		// We are echoing or better, so leaderNewEpoch is set
		lowWatermark = et.leaderNewEpoch.NewConfig.StartingCheckpoint.SeqNo + 1
		highWatermark = lowWatermark + uint64(2*et.networkConfig.CheckpointInterval) - 1
	}

	bucketStatus = make([]*status.Bucket, int(et.networkConfig.NumberOfBuckets))
	for i := range bucketStatus {
		bucketStatus[i] = &status.Bucket{
			ID:        uint64(i),
			Sequences: make([]status.SequenceState, int(highWatermark-lowWatermark)/len(bucketStatus)+1),
		}
	}

	setStatus := func(seqNo uint64, status status.SequenceState) {
		bucket := int(seqToBucket(seqNo, et.networkConfig))
		column := int(seqNo-lowWatermark) / len(bucketStatus)
		if column >= len(bucketStatus[bucket].Sequences) {
			// XXX this is a nasty case which can happen sometimes,
			// when we've begun echoing a new epoch, before we have
			// actually executed through the checkpoint selected as
			// the base for the new epoch.  Working on a solution
			// but as this is simply status, ignoring
			return
		}
		bucketStatus[bucket].Sequences[column] = status
	}

	if et.state <= etFetching {
		for seqNo := range et.myEpochChange.qSet {
			setStatus(seqNo, status.SequencePreprepared)
		}

		for seqNo := range et.myEpochChange.pSet {
			setStatus(seqNo, status.SequencePrepared)
		}

		for seqNo := lowWatermark; seqNo <= et.commitState.lastCommit; seqNo++ {
			setStatus(seqNo, status.SequenceCommitted)
		}
		return
	}

	for seqNo := lowWatermark; seqNo <= highWatermark; seqNo++ {
		var state status.SequenceState

		if et.state == etEchoing {
			state = status.SequencePreprepared
		}

		if et.state == etReadying {
			state = status.SequencePrepared
		}

		if seqNo <= et.commitState.lastCommit || et.state == etReady {
			state = status.SequenceCommitted
		}

		setStatus(seqNo, state)
	}

	return
}

func (et *epochTarget) status() *status.EpochTarget {
	result := &status.EpochTarget{
		EpochChanges: make([]*status.EpochChange, 0, len(et.changes)),
		Echos:        make([]uint64, 0, len(et.echos)),
		Readies:      make([]uint64, 0, len(et.readies)),
		Suspicions:   make([]uint64, 0, len(et.suspicions)),
	}

	for node, change := range et.changes {
		result.EpochChanges = append(result.EpochChanges, change.status(uint64(node)))
	}
	sort.Slice(result.EpochChanges, func(i, j int) bool {
		return result.EpochChanges[i].Source < result.EpochChanges[j].Source
	})

	for _, echoMsgs := range et.echos {
		for node := range echoMsgs {
			result.Echos = append(result.Echos, uint64(node))
		}
	}

	sort.Slice(result.Echos, func(i, j int) bool {
		return result.Echos[i] < result.Echos[j]
	})

	for _, readyMsgs := range et.readies {
		for node := range readyMsgs {
			result.Readies = append(result.Readies, uint64(node))
		}
	}
	sort.Slice(result.Readies, func(i, j int) bool {
		return result.Readies[i] < result.Readies[j]
	})

	for node := range et.suspicions {
		result.Suspicions = append(result.Suspicions, uint64(node))
	}
	sort.Slice(result.Suspicions, func(i, j int) bool {
		return result.Suspicions[i] < result.Suspicions[j]
	})

	return result
}
