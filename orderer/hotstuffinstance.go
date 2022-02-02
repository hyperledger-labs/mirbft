// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orderer

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/announcer"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/tracing"
)

// Implements chained HotStuff for the sequence numbers of the segment.
// We keep the notion of view coupled with the leader of the segment so that only in view=0 new bathces are proposed.
// Notice that in chained HotStuff the term view is coupled with proposals.
// Here the sequence number plays the role of view in HotStuff.
type hotStuffInstance struct {
	segment manager.Segment  // The segment of the instance
	orderer *HotStuffOrderer // The HotStuff orderer

	leader    int32
	view      int32           // In each view we have a persistent leader for the segment
	next      int             // The segment *index* of the next to be proposed seq no
	height2sn map[int32]int32 // Height to segment seq no
	sn2height map[int32]int32 // Segment seq no to height

	nodes   []*hotStuffNode // HotStuff nodes (including dummy nodes)
	vheight int32           // The height of the last voted node
	leaf    *hotStuffNode   // Leaf node
	locked  *hotStuffNode   // The latest locked node
	highQC  *pb.HotStuffQC  // Highest quorum certificate available

	viewChangeTimeout time.Duration                           // View change duration timeout
	backlog           *hotStuffBacklog                        // Backlog for future segment seq nos
	serializer        *ordererChannel                         // Channel of messages that must be handled sequentially
	newViewVotes      map[int32]map[int32]*pb.HotStuffNewView // Structure to count the new view votes (height -> senderID -> vote)

	stopProp sync.Once
	newBatch chan *request.Batch //

	// After all the sequence numbers of the segment are proposed the leader keeps advancing
	// the height until the last sequence number commits - an therefore the instance is killed.
	// To keep advancing the height without using sequence numbers of the next segment, the leader
	// reuses the last sequence number of the segment.
	// No more that one heights will be committed with the last sequence number of the segment, since
	// the segment will be killed once the last sequence number is committed.
	segmentProposed bool
}

type hotStuffNode struct {
	height    int32
	sn        int32
	leader    int32
	node      *pb.HotStuffNode
	batch     *request.Batch
	parent    *hotStuffNode
	votes     map[int32]*pb.SignedMsg // Should be append only to prevent double voting.
	digest    []byte                  // The digest of the proposal (preprepare) message
	quorum    bool                    // True if there is a vote quorum
	announced bool                    // True if the node has been announced
	dummy     bool                    // Node that should not be executed (announced)
	// only proposed to make sure the segment finishes
	viewChangeTimer *time.Timer // Timer to start a view change
}

func (hi *hotStuffInstance) init(seg manager.Segment, orderer *HotStuffOrderer) {
	logger.Info().Int("segment", seg.SegID()).
		Int32("first", seg.FirstSN()).
		Msg("Starting HotStuff instance.")
	// Start the instance at view 0
	hi.view = 0

	// Initialize leader
	hi.leader = segmentLeader(seg, hi.view)

	// Attach segment to the instance
	hi.segment = seg

	// Attach orderer to the instance
	hi.orderer = orderer

	// Initialize backlog
	hi.backlog = newHotStuffBacklog(hi)

	// Initialize view change timeout duration
	hi.viewChangeTimeout = config.Config.ViewChangeTimeout

	// Initialise nodes
	hi.nodes = make([]*hotStuffNode, 0, 0)

	// Initialize new view  vote counting structure
	hi.newViewVotes = make(map[int32]map[int32]*pb.HotStuffNewView)

	// Initialise the root node
	node := &pb.HotStuffNode{
		Height: 0,
		Parent: nil,
		Batch: &pb.Batch{
			Requests: make([]*pb.ClientRequest, 0, 0),
		},
		Certificate: &pb.HotStuffQC{
			Height:    0,
			Signature: nil,
		},
	}
	rootNode :=
		&hotStuffNode{
			height:    0,
			node:      node,
			digest:    hotStuffDigest(node),
			dummy:     true,
			announced: true, // We never announce the root node
		}

	rootNode.node.Certificate = &pb.HotStuffQC{
		Height:    0,
		Signature: rootNode.node.Certificate.Signature,
	}
	hi.addNode(rootNode)
	rootNode.parent = rootNode
	hi.vheight = 0
	hi.locked = rootNode
	hi.leaf = rootNode
	hi.highQC = rootNode.node.Certificate

	// Initalize channel
	hi.serializer = newOrdererChannel(channelSize)

	// Initialize indexing of segment sequence numbers
	hi.next = 0
	hi.height2sn = make(map[int32]int32)
	hi.sn2height = make(map[int32]int32)

	// Initialize the batch channel as a buffered channel of one batch
	hi.newBatch = make(chan *request.Batch, 1)
}

func (hi *hotStuffInstance) start() {
	// If this instance leads the segment
	if isLeading(hi.segment, membership.OwnID, hi.view) {
		logger.Debug().Int("segment", hi.segment.SegID()).Msg("Leading segment.")

		// Schedule a batch for the first sn in the segment
		go func() {
			hi.newBatch <- hi.segment.Buckets().CutBatch(config.Config.BatchSize, config.Config.BatchTimeout)
		}()

		// Send a proposal for the *fist* sn in the segment
		hi.proposeSN(hi.segment.FirstSN())
	}
}

// Proposes a new value for sequence number sn in Segment segment by creating a new leaf node and broadcasting
// the proposal to all followers of the segment.
func (hi *hotStuffInstance) proposeSN(sn int32) {
	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", hi.leaf.height+1).
		Int32("view", hi.view).
		Int32("senderID", membership.OwnID).
		Bool("proposed", hi.segmentProposed).
		Msg("Creating PROPOSAL.")

	// Create a new node
	// New batches are proposed only in view 0
	var batch *request.Batch
	if hi.view == 0 && !hi.segmentProposed {
		// Wait for a new batch
		batch = <-hi.newBatch

		logger.Info().Int32("sn", sn).
			Int("segment", hi.segment.SegID()).
			Int32("height", hi.leaf.height+1).
			Int32("view", hi.view).
			Int32("senderID", membership.OwnID).
			Int("nReq", len(batch.Requests)).
			Msg("New BATCH.")

		hi.next = hi.next + 1
		// If we have proposed all the sequence numbers in the segment, mark the segment as "proposed"
		if int32(hi.next) == hi.segment.Len() {
			hi.segmentProposed = true
		}

		// If the segment is not proposed yet schedule a new batch
		if !hi.segmentProposed {
			go func() {
				hi.newBatch <- hi.segment.Buckets().CutBatch(config.Config.BatchSize, config.Config.BatchTimeout)
			}()
		}

	} else {
		batch = &request.Batch{Requests: make([]*request.Request, 0, 0)}
		hi.next = hi.next + 1
	}

	new := hi.newNode(hi.leaf, batch, hi.highQC, sn, hi.leaf.height+1, membership.OwnID)

	if _, ok := hi.sn2height[sn]; ok {
		new.dummy = true
	}

	if hi.view > 1 {
		new.node.Aborted = true
	}

	hi.leaf = new

	// Create message
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_Proposal{
			Proposal: &pb.HotStuffProposal{
				Leader: membership.OwnID,
				Node:   new.node,
			},
		},
	}

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", new.height).
		Int32("view", hi.view).
		Int32("senderId", membership.OwnID).
		Bool("proposed", hi.segmentProposed).
		Msg("Sending PROPOSAL.")

	tracing.MainTrace.Event(tracing.PROPOSE, int64(sn), int64(len(batch.Requests)))

	// Handle own proposal as follower to make sure state for this node is created before votes are received
	hi.handleProposal(msg.GetProposal(), msg)

	// Enqueue the message for all followers
	for _, nodeID := range hi.segment.Followers() {
		// Skip sending to self
		if nodeID == membership.OwnID {
			continue
		}
		messenger.EnqueuePriorityMsg(msg, nodeID)
	}
}

func (hi *hotStuffInstance) handleProposal(proposal *pb.HotStuffProposal, msg *pb.ProtocolMessage) error {
	sn := msg.Sn
	senderID := msg.SenderId

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", proposal.Node.Height).
		Int32("certificate", proposal.Node.Certificate.Height).
		Int32("senderId", senderID).
		Msg("Handling PROPOSAL.")

	logger.Info().Int32("vheigt", hi.vheight).
		Int("segment", hi.segment.SegID()).
		Int32("leaf", hi.leaf.node.Height).
		Int32("highQC", hi.highQC.Height).
		Int32("locked", hi.locked.node.Height).
		Int("Nodes", len(hi.nodes)).
		Msg("Follower's initial local state")

	if senderID != proposal.Leader {
		hi.sendNewView()
		return fmt.Errorf("malformed message: sender %d does not match leader %d", senderID, proposal.Leader)
	}

	// TODO can a Byzantine leader make you get stuck in a future view?
	// Update your view if necessary
	if proposal.Node.View > hi.view {
		hi.startView(proposal.Node.View)
	}

	// Check if we already voted for this height.
	// For each height we only vote once.
	// If a new view occurs an un-decided sequence number will be proposed at a different height.
	if hi.vheight >= proposal.Node.Height {
		logger.Warn().
			Int("segment", hi.segment.SegID()).
			Int32("height", proposal.Node.Height).
			Msgf("Already voted for height %d", hi.vheight)
		return nil
	}

	// Check we have already voted for the certificate height to use certificate as a parent.
	// Otherwise backlog the message until we do.
	if proposal.Node.Certificate.Height >= int32(len(hi.nodes)) {
		logger.Warn().
			Int("segment", hi.segment.SegID()).
			Int32("height", proposal.Node.Height).
			Msgf("Out of order message, have not voted for parent yet. I am at vheight %d: ", hi.vheight)
		hi.backlog.add(proposal.Node.Height, msg)
		return nil
	}

	// Verify the certificate
	if proposal.Node.Height > 1 {
		if err := hi.orderer.CheckCert(proposal.Node.Parent, proposal.Node.Certificate.Signature); err != nil {
			hi.sendNewView()
			return fmt.Errorf("invalid certificate from %d: %s", senderID, err.Error())
		}
	}

	// Check if the proposal is for a new view.
	// If so, initialize the new view.
	// TODO why trust view number here?
	if proposal.Node.View > hi.view {
		hi.startView(proposal.Node.View)
	}

	// Check if the proposal extends the locked node
	if !extends(proposal.Node, hi.locked.node) && hi.locked.height >= proposal.Node.Certificate.Height {
		logger.Warn().
			Int("segment", hi.segment.SegID()).
			Int32("height", proposal.Node.Height).
			Msgf("Proposal node does not extend locked node with height %d", hi.vheight)
		return nil
	}

	batch := request.NewBatch(proposal.Node.Batch)
	if batch == nil {
		hi.sendNewView()
		return fmt.Errorf("malformed message from sender %d: invalid batch", senderID)
	}

	// Check that proposal does not contain preprepared ("in flight") requests.
	if err := batch.CheckInFlight(); err != nil {
		return fmt.Errorf("proposal %d from %d contains in flight requests: %s", sn, senderID, err.Error())
	}

	// Check that proposal does not contain requests that do not match the current active bucket
	if err := batch.CheckBucket(hi.segment.Buckets().GetBucketIDs()); err != nil {
		return fmt.Errorf("proposal from %d contains in requests from invalid bucket: %s", senderID, err.Error())
	}

	hi.vheight = proposal.Node.Height

	new := hi.newNode(hi.nodes[proposal.Node.Certificate.Height], batch, proposal.Node.Certificate, sn, proposal.Node.Height, senderID)
	batch.MarkInFlight()

	// Update to own log
	hi.height2sn[proposal.Node.Height] = sn
	if _, ok := hi.sn2height[sn]; !ok {
		hi.sn2height[sn] = proposal.Node.Height // Only map the seq no the the first height
		// Nodes with the same seq no and and higher height are dummy hodes
		// only proposed to ensure the last seq no of the segment commits
	} else {
		new.dummy = true
	}
	hi.addNode(new)
	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", proposal.Node.Height).
		Int32("certificate", proposal.Node.Certificate.Height).
		Int32("senderId", senderID).
		Msg("Follower updated log.")

	hi.updateHighQC(new.node.Certificate)
	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", proposal.Node.Height).
		Int32("certificate", proposal.Node.Certificate.Height).
		Int32("senderId", senderID).
		Msg("Follower updated highQC.")

	if new.parent.parent.height > hi.locked.height {
		hi.locked = new.parent.parent
	}

	decided := new.parent.parent.parent
	if !decided.announced {
		hi.announce(decided, hi.height2sn[decided.height], decided.batch.Message(), decided.digest, decided.node.Aborted)
	}

	// Return to serializer the proposal with the next height to be voted.
	hi.backlog.process(hi.vheight + 1)

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", proposal.Node.Height).
		Int32("certificate", proposal.Node.Certificate.Height).
		Int32("senderId", senderID).
		Msg("Follower processed backlog.")

	// Send vote
	hi.sendVote(new)

	return nil
}

func (hi *hotStuffInstance) sendVote(node *hotStuffNode) {
	logger.Info().Int32("sn", hi.height2sn[node.height]).
		Int("segment", hi.segment.SegID()).
		Int32("height", node.height).
		Int32("leader", hi.leader).
		Msg("Creating VOTE.")

	vote := &pb.HotStuffVote{
		Height: node.height,
		Digest: node.digest,
	}

	data, err := proto.Marshal(vote)
	if err != nil {
		logger.Error().Err(err)
		return
	}

	// TODO should we also sign the height?
	signature, err := hi.orderer.Sign(node.digest)
	if err != nil {
		logger.Error().Err(err)
		return
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       hi.height2sn[node.height],
		Msg: &pb.ProtocolMessage_Vote{
			Vote: &pb.SignedMsg{
				Data:      data,
				Signature: signature,
			},
		},
	}

	logger.Info().Int32("sn", hi.height2sn[node.height]).
		Int("segment", hi.segment.SegID()).
		Int32("height", node.height).
		Int32("leader", hi.leader).
		Msg("Sending VOTE.")

	// Enqueue the message to the leader
	messenger.EnqueueMsg(msg, node.leader)
}

func (hi *hotStuffInstance) handleVote(signed *pb.SignedMsg, sn, senderID int32) error {
	vote := &pb.HotStuffVote{}
	err := proto.Unmarshal(signed.Data, vote)
	if err != nil {
		return fmt.Errorf("could not unmarshall signed vote message from %d for seq no %d", senderID, sn)
	}

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", vote.Height).
		Int32("senderId", senderID).
		Msg("Handling VOTE.")

	// If the message is from a previous height, ignore
	if hi.leaf.height > vote.Height {
		logger.Debug().
			Int("segment", hi.segment.SegID()).
			Msgf("deprecated height %d, we are in height %d", vote.Height, hi.leaf.height)
		return nil
	}

	// Message cannot be from a future height
	if hi.leaf.height < vote.Height {
		return fmt.Errorf("vote from future height %d shouldn't have been received, we are in height %d", vote.Height, hi.leaf.height)
	}

	// Make sure there is only one vote form each follower
	if _, ok := hi.leaf.votes[senderID]; ok {
		return fmt.Errorf("duplicate vote message from %d", senderID)
	}

	// Make sure the vote matches the leaf
	if bytes.Compare(hi.leaf.digest, vote.Digest) != 0 {
		return fmt.Errorf("vote for height %x form %d doesn't match leaf at height %x", vote.Digest, senderID, hi.leaf.digest)
	}

	// Verify the signature share on the message
	if err := hi.orderer.CheckSig(vote.Digest, senderID, signed.Signature); err != nil {
		return fmt.Errorf("could not verify signature share from %d for seq no %d", senderID, sn)
	}

	hi.leaf.votes[senderID] = signed

	// Check for a quorum of votes
	if !hi.leaf.quorum && voteQuorum(hi.leaf) {
		hi.leaf.quorum = true
	}

	// If no vote quorum wait for more votes
	if !hi.leaf.quorum {
		return nil
	}

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", vote.Height).
		Int32("senderId", senderID).
		Msg("Assembling CERT.")

	// Assemble certificate
	signatures := make([][]byte, len(hi.leaf.votes))
	i := 0
	for _, v := range hi.leaf.votes {
		signatures[i] = v.Signature
		i++
	}

	certificate, err := hi.orderer.AssembleCert(hi.leaf.digest, signatures)
	if err != nil {
		return fmt.Errorf("could not assemble certificate: %s", err.Error())
	}

	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", vote.Height).
		Int32("senderId", senderID).
		Msg("Updating  state.")

	hi.updateHighQC(&pb.HotStuffQC{Height: vote.Height, Node: hi.leaf.node, Signature: certificate})

	// Propose next batch
	// Check we have still un-proposed sequence numbers in the segment
	if int32(hi.next) < hi.segment.Len() {
		hi.proposeSN(hi.segment.SNs()[hi.next])
		return nil
	}

	// If we have proposed up to last height + 2 we have poroposed enough to make sure the last height commits
	if int32(hi.next) > hi.segment.Len()+2 {
		return nil
	}

	// Reuse the last sequence number to make sure that it commits
	hi.proposeSN(hi.segment.LastSN())

	return nil
}

func (hi *hotStuffInstance) sendNewView() {
	// Advance view
	hi.view += 1

	// Update leader
	hi.leader = segmentLeader(hi.segment, hi.view)

	// Send a vote for the highQC as a NewVew
	newview := &pb.HotStuffNewView{
		View:        hi.view,
		Certificate: hi.highQC,
	}

	// Send also the node of the certificate
	// TODO can we optimize this and lazily fetch the node?
	newview.Certificate.Node = hi.nodes[hi.highQC.Height].node

	orderMsg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       hi.height2sn[hi.highQC.Height],
		Msg: &pb.ProtocolMessage_HotstuffNewview{
			HotstuffNewview: newview,
		},
	}

	logger.Info().
		Int("segment", hi.segment.SegID()).
		Int32("sn", hi.height2sn[hi.highQC.Height]).
		Int32("view", hi.view).
		Int32("height", newview.Certificate.Height).
		Int32("leader", hi.leader).
		Msg("Sending NEW VIEW.")

	// Enqueue the message to the new leader
	messenger.EnqueueMsg(orderMsg, hi.leader)
}

func (hi *hotStuffInstance) handleNewView(newview *pb.HotStuffNewView, sn, senderID int32) error {
	logger.Info().Int32("sn", sn).
		Int("segment", hi.segment.SegID()).
		Int32("height", newview.Certificate.Height).
		Int32("senderID", senderID).
		Msg("Handling NEW VIEW.")

	// Check if we need to procees this message
	if newview.View < hi.view {
		return fmt.Errorf("old new view from %d for view %d, we are already in view %d", senderID, newview.View, hi.view)
	}

	// Initialize votes for this height if necessary
	if _, ok := hi.newViewVotes[newview.Certificate.Height]; !ok {
		hi.newViewVotes[newview.Certificate.Height] = make(map[int32]*pb.HotStuffNewView)
	}

	// Make sure there is only one vote form each follower
	if _, ok := hi.newViewVotes[newview.Certificate.Height][senderID]; ok {
		return fmt.Errorf("duplicate new view vote message from %d", senderID)
	}

	// Check if you collected a quorum
	if len(hi.newViewVotes[newview.Certificate.Height]) < 2*membership.Faults()+1 {
		return nil
	}

	// Find the highest QC you have heard of.
	// Noone could have committed to a QC higher than this,
	// otherwise some of the 2f+1 votes would have included that other higher QC.
	highQC := hi.highQC
	for _, vote := range hi.newViewVotes[newview.Certificate.Height] {
		if vote.Certificate.Height > highQC.Height {
			highQC = vote.Certificate
		}
	}

	hi.updateHighQC(highQC)

	hi.startView(newview.View)

	hi.next = len(hi.nodes) - 1

	// Propose next sn
	if int32(hi.next) < hi.segment.Len() {
		hi.proposeSN(hi.leaf.height + 1)
		return nil
	}

	// If we have proposed up to last height + 2 we have poroposed enough to make sure the last height commits
	if int32(hi.next) > hi.segment.Len()+2 {
		return nil
	}

	// Reuse the last sequence number to make sure that it commits
	hi.proposeSN(hi.segment.LastSN())

	return nil
}

func (hi *hotStuffInstance) handleMissingEntry(msg *pb.MissingEntry) {
	logger.Info().
		Int("segment", hi.segment.SegID()).
		Int32("view", hi.view).
		Int32("sn", msg.Sn).
		Msg("Handling MissingEntry.")

	// Find if there exists a height for the missing entry seq no
	height, ok := hi.sn2height[msg.Sn]

	// If there exists no a height - and therefore no node for this seq no
	// Then simply announce the the entry
	if !ok {
		hi.announce(nil, msg.Sn, msg.Batch, msg.Digest, msg.Aborted)
	} else {
		// There must exist a node for this height
		node := hi.nodes[height]
		// If the node is not announced
		if !node.announced {
			node.announced = true
			if node.batch != nil {
				node.batch.Resurrect()
			}
			node.batch = request.NewBatch(msg.Batch)
			node.batch.MarkInFlight()
			node.digest = msg.Digest
			hi.announce(node, msg.Sn, msg.Batch, msg.Digest, msg.Aborted)
		}
	}
}

func (hi *hotStuffInstance) announce(node *hotStuffNode, sn int32, reqBatch *pb.Batch, digest []byte, aborted bool) {
	if node != nil {
		// Cancel timer
		if node.viewChangeTimer != nil {
			logger.Debug().
				Int("segment", hi.segment.SegID()).
				Int32("sn", node.sn).
				Int32("height", node.height).
				Msg("Cancelling timeout.")
			notFired := node.viewChangeTimer.Stop()
			if !notFired {
				// This is harmelss, since the timeout, even though generated, will be ignored.
				logger.Warn().Int32("sn", sn).Msg("Timer fired concurently with being canceled.")
			}
		}

		// We don't announce the root node
		if node.announced {
			return
		}

		// Mark node as announced
		node.announced = true

		// Remove batch requests
		request.RemoveBatch(node.batch)
	}

	logger.Info().
		Int("segment", hi.segment.SegID()).
		Int32("sn", hi.height2sn[node.height]).
		Int32("height", node.height).
		Msg("Announcement.")

	logEntry := &log.Entry{
		Sn:      sn,
		Batch:   reqBatch,
		Aborted: aborted,
		Digest:  digest,
	}

	// Announce decision
	announcer.Announce(logEntry)
}

// Creates a new node
func (hi *hotStuffInstance) newNode(parent *hotStuffNode, batch *request.Batch, qc *pb.HotStuffQC, sn int32, height int32, leader int32) *hotStuffNode {
	node := &pb.HotStuffNode{
		View:   hi.view,
		Parent: parent.digest,
		Batch:  batch.Message(),
		Certificate: &pb.HotStuffQC{
			Height:    qc.Height,
			Signature: qc.Signature,
		},
		Height: height,
	}
	new := &hotStuffNode{
		sn:     sn,
		height: height,
		leader: leader,
		node:   node,
		parent: parent,
		batch:  batch,
		digest: hotStuffDigest(node),
		votes:  make(map[int32]*pb.SignedMsg),
		dummy:  false,
	}
	if _, ok := hi.sn2height[sn]; ok {
		new.dummy = true
	}
	logger.Debug().
		Int("segment", hi.segment.SegID()).
		Int32("sn", sn).
		Int32("height", height).
		Int32("parent", parent.height).
		Msg("New node")
	return new
}

// Adds a new node into the log
func (hi *hotStuffInstance) addNode(node *hotStuffNode) {
	logger.Debug().
		Int("segment", hi.segment.SegID()).
		Int32("sn", node.sn).
		Int32("height", node.height).
		Msg("Adding new node.")
	if int32(len(hi.nodes)) <= node.height {
		for height := int32(len(hi.nodes)); height <= node.height; height++ {
			hi.nodes = append(hi.nodes, &hotStuffNode{height: height, sn: -1, dummy: true})
		}
	}

	if hi.nodes[node.height] != nil {
		if hi.nodes[node.height].node != nil {
			return
		}
	}

	hi.nodes[node.height] = node
	// Set timer for timeout
	if node.dummy {
		return
	}

	hi.setViewChangeTimer(node.sn, node)
}

func (hi *hotStuffInstance) updateHighQC(qc *pb.HotStuffQC) {
	if qc.Height > hi.highQC.Height {
		hi.highQC = qc
		if int32(len(hi.nodes)) < qc.Height {
			logger.Debug().
				Int32("heigth", qc.Node.Height).
				Msg("Adding dummy node")
			// If we dont' have the node of the highQC locally, create a new node from the highQC node
			// with parent the highest available node.
			batch := request.NewBatch(qc.Node.Batch)
			leaf := hi.newNode(hi.nodes[len(hi.nodes)-1], batch, qc.Node.Certificate, -1, qc.Node.Height, hi.leader)
			// Potentially adding dummy nodes inbetween.
			hi.addNode(leaf)
			hi.leaf = leaf
		} else {
			hi.leaf = hi.nodes[hi.highQC.Height]
		}
	}
}

// Return true if n1 extends n2
func extends(n1 *pb.HotStuffNode, n2 *pb.HotStuffNode) bool {
	return bytes.Compare(n1.Parent, hotStuffDigest(n2)) == 0

}

func hotStuffDigest(node *pb.HotStuffNode) []byte {
	return request.BatchDigest(node.Batch)
}

func voteQuorum(node *hotStuffNode) bool {
	// Check if enough unique prepare messages are received
	if len(node.votes) < 2*membership.Faults()+1 {
		return false
	}
	return true
}

func (hi *hotStuffInstance) setViewChangeTimer(sn int32, node *hotStuffNode) {
	if node.viewChangeTimer != nil {
		logger.Debug().
			Int("segment", hi.segment.SegID()).
			Int32("sn", sn).
			Int32("height", node.height).
			Msg("Timer already started.")
		return
	} else {
		logger.Debug().Int("segment", hi.segment.SegID()).
			Int32("sn", sn).
			Int32("height", node.height).
			Msg("Starting timer.")
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       node.sn, // SN -1 indicates that this is not a batch timeout, but a new view or checkpoint timeout.
		Msg: &pb.ProtocolMessage_Timeout{
			Timeout: &pb.Timeout{
				Sn:   node.sn,
				View: hi.view,
			}},
	}
	node.viewChangeTimer = time.AfterFunc(hi.viewChangeTimeout, func() { hi.serializer.serialize(msg) })
}

func (hi *hotStuffInstance) processSerializedMessages() {
	logger.Debug().
		Int("segment", hi.segment.SegID()).
		Msg("Starting serialized message processing.")
	for msg := range hi.serializer.channel {
		if msg == nil {
			return
		}
		switch m := msg.Msg.(type) {
		case *pb.ProtocolMessage_HotstuffNewview:
			logger.Debug().Int32("sn", msg.Sn).Int32("senderId", msg.SenderId).Msg("Received NEW VIEW")
			newview := m.HotstuffNewview
			err := hi.handleNewView(newview, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Int("segment", hi.segment.SegID()).
					Msg("HotStuffOrderer cannot handle new view message.")
			}
		case *pb.ProtocolMessage_Timeout:
			// If the timeout sequence number is -1 (this is the case for new view timeouts), there is no pbftBatch.
			// Otherwise, there is always a pbftBatch for every sequence number of the current view.
			if m.Timeout.View < hi.view || (m.Timeout.Sn != -1 && hi.nodes[hi.sn2height[m.Timeout.Sn]].announced) {
				// If the views in this debug message are the same, that means the request has been committed in the meantime.
				logger.Debug().
					Int32("sn", m.Timeout.Sn).
					Int("segment", hi.segment.SegID()).
					Int32("timeoutView", m.Timeout.View).
					Int32("currentView", hi.view).
					Msg("Ignoring outdated timeout.")
			} else {
				logger.Warn().
					Int("segment", hi.segment.SegID()).
					Int32("sn", msg.Sn).
					Int32("view", m.Timeout.View).
					Msg("Timeout")
				hi.sendNewView()
			}
		case *pb.ProtocolMessage_Proposal:
			logger.Debug().Int32("sn", msg.Sn).Int32("senderId", msg.SenderId).Msg("Received PROPOSAL")
			proposal := m.Proposal
			err := hi.handleProposal(proposal, msg)
			if err != nil {
				logger.Error().
					Err(err).
					Int("segment", hi.segment.SegID()).
					Msg("HotStuff orderer cannot handle proposal message.")
			}
		case *pb.ProtocolMessage_Vote:
			logger.Debug().Int32("sn", msg.Sn).Int32("senderId", msg.SenderId).Msg("Received VOTE")
			vote := m.Vote
			err := hi.handleVote(vote, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Int("segment", hi.segment.SegID()).
					Msg("HotStuff orderer cannot handle vote message.")
			}
		case *pb.ProtocolMessage_MissingEntry:
			hi.handleMissingEntry(m.MissingEntry)
		default:
			logger.Error().
				Str("msg", fmt.Sprint(m)).
				Int("segment", hi.segment.SegID()).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("HotSruff orderer cannot handle message. Unknown message type.")
		}
	}
}

func (hi *hotStuffInstance) subscribeToBacklog() {
	// Check for backloged backlogMsgs for this segment
	hi.orderer.backlog.subscribers <- backlogSubscriber{serializer: hi.serializer, segment: hi.segment}
}

func (hi *hotStuffInstance) stopProposing() {
	hi.stopProp.Do(func() {
		close(hi.newBatch)
	})
}

func (hi *hotStuffInstance) startView(view int32) {
	if hi.view > view {
		panic("Starting a view older than the current view")
	}

	tracing.MainTrace.Event(tracing.VIEW_CHANGE, int64(hi.segment.SegID()), int64(hi.view))

	hi.view = view

	hi.newViewVotes = make(map[int32]map[int32]*pb.HotStuffNewView)

	// In ISS, we only propose fresh batches in view 0
	if view > 0 {
		hi.stopProposing()
	}

}
