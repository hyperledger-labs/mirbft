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
	"fmt"
	"sync"
	"time"

	"math/rand"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/announcer"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	"github.com/hyperledger-labs/mirbft/profiling"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/tracing"
)

const candidate string = "candidate"
const follower string = "follower"
const leader string = "leader"

type raftBatch struct {
	sn        int32
	request   *pb.RaftAppendEntryRequest
	batch     *request.Batch
	committed bool
}

// Represents a Raft instance implementation.
// Raft instance is responsible for ordering sequence numbers from a single segment
type raftInstance struct {
	// The segment of the instance
	segment manager.Segment

	// The raft orderer
	orderer *RaftOrderer

	// This is the term this Raft server is currently in
	term int32

	// This is the Raft peer that this server has voted for in *this* term (if any)
	votedFor int32

	// Raft instance log: Raft entries per index
	log map[int32]*raftBatch

	// Last log index
	last int32

	// Last log index + 1
	next int32

	// Next sequnce number index
	nextSnIndex int32

	// The state this server is currently in, can be follower, candidate, or leader
	status string

	// Index of the highest log entry *known* to be committed.
	// meaning they will not change, and can safely be applied to the state machine.
	commitIndex int32

	// Term of the highest log entry *known* to be committed.
	commitTerm int32

	// Index of the highest log entry applied to the state machine (announced).
	lastApplied int32

	// nextIndex is a guess as to how much of our log (as leader) matches that of
	// each other peer. This is used to determine what entries to send to each peer next.
	nextIndex map[int32]int32

	// matchIndex is a measurement of how much of our log (as leader) we know to be
	// replicated at each other server. This is used to determine when some prefix
	// of entries in our log from the current term has been replicated to a
	// majority of servers, and is thus safe to apply.
	matchIndex map[int32]int32

	// Timer to start a new election
	electionTimer *time.Timer

	// Positive votes in the current term. If the number of votes exceeds majority the node becomes
	// leader for the current term. The key is the ID of the voter.
	votes map[int32]bool

	// Minimum election timeout duration. Each node sets the election timeout randomly
	// in [minElectionTimeout, 2*minElectionTimeout]. The timeout is reset each time the leader
	// sends a heartbeat.
	minElectionTimeout time.Duration

	sn2index map[int32]int32 // Segment seq no to index

	// Message handling related fields
	stepDown   chan bool       // If a leader reads a value from this channel, they have step down
	serializer *ordererChannel // Channel of common case backlogMsgs
	lock       sync.Mutex
	stopProp   sync.Once
	cutBatch   chan struct{} // Channel for synchronizing batch cutting

	// State recovery related fiels
	announced map[int32]bool // Keeps track of the announced sequence numbers to avoid announcing them twice
}

// We start index from 0 (In Raft paper they start form 1)
func (ri *raftInstance) init(seg manager.Segment, orderer *RaftOrderer) {
	logger.Info().Int("segment", seg.SegID()).
		Int32("first", seg.FirstSN()).
		Msg("Starting Raft instance.")

	ri.segment = seg
	ri.orderer = orderer
	ri.nextSnIndex = 0

	// Initialize protocol instance
	ri.status = follower
	if seg.Leaders()[0] == membership.OwnID {
		ri.status = leader
	}
	ri.term = 0
	ri.votedFor = seg.Leaders()[0]
	ri.votes = make(map[int32]bool)

	ri.log = make(map[int32]*raftBatch)
	ri.last = -1
	ri.next = 0
	ri.commitIndex = -1
	ri.commitTerm = -1
	ri.lastApplied = -1

	ri.nextIndex = make(map[int32]int32)
	ri.matchIndex = make(map[int32]int32)
	for _, id := range seg.Followers() {
		ri.nextIndex[id] = 0
		ri.matchIndex[id] = -1
	}

	ri.minElectionTimeout = config.Config.ViewChangeTimeout

	ri.sn2index = make(map[int32]int32)
	ri.announced =  make(map[int32]bool)

	// Initalize channel
	ri.serializer = newOrdererChannel(channelSize)
}

// FIXME Raft cannot terminate as it is because the leader who commits first all the sequence numbers of the segments
// FIXME kills the segment and does not ensure his followers commit as well.
// We slightly deviate from the raft protocol here.
// The first leader is fixed and is the leader of the segment.
// This is to have each segment starts with a different leader.
func (ri *raftInstance) start() {
	// If this instance leads the segment
	if ri.status == leader {
		// Wait until the log progressed far enough
		logger.Debug().
			Int("segID", ri.segment.SegID()).
			Int32("after", ri.segment.StartsAfter()).
			Msg("Waiting for log entry.")
		log.WaitForEntry(ri.segment.StartsAfter())
		logger.Debug().Int("segID", ri.segment.SegID()).Msg("Leading segment.")
		go ri.heartbeat()
	}
}

// Sent a heartbeat as long as you are leader
// If you have available sequence numbers and you are in the first term send a new batch with the heartbeat
func (ri *raftInstance) heartbeat() {
	logger.Info().Int("segID", ri.segment.SegID()).Msg("Stating  heartbeat.")
	for {
		if ri.status != leader {
			return
		}
		logger.Debug().Msg("Waiting for heartbeat timeout.")
		time.Sleep(config.Config.BatchTimeout)
		logger.Debug().Msg("Heartbeat timeout expired.")
		select {
		case <-ri.stepDown:
			logger.Info().Msg("Stopping heartbeat.")
			return
		default:
			// Create a message
			msg := &pb.ProtocolMessage{
				SenderId: membership.OwnID,
				Sn:       ri.segment.LastSN(), // any sequence number of the segment, needed for dispatching
			}

			// Initialize an empty request
			request := &pb.RaftAppendEntryRequest{
				Term:      ri.term,
				Index:     -1,
				PrevIndex: -1,
				PrevTerm:  -1,
			}

			// If there are available sequence numbers
			if ri.nextSnIndex < ri.segment.Len() {
				// Assign the specific sequence number
				sn := ri.segment.SNs()[ri.nextSnIndex]
				msg.Sn = sn

				// Cut immediately a batch
				batch := ri.segment.Buckets().CutBatch(config.Config.BatchSize, 0)
				logger.Info().
					Int32("sn", sn).
					Int("segment", ri.segment.SegID()).
					Int("requests", len(batch.Requests)).
					Msg("Cutting new batch")
				if config.Config.SignRequests {
					// TODO: Do something useful with the result of signature verification
					if err := batch.CheckSignatures(); err != nil {
						logger.Fatal().Msg("Signature verification of freshly cat Batch failed.")
					}
				}
				batch.MarkInFlight()
				request.Batch = batch.Message()

				// Increase next sequence number index
				ri.nextSnIndex++
			}

			msg.Msg = &pb.ProtocolMessage_RaftNewseqno{RaftNewseqno: request}
			// Schedule the heartbeat
			ri.serializer.serialize(msg)
		}
	}
}

func (ri *raftInstance) subscribeToBacklog() {
	// Check for backloged messages for this segment
	ri.orderer.backlog.subscribers <- backlogSubscriber{segment: ri.segment, serializer: ri.serializer}
}

func (ri *raftInstance) processSerializedMessages() {
	logger.Info().
		Int("segID", ri.segment.SegID()).
		Msg("Starting serialized message processing.")

	for msg := range ri.serializer.channel {
		// To make sure noone writes anymore on closing the segment we write a special value (nil)
		if msg == nil {
			ri.stopProposing()
		}

		switch m := msg.Msg.(type) {
		case *pb.ProtocolMessage_Timeout:
			logger.Warn().
				Int32("index", ri.last).
				Int("segment", ri.segment.SegID()).
				Msg("Election timeout.")
			ri.newTerm()
		case *pb.ProtocolMessage_RaftNewseqno:
			req := m.RaftNewseqno
			logger.Debug().
				Int32("leader", membership.OwnID).
				Msg("Heartbeat.")
			ri.SendAppendEntryRequest(req, msg.Sn)
		case *pb.ProtocolMessage_RaftVoteRequest:
			req := m.RaftVoteRequest
			err := ri.HandleVoteRequest(req, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Msg("Raft orderer cannot handle vote request message.")
			}
		case *pb.ProtocolMessage_RaftVoteResponse:
			resp := m.RaftVoteResponse
			err := ri.HandleVoteResponse(resp, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Msg("Raft orderer cannot handle vote response message.")
			}
		case *pb.ProtocolMessage_RaftAppendEntryRequest:
			req := m.RaftAppendEntryRequest
			err := ri.HandleAppendEntryRequest(req, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Msg("Raft orderer cannot handle append entry request message.")
			}
		case *pb.ProtocolMessage_RaftAppendEntryResponse:
			resp := m.RaftAppendEntryResponse
			err := ri.HandleAppendEntryResponse(resp, msg.Sn, msg.SenderId)
			if err != nil {
				logger.Error().
					Err(err).
					Msg("Raft orderer cannot handle append entry resp message.")
			}
		case *pb.ProtocolMessage_MissingEntry:
			ri.handleMissingEntry(m.MissingEntry)
		default:
			logger.Error().
				Str("msg", fmt.Sprint(m)).
				Int32("sn", msg.Sn).
				Int32("senderID", msg.SenderId).
				Msg("Raft cannot handle message. Unknown message type.")
		}
	}
}

func (ri *raftInstance) sendVoteRequest(candidate int32, sn int32) {
	// Create a vote message
	voteRequest := &pb.RaftVoteRequest{
		CandidateId: candidate,
	}

	msg := &pb.ProtocolMessage{
		Sn:       sn,
		SenderId: membership.OwnID,
		Msg: &pb.ProtocolMessage_RaftVoteRequest{
			RaftVoteRequest: voteRequest,
		},
	}

	// Enqueue the message for all followers
	for _, nodeID := range ri.segment.Followers() {
		messenger.EnqueueMsg(msg, nodeID)
	}
}

func (ri *raftInstance) HandleVoteRequest(req *pb.RaftVoteRequest, sn, senderID int32) error {
	logger.Info().
		Int32("term", ri.term).
		Int32("senderID", senderID).
		Msg("Handling Vote Request")

	// Initialize response
	resp := &pb.RaftVoteResponse{
		Term:        ri.term,
		CandidateId: senderID,
		VoteGranted: true,
	}

	// Step down if you find out about a higher term
	if req.Term > ri.term {
		ri.term = req.Term
		ri.votedFor = -1
		ri.status = follower
		ri.stopProposing()
	}

	// If your term is higher than candidate's term, don't grant vote
	if req.Term < ri.term {
		resp.VoteGranted = false
		resp.Term = ri.term
		ri.SendVoteResponse(resp, sn, senderID)
		return nil
	}

	// If you have already voted for someone who is not this candidate, don't grant vote
	if ri.votedFor >= 0 && ri.votedFor != senderID {
		resp.VoteGranted = false
		ri.SendVoteResponse(resp, sn, senderID)
		return nil
	}

	// If candidate's log is not as up-to-date as local log, don't grant vote
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	if req.LastTerm < ri.commitTerm {
		resp.VoteGranted = false
		ri.SendVoteResponse(resp, sn, senderID)
		return nil
	}
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	if req.LastTerm == ri.commitTerm && req.LastSn < ri.commitIndex {
		resp.VoteGranted = false
		ri.SendVoteResponse(resp, sn, senderID)
		return nil
	}

	// If you grant vote update your state and reset the election timeout
	ri.votedFor = senderID
	timeout := electionTimeout(ri.minElectionTimeout)
	ri.setElectionTimer(timeout)

	// Send response to the sender
	ri.SendVoteResponse(resp, sn, senderID)
	return nil
}

func (ri *raftInstance) SendVoteResponse(resp *pb.RaftVoteResponse, sn, nodeID int32) {
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_RaftVoteResponse{
			RaftVoteResponse: resp,
		},
	}

	messenger.EnqueueMsg(msg, nodeID)
}

func (ri *raftInstance) HandleVoteResponse(resp *pb.RaftVoteResponse, sn, senderID int32) error {
	logger.Info().
		Int32("term", ri.term).
		Int32("senderID", senderID).
		Msg("Handling Vote Response")

	// Ignore messages from precious terms
	if resp.Term < ri.term {
		return fmt.Errorf("request from %d from old term %d - we are now at %d", senderID, resp.Term, ri.term)
	}

	// Ignore message if you are not a candidate
	if ri.status != candidate {
		return fmt.Errorf("ignoring vote from , not a candidate")
	}

	// Update your term if the voters term is higher and step down
	if resp.Term > ri.term {
		ri.term = resp.Term
		ri.status = follower
		ri.votedFor = 0
		logger.Info().
			Int32("senderID", senderID).
			Int32("term", ri.term).
			Msg("Stepping down to follower. Higher term found")
		ri.stepDown <- true
		return nil
	}

	// Check if vote is granted
	if !resp.VoteGranted {
		logger.Info().
			Int32("senderID", senderID).
			Int32("term", ri.term).
			Msg("Vote not granted")
		return nil
	}

	// Make sure you don't double count
	if _, ok := ri.votes[senderID]; ok {
		return fmt.Errorf("vote from %")
	}

	// Record vote
	ri.votes[senderID] = true

	// Check for majority
	if len(ri.votes) > 2*membership.Faults() {
		ri.status = leader
		ri.votedFor = 0
		for _, id := range ri.segment.Followers() {
			ri.nextIndex[id] = ri.next
			ri.matchIndex[id] = 0
		}
	}

	// Send AppendEntryRequest heartbeat
	go ri.heartbeat()

	return nil
}

func (ri *raftInstance) SendAppendEntryRequest(req *pb.RaftAppendEntryRequest, sn int32) {
	// Only send AppendEntryRequest if you are leader
	if ri.status != leader {
		return
	}

	if len(ri.log) > 0 {
		req.PrevIndex = ri.log[ri.last].request.Index
		req.PrevTerm = ri.log[ri.last].request.Term
	}

	req.LeaderCommit = ri.commitIndex

	if req.Batch != nil {
		req.Index = ri.next

		// TODO: This is unnecessary re-adding of the requests in the batch. Pass the Batch directly!
		//       (As this batch is locally produced by CutBatch, then the batch message is extracted, and then converted to a Batch again.)
		// Update own log
		newEntry := &raftBatch{
			sn:      sn,
			request: req,
			batch:   request.NewBatch(req.Batch),
		}

		ri.sn2index[sn] = req.Index
		ri.log[req.Index] = newEntry
		ri.last = ri.next
		ri.next = ri.last + 1

		logger.Debug().
			Int32("next", ri.next).
			Int32("last", ri.last).
			Msg("Updated leader state")

		tracing.MainTrace.Event(tracing.PROPOSE, int64(sn), int64(len(req.Batch.Requests)))
	}

	// Enqueue new log entry and potentially uncommitted entries to each follower
	for _, nodeID := range ri.segment.Followers() {
		if nodeID == membership.OwnID {
			continue
		}
		for i := ri.nextIndex[nodeID]; i < ri.last; i++ {
			if i < 0 {
				continue
			}
			entry := ri.log[i]
			msg := &pb.ProtocolMessage{
				SenderId: membership.OwnID,
				Sn:       entry.sn,
				Msg: &pb.ProtocolMessage_RaftAppendEntryRequest{
					RaftAppendEntryRequest: &pb.RaftAppendEntryRequest{
						Index:        entry.request.Index,
						Term:         ri.term,
						PrevIndex:    entry.request.PrevIndex,
						PrevTerm:     req.PrevTerm,
						LeaderCommit: req.LeaderCommit,
						Batch:        entry.request.Batch,
					},
				},
			}
			logger.Debug().
				Int32("sn", entry.sn).
				Int32("term", ri.term).
				Int32("prevIndex", entry.request.PrevIndex).
				Int32("index", entry.request.Index).
				Int32("leaderCommit", req.LeaderCommit).
				Int32("followerID", nodeID).
				Msg("Sending Append Entry Request")

			messenger.EnqueueMsg(msg, nodeID)
		}
	}

	// Enqueue the heartbeat to all followers (including if available the latest entry)
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_RaftAppendEntryRequest{
			RaftAppendEntryRequest: req,
		},
	}

	for _, nodeID := range ri.segment.Followers() {
		if nodeID == membership.OwnID {
			continue
		}
		logger.Debug().
			Int32("sn", sn).
			Int32("term", req.Term).
			Int32("prevIndex", req.PrevIndex).
			Int32("index", req.Index).
			Int32("leaderCommit", req.LeaderCommit).
			Int32("followerID", nodeID).
			Msg("Sending Append Entry Request")
		messenger.EnqueueMsg(msg, nodeID)
	}
}

func (ri *raftInstance) HandleAppendEntryRequest(req *pb.RaftAppendEntryRequest, sn, senderID int32) error {
	logger.Info().
		Int32("term", ri.term).
		Int32("sn", sn).
		Int32("index", req.Index).
		Int32("senderID", senderID).
		Msg("Handling Append Entry Request")

	// Step down if you find about a higher term
	if req.Term > ri.term {
		ri.term = req.Term
		ri.status = follower
		ri.votedFor = -1
		logger.Info().
			Int32("term", ri.term).
			Int32("senderID", senderID).
			Msg("New term found. Stepping down to follower")
		ri.stepDown <- true
	}

	// Step down if you find about a new leader
	if ri.status == candidate {
		ri.status = follower
		logger.Info().
			Int32("term", ri.term).
			Int32("senderID", senderID).
			Msg("New leader found. Stepping down to follower")
		ri.stopProposing()
	}

	// Reset election timeout
	timeout := electionTimeout(ri.minElectionTimeout)
	ri.setElectionTimer(timeout)

	// Initialize response
	resp := &pb.RaftAppendEntryResponse{
		Index:     req.Index,
		Term:      req.Term,
		Success:   true,
		NextIndex: ri.next,
	}

	// Unsuccessful if your term is higher
	if req.Term < ri.term {
		resp.Term = ri.term
		resp.Success = false
		logger.Warn().
			Int32("term", ri.term).
			Int32("senderID", senderID).
			Int32("index", resp.Index).
			Bool("success", resp.Success).
			Msg("Local term is higher")
		ri.SendAppendEntryResponse(resp, sn, senderID)
		return nil
	}

	// Unsuccessful if your log does not contain any entry for the previous log entry of the leader
	// Or if the entry for the previous log entry of the leader is from a different term
	prev := ri.log[req.PrevIndex]
	if (prev == nil && req.PrevIndex >= 0) || (prev != nil && prev.request.Term != req.PrevTerm) {
		resp.Success = false
		resp.NextIndex = ri.next
		logger.Warn().
			Int32("term", ri.term).
			Int32("senderID", senderID).
			Int32("index", resp.Index).
			Int32("prevIndex", req.PrevIndex).
			Bool("success", resp.Success).
			Msg("Leader's previous log entry not found")
		ri.SendAppendEntryResponse(resp, sn, senderID)
		return nil
	}

	// If the request has conflict (same index and different term)
	// delete the existing entry and all that follow it
	if entry, ok := ri.log[req.Index]; ok {
		if entry.request.Term != req.Term {
			for i := req.Index; i <= ri.last; i++ {
				delete(ri.log, i)
			}
			ri.last = req.Index - 1
		}
	}

	if req.Batch != nil {
		// If there exists no previous entry for this index
		if _, ok := ri.log[req.Index]; !ok  {
			// Sanity checks - we don't need to check for Byzantine behavior with Raft.
			// Check that proposal requests are valid
			batch := request.NewBatch(req.Batch)
			if batch == nil {
				return fmt.Errorf("proposal from %d contains invalid requests", senderID)
			}
			// Check that proposal does not contain preprepared ("in flight") requests.
			//if err := batch.CheckInFlight(); err != nil {
			//	return fmt.Errorf("proposal from %d contains in flight requests: %s", senderID, err.Error())
			//}
			// Check that proposal does not contain requests that do not match the current active bucket
			//if err := batch.CheckBucket(ri.segment.Buckets().GetBucketIDs()); err != nil {
			//	return fmt.Errorf("proposal from %d contains in requests from invalid bucket: %s", senderID, err.Error())
			//}
			// Mark requests as preprepared
			//batch.MarkInFlight()

			// Create a new entry from the request
			// If there exist already an entry replace it
			newEntry := &raftBatch{
				sn:      sn,
				request: req,
				batch:   batch,
			}
			ri.log[req.Index] = newEntry
		}

		// If the request is advancing the log
		if req.Index > ri.last {
			ri.last = req.Index
			ri.next = ri.last + 1
		}
		resp.Index = ri.last
		resp.NextIndex = ri.next
	}

	// Update commit index if the highest locally committed value is lower than leader's highest committed value
	logger.Debug().
		Int32("term", ri.term).
		Int32("lastApplied", ri.lastApplied).
		Int32("commitIndex", ri.commitIndex).
		Int32("leaderCommit", req.LeaderCommit).
		Int32("lastNewEntry", ri.last).
		Msg("Maybe update commit index")
	if req.LeaderCommit > ri.commitIndex {
		ri.commitIndex = min(req.LeaderCommit, ri.last)
		logger.Info().
			Int("segment", ri.segment.SegID()).
			Int32("index", ri.commitIndex).
			Msg("Committed.")
		ri.maybeAnnounce()
	}

	ri.SendAppendEntryResponse(resp, sn, senderID)
	return nil
}

func (ri *raftInstance) SendAppendEntryResponse(resp *pb.RaftAppendEntryResponse, sn, nodeID int32) {
	logger.Debug().
		Int32("term", ri.term).
		Int32("index", resp.Index).
		Int32("sn", sn).
		Int32("nextIndex", resp.NextIndex).
		Bool("success", resp.Success).
		Msg("Sending Append Entry Response")

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_RaftAppendEntryResponse{
			RaftAppendEntryResponse: resp,
		},
	}
	messenger.EnqueueMsg(msg, nodeID)
}

func (ri *raftInstance) HandleAppendEntryResponse(resp *pb.RaftAppendEntryResponse, sn, senderID int32) error {
	logger.Info().
		Int32("term", ri.term).
		Int32("sn", sn).
		Int32("senderID", senderID).
		Int32("index", resp.Index).
		Bool("success", resp.Success).
		Msg("Handling Append Entry Response")

	// Ignore messages from precious terms
	if resp.Term < ri.term {
		return fmt.Errorf("request from old term %d - we are now at %d", resp.Term, ri.term)
	}

	// Step down if you find about a higher term
	if resp.Term > ri.term {
		ri.term = resp.Term
		ri.status = follower
		ri.votedFor = -1
		logger.Info().
			Int32("term", ri.term).
			Int32("senderID", senderID).
			Msg("New term found. Stepping down to follower")
		ri.stepDown <- true
	}

	if resp.Success {
		// Update nextIndex and matchIndex for follower
		ri.nextIndex[senderID] = resp.NextIndex
		ri.matchIndex[senderID] = resp.Index
		ri.maybeCommit()
		ri.maybeAnnounce()
		return nil
	}

	// If response indicates inconsistency
	// decrease nextIndex for the follower
	ri.nextIndex[senderID]--

	return nil
}

func (ri *raftInstance) handleMissingEntry(msg *pb.MissingEntry) {
	logger.Info().
		Int("segment", ri.segment.SegID()).
		Int32("view", ri.term).
		Int32("sn", msg.Sn).
		Msg("Handling MissingEntry.")

	// If the sequence number is announced do nothing
	if ri.announced[msg.Sn] {
		return
	}

	// Find if there exists a height for the missing entry seq no
	// Resurrect the existing batch
	if index, ok := ri.sn2index[msg.Sn]; ok {
		entry := ri.log[index]
		if entry.batch != nil {
			entry.batch.Resurrect()
		}
	}

	// Add the new batch
	batch := request.NewBatch(msg.Batch)
	batch.MarkInFlight()

	// And announce it
	request.RemoveBatch(batch)
	announcer.Announce(&log.Entry{Sn: msg.Sn, Batch: batch.Message()})
	ri.announced[msg.Sn] = true
}

func (ri *raftInstance) maybeCommit() {
	toCommit := make(map[int32]int)
	for _, index := range ri.matchIndex {
		if index > ri.commitIndex {
			if _, ok := toCommit[index]; !ok {
				toCommit[index] = 0
			}
			toCommit[index]++
		}
	}

	for index, count := range toCommit {
		if count < 2*membership.Faults() {
			continue
		}
		if index > ri.commitIndex {
			ri.commitIndex = index
			logger.Info().Int("segment", ri.segment.SegID()).Int32("index", ri.commitIndex).Msg("Committed.")
		}
	}
}

// In order announcement
// If you have committed an entry higher than the one you last announced (applied)
// then announce the next one.
func (ri *raftInstance) maybeAnnounce() {
	if ri.commitIndex <= ri.lastApplied {
		return
	}

	ri.lastApplied++
	index := ri.lastApplied
	ri.announced[ri.log[index].sn] = true

	logger.Info().Int32("sn", ri.log[index].sn).
		Int("segment", ri.segment.SegID()).
		Int32("index", index).
		Msg("Announcement.")

	// Remove batch requests
	request.RemoveBatch(ri.log[index].batch)
	announcer.Announce(&log.Entry{Sn: ri.log[index].sn, Batch: ri.log[index].batch.Message()})

	ri.maybeAnnounce()
}

func (ri *raftInstance) newTerm() {
	if config.Config.DisabledViewChange {
		profiling.StopProfiler()
		tracing.MainTrace.Stop()
		logger.Fatal().Int("segID", ri.segment.SegID()).Msg("VIEWCHANGE disabled, peer exits.")
	}
	tracing.MainTrace.Event(tracing.VIEW_CHANGE, int64(ri.segment.SegID()), int64(ri.term))

	// Advance term (view) and reset vote counting
	ri.status = candidate
	ri.votes = make(map[int32]bool)
	ri.term++

	logger.Info().
		Int32("term", ri.term).
		Msg("New term")

	// Start timer for the next term with double timeout
	timeout := electionTimeout(ri.minElectionTimeout)
	ri.setElectionTimer(timeout)

	// Vote for yourself
	ri.sendVoteRequest(membership.OwnID, ri.segment.LastSN())
}

// Raft implementation has a single timeout for each term (view)
// The sequence number in the timeout message has no particular semantics
// It just needs to be a sequence number of the segment for correct message handling.
func (ri *raftInstance) setElectionTimer(timeout time.Duration) {
	if ri.electionTimer != nil {
		logger.Debug().Msg("Cancelling election timeout.")
		ri.electionTimer.Stop()
	}

	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       ri.segment.FirstSN(), // any sequence number of the segment
		Msg: &pb.ProtocolMessage_Timeout{
			Timeout: &pb.Timeout{},
		},
	}

	ri.electionTimer =
		time.AfterFunc(timeout, func() {
			logger.Debug().Int32("index", ri.last).Msg("Serializing election timeout.")
			ri.serializer.serialize(msg)
		})
}

// returns a random timeout between min and 2*min
func electionTimeout(min time.Duration) time.Duration {
	offset := min.Nanoseconds()
	randDuration := rand.Int63n(offset)
	totalDuration := randDuration + offset
	logger.Debug().Int64("ns", totalDuration).Msg("Randomized timeout")
	return time.Duration(totalDuration) * time.Nanosecond
}

func (ri *raftInstance) stopProposing() {
	ri.stopProp.Do(func() {
		ri.status = follower
		ri.stepDown <- true
	})
}
