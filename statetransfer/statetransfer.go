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

package statetransfer

import (
	"fmt"
	"math/rand"
	"time"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

const (
	startDelay                = 500 * time.Millisecond
	receivedEntriesBufferSize = 4096
	entryFetchInterval        = 500 * time.Millisecond
)

type missingEntry struct {
	Sn int32
}

var (
	OrdererEntryHandler func(*log.Entry) = nil

	missingEntries    = make(map[int32]*missingEntry)
	newMissingEntries = make(chan *missingEntry)
	receivedEntries   = make(chan *pb.MissingEntry, receivedEntriesBufferSize)
)

func Init() {
	go processMissingEntries()
}

func HandleMessage(msg *pb.ProtocolMessage) {
	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_MissingEntryReq:
		handleRequest(m.MissingEntryReq, msg.SenderId)
	case *pb.ProtocolMessage_MissingEntry:
		receivedEntries <- m.MissingEntry
	}
}

func CatchUp(checkpoint *pb.StableCheckpoint) {
	// Give the protocol some time to acquire the entries normally.
	time.Sleep(startDelay)

	sources := make([]int32, len(checkpoint.Proof))
	for peerID, _ := range checkpoint.Proof {
		sources = append(sources, peerID)
	}

	// Ask for each missing entry in parallel.
	for _, sn := range log.Missing(checkpoint.Sn) {
		go FetchMissingEntry(sn, sources)
	}
}

func FetchMissingEntry(sn int32, sources []int32) {

	// Create a new missing entry data structure.
	newMissingEntries <- &missingEntry{
		Sn: sn,
		// TODO: Add all data needed to verify the proof carried by responses.
	}

	// Create a copy of the list of sources and randomize their order.
	shuffledSources := make([]int32, len(sources), len(sources))
	copy(shuffledSources, sources)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuffledSources), func(i, j int) {
		shuffledSources[i], shuffledSources[j] = shuffledSources[j], shuffledSources[i]
	})

	// TODO: The rest of this function must be executed by the serial processing thread.
	//       With the current implementation, it is theoretically possible that the response will be processed
	//       before the missingEntry object.

	logger.Info().
		Int32("sn", sn).
		Int("nSources", len(shuffledSources)).
		Int32("firstSource", shuffledSources[0]).
		Msg("Fetching missing entry.")

	// Create new request message
	msg := &pb.ProtocolMessage{
		SenderId: membership.OwnID,
		Sn:       sn,
		Msg: &pb.ProtocolMessage_MissingEntryReq{MissingEntryReq: &pb.MissingEntryRequest{
			Sn:             sn,
			PayloadRequest: true,
		}},
	}

	// Keep sending entry request messages until the entry appears in the log.
	sIndex := 0
	delay := entryFetchInterval
	for log.GetEntry(sn) == nil {

		logger.Debug().
			Int32("sn", sn).
			Int32("peerID", shuffledSources[sIndex]).
			Msg("Requesting missing entry.")

		messenger.EnqueueMsg(msg, shuffledSources[sIndex])

		time.Sleep(delay)
		delay *= 2
	}
}

// This is the only thread that manipulates the data structures of this package.
// It inserts new missing entries and handles responses to missing entry requests.
func processMissingEntries() {
	// TODO: implement graceful shutdown (by closing the channels).

	// Loop processing data received over channels.
	// Exits when a channel is closed.
	for {
		select {
		case missingEntry, ok := <-newMissingEntries:
			if !ok {
				break
			}
			missingEntries[missingEntry.Sn] = missingEntry
		case resp, ok := <-receivedEntries:
			if !ok {
				break
			}
			if err := processResponse(resp); err != nil {
				logger.Error().Err(err).Int32("sn", resp.Sn).Msg("Invalid response to missing entry request.")
			}
		}
	}
}

func processResponse(resp *pb.MissingEntry) error {

	me, ok := missingEntries[resp.Sn]

	// If there is no missing entry corresponding to this response, we already obtained one earlier and ignore this one.
	if !ok {
		return nil
	}

	// Verify obtained response with respect to the corresponding checkpoint.
	if err := verifyResponse(resp, me); err != nil {
		return fmt.Errorf("Invalid response to missing entry request: %v", err)
	}

	// Clean up to prevent handling duplicate responses
	delete(missingEntries, resp.Sn)

	// Create a new entry object
	entry := &log.Entry{
		Sn:        resp.Sn,
		Batch:     resp.Batch,
		Digest:    resp.Digest,
		Aborted:   resp.Aborted,
		Suspect:   resp.Suspect,
		ProposeTs: 0,
		CommitTs:  time.Now().UnixNano(),
	}

	logger.Info().Int32("sn", entry.Sn).Msg("Fetched missing entry.")

	// Process the new entry through the orderer instead of directly inserting it in the log.
	// This gives protocol executed by the orderer a chance to react to this event.
	OrdererEntryHandler(entry)
	return nil
}

func verifyResponse(resp *pb.MissingEntry, me *missingEntry) error {
	// TODO: This is a STUB. Implement proper checkpoint proofs and verify responses.

	return nil
}

func handleRequest(req *pb.MissingEntryRequest, senderID int32) {

	// In this simple implementation, we send the entry if we have it, otherwise we ignore the request.
	if entry := log.GetEntry(req.Sn); entry != nil {
		msg := &pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       entry.Sn,
			Msg: &pb.ProtocolMessage_MissingEntry{MissingEntry: &pb.MissingEntry{
				Sn:      entry.Sn,
				Digest:  entry.Digest,
				Aborted: entry.Aborted,
				Suspect: entry.Suspect,
				Proof:   "Dummy Proof", // TODO: Use an actual proof.
			}},
		}

		// Only append batch data if payload is requested
		if req.PayloadRequest {
			msg.Msg.(*pb.ProtocolMessage_MissingEntry).MissingEntry.Batch = entry.Batch
		}

		logger.Info().Int32("sn", req.Sn).Int32("peerID", senderID).Msg("Sending missing entry.")
		messenger.EnqueueMsg(msg, senderID)
	} else {
		logger.Debug().Int32("sn", req.Sn).Int32("peerID", senderID).Msg("Ignoring missing entry request.")
	}
}
