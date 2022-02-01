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

package request

import (
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/log"
	"github.com/hyperledger-labs/mirbft/util"
)

// Buffers Requests from a single client.
// Only requests within the watermark window can be added to the Buffer.
// The Buffer also maintains a backlog of requests with client sequence numbers higher than the watermark window.
// When the capacity of the backlog is exceeded, requests above the watermark window cannot be added.
// ATTENTION! While the Buffer can be locked, not all methods are thread-safe.
//            No two goroutines must access the buffer concurrently without proper synchronization.
type Buffer struct {
	// Any modification (or read of potentially concurrently modified values)
	// of the request buffer requires acquiring this lock.
	// (Using this syntax, one can use .Lock() and .Unlock() methods directly on the Buffer.)
	sync.RWMutex

	// ID of the client this buffer is associated to.
	ClientID int32

	// Start of the client watermark window (as seen locally by the replica).
	// All ClientRequests with SNs lower than LowWatermark were present in the past,
	// but are now removed from the buffer.
	// Only requests with sequence numbers between LowWatermark
	// and (LowWatermark + config.Config.ClientWatermarkWindowSize) can be added to the buffer.
	// Additionaly, up to config.Config.ClientRequestBacklogSize requests will be stored in the buffer's backlog
	// and added automatically when LowWatermark increases in Buffer.AdvanceWatermarks()
	LowWatermark int32

	// Map from client sequence numbers to boolean values indicating whether the corresponding request has been committed.
	// (All client sequence numbers below are also considered committed).
	requestsCommitted map[int32]bool

	// A backlog of size at least one client watermark window. (Needs to be configured in the config file.)
	// The client cannot be aware of when checkpoints are done so we backlog messages to process them
	// when the client watermarks advance.
	backlog *util.ChannelBuffer
	//backlog []*pb.ClientRequest
}

// Allocates and returns a new Buffer.
func NewBuffer(clientID int32) *Buffer {
	return &Buffer{
		ClientID:          clientID,
		LowWatermark:      0,
		requestsCommitted: make(map[int32]bool),
		backlog:           util.NewChannelBuffer(config.Config.ClientRequestBacklogSize),
	}
}

// Adds a ClientRequest req to the buffer, if req has never been added before.
// Does nothing if req has been added in the past, even if it has been removed in the meantime.
// If the request is added to the buffer, Add() adds the request to the corresponding bucket as well.
// If, after the call to Add(), the request is in the Buffer
// (either has just been added or has been in the buffer before),
// Add() returns a pointer to the stored Request struct of which req is a part.
// TODO: Update the comments when request verification is added to this method.
// If the request is not part of the buffer after the call to Add() (it has been ignored or backlogged),
// Add() returns nil.
// ATTENTION: The Add() method does not lock the buffer (as it is also called from another method that does).
//            Still, the Buffer must be locked when calling Add().
func (b *Buffer) Add(req *Request) bool {

	// Convenience variables
	clientSN := req.Msg.RequestId.ClientSn
	clientWatermarkWindowSize := int32(config.Config.ClientWatermarkWindowSize)

	// Request is ahead of the client watermark window.
	// Try backlogging it, if backlogging fails, ignore it.
	if clientSN >= b.LowWatermark+clientWatermarkWindowSize {

		// Print Warning.
		logger.Debug().
			Int32("lowWM", b.LowWatermark).
			Int32("windowSize", clientWatermarkWindowSize).
			Int32("clSn", clientSN).
			Int32("clId", req.Msg.RequestId.ClientId).
			Msg("Request sequence number ahead of client's watermark window.")

		// Try to add request to the buffer's backlog, if backlog is not yet full.
		// When advancing the watermarks, b.Add() will be called again for all requests in the backlog.
		b.backlog.Add(req)

		// Return nil, as request is not present in the buffer at this point.
		return false

		// Request is below the client watermark window.
		// Ignore it.
	} else if clientSN < b.LowWatermark {
		logger.Debug().
			Int32("lowWM", b.LowWatermark).
			Int32("windowSize", clientWatermarkWindowSize).
			Int32("clSn", clientSN).
			Int32("clId", req.Msg.RequestId.ClientId).
			Msg("Request sequence number below client's watermark window.")

		// Request not present in the bucket at this point.
		// Do not retry with a verified signature.
		return false

		// Request is within the current watermark window.
		// Try adding it to the bucket.
	} else {
		return true
	}
}

// Processes log entries for advancing the client watermark.
// Tries to add requests from the backlog back to the buffer
// (since some of them might be now in the watermark window).
// Returns the old and the new watermark.
func (b *Buffer) AdvanceWatermarks(entries []interface{}) watermarkRange { // expected type of entries: []*log.Entry

	// We need to lock the buffer to prevent concurrent incoming requests to observe inconsistent watermarks.
	b.Lock()
	defer b.Unlock()

	// Set the "committed" flag for all the requests among the committed entries that belong to this client buffer.
	for _, entry := range entries {
		for _, req := range entry.(*log.Entry).Batch.Requests {
			if req.RequestId.ClientId == b.ClientID {
				b.requestsCommitted[req.RequestId.ClientSn] = true
			}
		}
	}

	// Save old watermark for returning it later.
	oldWM := b.LowWatermark

	// Advance the watermark as far as possible:
	// Read out the next position of the buffer.
	// If it is set to true, it means that a request has been committed.
	// Garbage collect the position (by incrementing the low watermark)
	// and try the next position.
	for committed := b.requestsCommitted[b.LowWatermark]; committed; committed = b.requestsCommitted[b.LowWatermark] {
		delete(b.requestsCommitted, b.LowWatermark)
		b.LowWatermark++
	}

	// Try to re-add backlogged requests in their buckets
	// This needs to be done in a separate goroutine, because the buffer is locked at this point
	// and adding needs to acquire the lock.
	// In addition to preventing a deadlock, as potentially signatures need to be verified during adding,
	// we avoid holding on to the lock during verification.
	// b.backlog.Get() is called before starting the goroutine (i.e. while the buffer is still locked)
	// to prevent unnecessary handling of concurrently backlogged requests.
	go b.processBacklog(b.backlog.Get())

	logger.Debug().
		Int32("clientId", b.ClientID).
		Int("numEntries", len(entries)).
		Int32("oldWM", oldWM).
		Int32("newWM", b.LowWatermark).
		Int32("wmDelta", b.LowWatermark-oldWM).
		Msg("Advanced watermark window.")

	return watermarkRange{
		oldWM: oldWM,
		newWM: b.LowWatermark,
	}
}

// Tries to add all requests from the backlog to the Buffer.
// (Some requests might end up in the backlog again.)
func (b *Buffer) processBacklog(requests []interface{}) {

	for _, req := range requests {
		Add(req.(*Request))
	}

}
