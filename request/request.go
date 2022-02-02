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
	"encoding/binary"
	"sync"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

var (
	// Uncommitted requests received from clients, organized in buffers indexed by client ID.
	// For each known client, this map (indexed by client ID) contains a buffer of requests from that client.
	// This data structure is necessary to filter out ClientRequests that are outside of the client watermark window.
	// It buffers up to config.Config.ClientRequestBacklogSize client requests ahead of the watermark window.
	buffers = make(map[int32]*Buffer)

	// Lock to guard the map of request Buffers.
	// For every incoming request we need to look up (by client ID) to which Buffer to put the request.
	// TODO: This lock is on the critical path all requests!
	//       Even though it is almost only RLocked, this can cause cache contention among threads that access it.
	//       With many threads, this is known to have negative performance impacts.
	//       Test if this is the case and do something about it.
	//       sync.Map could help here. For the price of quite a lot of pointer chasing (probably only in cache),
	//       we remove all contention (the way we use the map avoids contention completely).
	buffersLock sync.RWMutex

	// Holds all the buckets to which client requests are added after being added to Buffers.
	Buckets []*Bucket

	// Channels to which gRPC threads are writing incoming requests.
	// A configurable number of threads (config.Config.RequestHandlerThreads) reads these channels and puts the requests
	// in corresponding Buffers. This indirection is required to avoid cache contention on the Buffer locks when there
	// are many clients (more than the number of physical cores of the machine).
	requestInputChannels []chan *pb.ClientRequest

	// Request verifier data structures.
	verifierChan chan *Request // Channel to which requests are written for verifying

	// Function used for verifying request batches. Set during initialization.
	batchVerifierFunc func(*Batch) bool
)

type watermarkRange struct {
	oldWM int32
	newWM int32
}

// Initialize the request package.
// Cannot be part of the init() function, as the configuration file is not yet loaded when init() is executed.
func Init() {

	// Initializes the Buckets.
	Buckets = make([]*Bucket, config.Config.NumBuckets)
	for i := range Buckets {
		Buckets[i] = NewBucket(i)
	}

	// Initialize request handler goroutines.
	// These threads are reading incoming requests from (buffered) input channels and putting them in Buffers / Buckets.
	// The gRPC threads (one per client) are writing these requests to the channels.
	// The number of request handler threads is limited on purpose to avoid cache contention when accessing the Buffers.
	// If the gRPC threads were to do this directly, cache contention could occur in deployments with many clients.
	requestInputChannels = make([]chan *pb.ClientRequest, config.Config.RequestHandlerThreads, config.Config.RequestHandlerThreads)
	for i := 0; i < config.Config.RequestHandlerThreads; i++ {

		// Create new request input channel and a thread that reads that reads requests from the channel
		// and adds those requests to the corresponding Buffers
		// TODO: Implement graceful shutdown!
		requestInputChannels[i] = make(chan *pb.ClientRequest, config.Config.RequestInputChannelBuffer)
		go func(i int) {
			for req := range requestInputChannels[i] {
				AddReqMsg(req)
			}
		}(i)
	}

	if config.Config.BatchVerifier == "sequential" {
		batchVerifierFunc = checkSignaturesSequential
	} else if config.Config.BatchVerifier == "parallel" {
		batchVerifierFunc = checkSignaturesParallel
	} else if config.Config.BatchVerifier == "external" {
		batchVerifierFunc = checkSignaturesExternal

		// Initialize request verifier goroutines.
		// These are a fixed number of threads only verifying request signatures.
		// The capacity of the verifier channel buffer is chosen such that one full batch for each verifier fits inside.
		// To stop the goroutines, close the verifier channel.
		verifierChan = make(chan *Request, config.Config.BatchSize*config.Config.RequestHandlerThreads)
		for i := 0; i < config.Config.RequestHandlerThreads; i++ {
			go func() {

				// Reads requests from the verifier channel,
				// verifies them and writes them to the channel indicated by the Request.
				for req := range verifierChan {
					if err := crypto.CheckSig(req.Digest,
						membership.ClientPubKey(req.Msg.RequestId.ClientId),
						req.Msg.Signature); err == nil {
						req.Verified = true
					}
					req.VerifiedChan <- req
				}

			}()
		}
	} else {
		logger.Fatal().Str("name", config.Config.BatchVerifier).Msg("Unknown batch verifier.")
	}

}

// Represents a client request and stores request-specific metadata.
type Request struct {

	// The request message received over the network.
	Msg *pb.ClientRequest

	// Digest of the request.
	Digest []byte

	// Pointer to the buffer the request maps to (through its client ID).
	// This, except for convenience, avoids acquiring the read lock on the Buffer map when looking up this request's Buffer.
	Buffer *Buffer

	// Pointer to the bucket the request maps to.
	// Note that this does not necessarily mean that the request is currently inserted in that bucket!
	// It only means that IF the request is in a bucket, this is the bucket.
	Bucket *Bucket

	// Flag indicating whether the request signature has been verified.
	Verified bool

	// Request is "in flight", i.e., has been added to or observed in some (protocol-specific) proposal message.
	// This flag tracks duplication.
	// A request should be marked as in flight upon being either added to or upon encountered in a proposal message.
	// If a request is already proposed it should not be found in any other proposal message.
	InFlight bool

	// Requests are stored in a doubly linked list in the bucket for constant-time (random-access) adding and removing
	// while still being able to use it as a FIFO queue.
	// We do not encapsulate the Request in a container "Element" struct to avoid allocations of those elements.
	// Both Next and Prev must be set to nil when the request is not in a bucket.
	Next *Request
	Prev *Request

	// Channel to which the verifying goroutine should write the verified request.
	// During verification of a batch, a channel is created and assigned to this field for all requests in the batch.
	// Then, the requests are written to a (different) channel for the verifier goroutines to process them.
	// When a verifier has verified the request's signature, it writes it to this channel in order to notify the
	// batch verification method that the verification of this request finished.
	VerifiedChan chan *Request
}

// Allocates a new Request object from a client request message and adds it by calling Add().
func AddReqMsg(reqMsg *pb.ClientRequest) *Request {
	return Add(&Request{
		Msg:      reqMsg,
		Digest:   Digest(reqMsg),
		Buffer:   getBuffer(reqMsg.RequestId.ClientId),
		Bucket:   getBucket(reqMsg),
		Verified: false, // signature has not yet been verified
		InFlight: false, // request has not yet been proposed (an identical one might have been, though, in which case we discard this request object)
		Next:     nil,   // This request object is not part of a bucket list.
		Prev:     nil,
	})
}

// Adds a request received as a protobuf message to the appropriate buffer and bucket.
func Add(req *Request) *Request {

	// The buffer needs to be Rlocked not only while checking watermarks, but also while adding the request to the Bucket!
	req.Buffer.RLock()
	defer req.Buffer.RUnlock()

	// Check request's watermarks and backlog it if necessary (by adding it to the Buffer)
	// Return if Request is not suited for processing right now (due to being outside of the watermark window).
	// The request might be backlogged though by the Buffer
	// (Note that the implementation of backlogging does not require a write lock on the buffer.).
	if !req.Buffer.Add(req) {
		return nil
	}

	// If Request is within the client watermark window, try looking it up in its bucket.
	// If an identical request has already been stored in the bucket, return that request.
	storedReq, retry := req.Bucket.AddRequest(req)

	// If retrying is not necessary (request added) or meaningful (request cannot be added anyway),
	// Return the result of the first optimistic attempt.
	if retry {
		// Check client signature.
		// Not checking whether signature checking is enabled,
		// since no retrying would be requested if signature checking was disabled.
		if err := crypto.CheckSig(req.Digest, membership.ClientPubKey(req.Msg.RequestId.ClientId), req.Msg.Signature); err == nil {
			req.Verified = true
		} else {
			logger.Warn().
				Err(err).
				Int32("clSn", req.Msg.RequestId.ClientSn).
				Int32("clId", req.Msg.RequestId.ClientId).
				Msg("Invalid request signature.")

			return nil
		}

		// Add verified request.
		// If request cannot be added this time, it is not because of an unverified signature.
		storedReq, _ = req.Bucket.AddRequest(req)
	}

	// Return the request object that ended up being stored, eithher on the first, or on the second attempt.
	// storedReq is nil if request is invalid (outside of the watermark window.)
	return storedReq
}

// Removes all requests in batch from their respective Buckets.
// The buffer is not affected by this function.
func RemoveBatch(batch *Batch) {

	// Do nothing if batch is empty.
	if len(batch.Requests) == 0 {
		return
	}

	// As this usually touches many Buckets and contends with the critical path of requests,
	// we first prepare lists of requests to be removed from each bucket and remove all at once,
	// to avoid frequent locking and unlocking of the buckets.
	toRemove := make(map[int][]*Request) // Maps bucket IDs to the lists of the corresponding clients' requests
	for _, req := range batch.Requests {
		bucketID := req.Bucket.GetId()
		reqs, ok := toRemove[bucketID]
		if !ok {
			reqs = make([]*Request, 0)
			toRemove[bucketID] = reqs
		}
		toRemove[bucketID] = append(reqs, req)
	}

	// For each bucket represented in this batch, remove the all the corresponding requests at once.
	// By construction, no list of requests is empty and all requests in each list are in the same bucket,
	// so taking the first reuest's bucket is safe.
	for _, reqs := range toRemove {
		reqs[0].Bucket.Remove(reqs)
	}
}

// Advances the client watermarks of all Buffers.
// This allows new Requests to be added to the Buffers.
func AdvanceWatermarks(entries []interface{}) { //expected type is []*log.Entry
	logger.Info().Int("numEntries", len(entries)).Msg("Advancing watermarks.")

	// We only acquire a read lock on the buffers map, since the map itself is not modified.
	// What is modified is the Buffers the map entries point to, but those have their own locks.
	buffersLock.RLock()
	defer buffersLock.RUnlock()

	// This map stores, for each client, its old and its new watermark.
	watermarks := &sync.Map{}

	// All buffers advance their watermarks in parallel and synchronize over this wait group.
	wg := sync.WaitGroup{}
	wg.Add(len(buffers))

	// Advance watermarks for each buffer in parallel.
	for _, buf := range buffers {
		go func(b *Buffer) {
			watermarks.Store(b.ClientID, b.AdvanceWatermarks(entries))
			wg.Done()
		}(buf)
	}
	wg.Wait()

	// Once watermarks have been advanced (and only then),
	// Prune the index of all the buckets in parallel.
	// Count the number of requests left in buckets for analytical purposes.
	wg.Add(len(Buckets))
	for _, bucket := range Buckets {
		go func(b *Bucket) {
			b.PruneIndex(watermarks)
			wg.Done()
		}(bucket)
	}
	wg.Wait()
}

// Returns a bucket to which the request message belongs.
func getBucket(req *pb.ClientRequest) *Bucket {
	return Buckets[GetBucketNr(req.RequestId.ClientId, req.RequestId.ClientSn)]
}

// This is the hash function that computes the bucket number of a request.
// This implementation assigns requests from the same client to buckets in a round-robin way.
func GetBucketNr(clID int32, clSN int32) int {
	return int((clID + clSN) % int32(config.Config.NumBuckets))
}

// Returns the request buffer associated with a client ID.
// If the request buffer does not exist, allocates a new one.
func getBuffer(clientID int32) *Buffer {

	// First, check if buffer is present only using a read lock.
	// This check only fails for the very first request from a client (unless we implement client GC later).
	// If we find the buffer, no modification to the buffers map is made and a read lock suffices.
	buffersLock.RLock()
	if buf, ok := buffers[clientID]; ok {
		buffersLock.RUnlock()
		return buf
	}
	buffersLock.RUnlock()

	// Otherwise (i.e., for the first request from a client),
	// acquire a write lock and add buffer for new client.
	buffersLock.Lock()
	defer buffersLock.Unlock()

	// The new check is required in case some other thread adds the buffer for this clientID in the meantime.
	// (Even if currently this should not happen anyway, as only one thread is dealing with a client connection.)
	if buf, ok := buffers[clientID]; ok {
		return buf
	} else {
		newBuf := NewBuffer(clientID)
		buffers[clientID] = newBuf
		return newBuf
	}
}

// This ugly ugly function is only a nasty workaround for the DummyOrderer
// to circumvent properly adding received requests to their buffers.
// Should never be used anywhere outside the DummyOrderer.
func UglyUglyDummyRegisterRequest(reqMsg *pb.ClientRequest) *Request {
	return &Request{
		Msg:      reqMsg,
		Bucket:   getBucket(reqMsg),
		InFlight: true,
	}
}

// Return the hash of a protobuf client request message.
func Digest(req *pb.ClientRequest) []byte {
	buffer := make([]byte, 0, 4+4+len(req.Payload)+len(req.Pubkey))
	id := RequestIDToBytes(req)
	buffer = append(buffer, id...)
	buffer = append(buffer, req.Payload...)
	buffer = append(buffer, req.Pubkey...)
	return crypto.Hash(buffer)
}

func RequestIDToBytes(req *pb.ClientRequest) []byte {
	buffer := make([]byte, 0, 0)
	sn := make([]byte, 4)
	binary.LittleEndian.PutUint32(sn, uint32(req.RequestId.ClientSn))
	buffer = append(buffer, sn...)
	id := make([]byte, 4)
	binary.LittleEndian.PutUint32(id, uint32(req.RequestId.ClientId))
	buffer = append(buffer, id...)
	return buffer
}
