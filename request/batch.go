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
	"fmt"
	"sync"
	"sync/atomic"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// Represents a batch of requests.
type Batch struct {
	Requests []*Request
}

// ATTENTION: access to InFLight field is not atomic.
// TODO: do we need to make it atomic? currently the orderer processes all messages pertaining an instance sequentially.
// TODO: is it possible to concurrently access the same request from different instances?

// Marks all requests in the batch as "in flight"
func (b *Batch) MarkInFlight() {
	for _, req := range b.Requests {
		req.InFlight = true
	}
}

// Checks if the batch contains "in flight" requests.
// If the batch has "in flight" requests the method returns an error.
func (b *Batch) CheckInFlight() error {
	for _, req := range b.Requests {
		if req.InFlight {
			return fmt.Errorf("request %d from %d is in flight", req.Msg.RequestId.ClientSn, req.Msg.RequestId.ClientId)
		}
	}
	return nil
}

// Checks if the requests in the batch match a specific bucket.
// If there exists some request that does not match the bucket id the method returns an error.
func (b *Batch) CheckBucket(activeBuckets []int) error {
	for _, req := range b.Requests {
		bucketID := getBucket(req.Msg).id
		found := false
		for _, b := range activeBuckets {
			if b == bucketID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("request %d from %d does not match any active bucket, should be in bucket %d", req.Msg.RequestId.ClientSn, req.Msg.RequestId.ClientId, bucketID)
		}
	}
	return nil
}

func (b *Batch) CheckSignatures() error {
	if batchVerifierFunc(b) {
		return nil
	} else {
		return fmt.Errorf("batch signature verification failed")
	}
}

// Creates a batch from a protobuf message and tries to add the requests of the message to their buffer.
// If the request is not added successfully (Add returns nil) this method also returns nil
func NewBatch(msg *pb.Batch) *Batch {

	logger.Debug().Int("nReq", len(msg.Requests)).Msg("Creating new Batch.")

	newBatch := &Batch{Requests: make([]*Request, len(msg.Requests), len(msg.Requests))}
	for i, reqMsg := range msg.Requests {
		req := AddReqMsg(reqMsg)
		if req == nil {
			logger.Warn().
				Int32("ClientId", reqMsg.RequestId.ClientId).
				Int32("ClientSn", reqMsg.RequestId.ClientSn).
				Msg("Invalid batch. Could not add / look up request.")
			return nil
		}
		newBatch.Requests[i] = req
	}

	// Check signatures of the requests in the new batch.
	if config.Config.SignRequests {
		if err := newBatch.CheckSignatures(); err != nil {
			logger.Fatal().Err(err).Msg("Invalid signature in new batch.")
			// TODO: Instead of crashing, just return nil.
			return nil
		}
	}

	return newBatch
}

// Returns a protobuf message containing this Batch.
func (b *Batch) Message() *pb.Batch {
	// Create empty Batch message
	msg := pb.Batch{
		Requests: make([]*pb.ClientRequest, len(b.Requests), len(b.Requests)),
	}

	// Populate Batch message with request messages
	for i, req := range b.Requests {
		msg.Requests[i] = req.Msg
	}

	// Return final Batch message
	return &msg
}

// Returns requests in the batch in their buckets after an unsuccessful proposal.
// TODO: Optimization: First group the requests by bucket and then prepend each group at once.
func (b *Batch) Resurrect() {
	for _, req := range b.Requests {
		req.InFlight = false
		req.Bucket.Prepend(req)
	}
}

func checkSignaturesSequential(b *Batch) bool {
	for _, req := range b.Requests {
		if !req.Verified {
			if err := crypto.CheckSig(req.Digest,
				membership.ClientPubKey(req.Msg.RequestId.ClientId),
				req.Msg.Signature); err != nil {
				logger.Warn().
					Err(err).
					Int32("clSn", req.Msg.RequestId.ClientSn).
					Int32("clId", req.Msg.RequestId.ClientId).
					Msg("Invalid request signature.")

				return false
			} else {
				req.Verified = true
			}
		}
	}

	return true
}

func checkSignaturesParallel(b *Batch) bool {
	var wg sync.WaitGroup
	wg.Add(len(b.Requests))
	invalidReqs := int32(0)

	for _, r := range b.Requests {
		if !r.Verified {
			go func(req *Request) {
				if err := crypto.CheckSig(req.Digest,
					membership.ClientPubKey(req.Msg.RequestId.ClientId),
					req.Msg.Signature); err != nil {
					logger.Warn().
						Err(err).
						Int32("clSn", req.Msg.RequestId.ClientSn).
						Int32("clId", req.Msg.RequestId.ClientId).
						Msg("Invalid request signature.")
					atomic.AddInt32(&invalidReqs, 1)
				} else {
					req.Verified = true
				}
				wg.Done()
			}(r)
		} else {
			wg.Done()
		}
	}
	wg.Wait()

	return invalidReqs == 0
}

func checkSignaturesExternal(b *Batch) bool {
	verifiedChan := make(chan *Request, len(b.Requests))
	invalidReqs := 0

	// Write all the unverified requests in the verifier channel.
	verifying := 0
	for _, r := range b.Requests {
		if !r.Verified {
			verifying++
			r.VerifiedChan = verifiedChan
			verifierChan <- r
		}
	}

	// Wait until the verifiers process all the requests and write them in the verifiedChan
	for verifying > 0 {
		verifying--
		req := <-verifiedChan
		req.VerifiedChan = nil
		if !req.Verified {
			logger.Warn().
				Int32("clSn", req.Msg.RequestId.ClientSn).
				Int32("clId", req.Msg.RequestId.ClientId).
				Msg("Request signature verification failed.")
			invalidReqs++
		}
	}

	return invalidReqs == 0
}

func BatchDigest(batch *pb.Batch) []byte {
	metadata := make([]byte, 0, 0)
	reqDigests := make([][]byte, len(batch.Requests), len(batch.Requests))
	for i, req := range batch.Requests {
		// Request id in bytes
		id := RequestIDToBytes(req)
		metadata = append(metadata, id...)
		// Request client public key
		metadata = append(metadata, req.Pubkey...)
		// Digest of the request
		reqDigests[i] = Digest(req)
	}
	return crypto.ParallelDataArrayHash(append(reqDigests, crypto.Hash(metadata)))
}
