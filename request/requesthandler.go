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
	"math/big"

	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/tracing"
)

// TODO: It's inefficient to hash a request every time it is needed to get the request ID

// This function is used by the messenger as the handler function for requests (the main file performs the assignment).
// Simply adds the received request to the corresponding request buffer.
// TODO: If too many threads (64 or more in the current deployment with 32-core machines) invoke Add(),
//       the buffer locks get extremely contended.
//       Have only a fixed (configurable) number of threads invoking Add().
//       Spawn those worker threads in the Init() function and make HandleRequest (this function) only write
//       the request to a channel (do we need a big channel buffer for this?) that a worker reads.
//       It would make sense to send requests from the same client to the same worker,
//       Since the Buffer lock to be acquired by the worker is determined by the clientID.
//       The lock being acquired by the same thread is crucial for avoiding contention.
//       If this is not enough, try having the worker threads add requests to buffers in batches.
//       (Although this might be very tricky if we want to avoid verifying signatures while holding the buffer lock,
//       and at the same time avoid verifying the signature again, in case the request is already present.)
func HandleRequest(req *pb.ClientRequest) {

	tracing.MainTrace.Event(tracing.REQ_RECEIVE, int64(req.RequestId.ClientId), int64(req.RequestId.ClientSn))

	if config.Config.RequestHandlerThreads > 0 {
		// Write request to the corresponding input channel for further processing by a request handler thread.
		// There is a fixed number of request handler threads (should be at most as many as there are physical cores)
		// to avoid cache contention on the request Buffers. To avoid this contention, it is also crucial that requests from
		// the same client (there is a separate Buffer per client) are handled by the same request handler thread.
		requestInputChannels[int(req.RequestId.ClientId)%config.Config.RequestHandlerThreads] <- req
	} else {
		AddReqMsg(req)
	}
}

func GetBucketByHashing(req *pb.ClientRequest) *Bucket {
	H := new(big.Int)
	H.SetString(crypto.Hspace, 10)

	// TODO keep this value somewhere else, its inefficient to calculate it all the time
	bucketSize := new(big.Int).Div(H, big.NewInt(int64(config.Config.NumBuckets)))

	reqKey := new(big.Int)
	reqKey.SetBytes(crypto.Hash(RequestIDToBytes(req)))

	// Calculating bucket id
	I := new(big.Int).Div(reqKey, bucketSize)
	i := I.Uint64()

	// If the calculated ID is higher that the number of buckets clearly we are doing something wrong
	if i > uint64(config.Config.NumBuckets-1) {
		panic("Request beyond bucket limits")
	}

	b := Buckets[i]

	return b
}
