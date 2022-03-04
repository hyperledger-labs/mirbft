/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package serializing

import (
	"encoding/binary"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
)

// TODO: Write doc.

func RequestForHash(req *requestpb.Request) [][]byte {

	// Encode integer fields.
	clientIDBuf := make([]byte, 8)
	reqNoBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(clientIDBuf, req.ClientId)
	binary.LittleEndian.PutUint64(reqNoBuf, req.ReqNo)

	// Note that the signature is *not* part of the hashed data.

	// Return serialized integers along with the request data itself.
	return [][]byte{clientIDBuf, reqNoBuf, req.Data}
}

func BatchForHash(batch *requestpb.Batch) [][]byte {

	// Allocate output slice.
	data := make([][]byte, len(batch.Requests), len(batch.Requests))

	// Collect all request digests in the batch.
	for i, reqRef := range batch.Requests {
		data[i] = reqRef.Digest
	}

	// Return populated output slice.
	return data
}
