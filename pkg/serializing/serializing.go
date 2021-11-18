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

	// Encode all data to be hashed.
	clientIDBuf := make([]byte, 8)
	reqNoBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(clientIDBuf, req.ClientId)
	binary.LittleEndian.PutUint64(reqNoBuf, req.ReqNo)

	// Note that the signature is *not* part of the hashed data.

	return [][]byte{clientIDBuf, reqNoBuf, req.Data}
}
