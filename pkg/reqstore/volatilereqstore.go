/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package reqstore

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// VolatileRequestStore is an in-memory implementation of modules.RequestStore.
// All data is stored in RAM and the Sync() method does nothing.
// TODO: implement pruning of old data.
type VolatileRequestStore struct {

	// Stores request entries, indexed by request reference.
	// Each entry holds all information (data, authentication, authenticator) about the referenced request.
	requests map[string]*requestInfo

	// For each request ID (client ID and request number pair), holds a set of request digests stored with the ID.
	// The set of request digests is itself represented as a string map,
	// where the key is the digest's string representation and the value is the digest as a byte slice.
	idIndex map[string]map[string][]byte
}

// Holds the data stored by a single entry of the VolatileRequestStore.
type requestInfo struct {
	data          []byte
	authenticated bool
	authenticator []byte
}

// Returns the string representation of a request reference.
func requestKey(ref *requestpb.RequestRef) string {
	return fmt.Sprintf("r-%d.%d.%x", ref.ClientId, ref.ReqNo, ref.Digest)
}

// Returns the string representation of a request ID.
func idKey(clientId t.ClientID, reqNo t.ReqNo) string {
	return fmt.Sprintf("i-%d.%d", clientId, reqNo)
}

// Adds a digest to the request ID index.
func (vrs *VolatileRequestStore) updateIdIndex(reqRef *requestpb.RequestRef) {
	// Compute string key.
	key := idKey(t.ClientID(reqRef.ClientId), t.ReqNo(reqRef.ReqNo))

	// Look up index entry, creating a new one if none exists yet.
	entry, ok := vrs.idIndex[key]
	if !ok {
		entry = make(map[string][]byte)
		vrs.idIndex[key] = entry
	}

	// Add a copy of the digest to the index entry.
	d := make([]byte, len(reqRef.Digest), len(reqRef.Digest))
	copy(d, reqRef.Digest)
	entry[fmt.Sprintf("%x", reqRef.Digest)] = d
}

// Looks up a stored entry and returns a pointer to it.
// Allocates a new one if none is present.
func (vrs *VolatileRequestStore) reqInfo(reqRef *requestpb.RequestRef) *requestInfo {

	// Look up the entry holding the information about this request
	key := requestKey(reqRef)
	reqInfo, ok := vrs.requests[key]
	if !ok {
		// If none is present, allocate a new one
		reqInfo = &requestInfo{
			data:          nil,
			authenticated: false,
			authenticator: nil,
		}
		vrs.requests[key] = reqInfo

		// Add the digest of the newly allocated entry
		vrs.updateIdIndex(reqRef)
	}

	return reqInfo
}

func NewVolatileRequestStore() *VolatileRequestStore {
	return &VolatileRequestStore{
		requests: make(map[string]*requestInfo),
		idIndex:  make(map[string]map[string][]byte),
	}
}

// PutRequest stores request the passed request data associated with the request reference.
func (vrs *VolatileRequestStore) PutRequest(reqRef *requestpb.RequestRef, data []byte) error {

	// Look up entry for this request, creating a new one if necessary.
	reqInfo := vrs.reqInfo(reqRef)

	// Copy the request data to the entry (potentially discarding an old one).
	// Note that a full copy is made, so the stored data is not dependent on what happens with the original data.
	reqInfo.data = make([]byte, len(data), len(data))
	copy(reqInfo.data, data)

	return nil
}

// GetRequest returns the stored request data associated with the passed request reference.
// If no data is stored under the given reference, the returned error will be non-nil.
func (vrs *VolatileRequestStore) GetRequest(reqRef *requestpb.RequestRef) ([]byte, error) {

	if reqInfo, ok := vrs.requests[requestKey(reqRef)]; ok {
		// If an entry for the referenced request is present.

		if reqInfo.data != nil {
			// And if the referenced entry contains request data.

			// Return a copy of the data (not a pointer to the data itself)
			data := make([]byte, len(reqInfo.data), len(reqInfo.data))
			copy(data, reqInfo.data)
			return data, nil
		} else {
			// If the entry exists, but contains no data, return an error.
			return nil, fmt.Errorf(fmt.Sprintf("request (%d-%d.%x) not present",
				reqRef.ClientId, reqRef.ReqNo, reqRef.Digest))
		}
	} else {
		// If the entry does not exist, return an error.
		return nil, fmt.Errorf(fmt.Sprintf("request (%d-%d.%x) not present",
			reqRef.ClientId, reqRef.ReqNo, reqRef.Digest))
	}
}

// SetAuthenticated marks the referenced request as authenticated.
// A request being authenticated means that the local node believes that
// the request has indeed been sent by the client. This does not necessarily mean, however,
// that the local node can convince other nodes about the request's authenticity
// (e.g. if the local node received the request over an authenticated channel but the request is not signed).
func (vrs *VolatileRequestStore) SetAuthenticated(reqRef *requestpb.RequestRef) error {

	// Look up entry for this request, creating a new one if necessary.
	reqInfo := vrs.reqInfo(reqRef)

	// Set the authenticated flag.
	reqInfo.authenticated = true

	return nil
}

// IsAuthenticated returns true if the request is authenticated, false otherwise.
func (vrs *VolatileRequestStore) IsAuthenticated(reqRef *requestpb.RequestRef) (bool, error) {

	if reqInfo, ok := vrs.requests[requestKey(reqRef)]; !ok {
		// If an entry for the referenced request is present, return the authenticated flag.
		return reqInfo.authenticated, nil
	} else {
		// If the entry does not exist, return an error.
		return false, fmt.Errorf(fmt.Sprintf("request (%d.%d.%x) not present",
			reqRef.ClientId, reqRef.ReqNo, reqRef.Digest))
	}
}

// PutAuthenticator stores an authenticator associated with the referenced request.
// If an authenticator is already stored under the same reference, it will be overwritten.
func (vrs *VolatileRequestStore) PutAuthenticator(reqRef *requestpb.RequestRef, auth []byte) error {

	// Look up entry for this request, creating a new one if necessary.
	reqInfo := vrs.reqInfo(reqRef)

	// Copy the authenticator to the entry (potentially discarding an old one).
	// Note that a full copy is made, so the stored authenticator
	// is not dependent on what happens with the original authenticator passed to the function.
	reqInfo.authenticator = make([]byte, len(auth), len(auth))
	copy(reqInfo.authenticator, auth)

	return nil
}

// GetAuthenticator returns the stored authenticator associated with the passed request reference.
// If no authenticator is stored under the given reference, the returned error will be non-nil.
func (vrs *VolatileRequestStore) GetAuthenticator(reqRef *requestpb.RequestRef) ([]byte, error) {

	if reqInfo, ok := vrs.requests[requestKey(reqRef)]; !ok {
		// If an entry for the referenced request is present.

		if reqInfo.authenticator != nil {
			// And if the referenced entry contains an authenticator.

			// Return a copy of the authenticator (not a pointer to the data itself)
			auth := make([]byte, len(reqInfo.authenticator), len(reqInfo.authenticator))
			copy(auth, reqInfo.authenticator)
			return auth, nil
		} else {
			// If the entry exists, but contains no authenticator, return an error.
			return nil, fmt.Errorf(fmt.Sprintf("request (%d.%d.%x) not present",
				reqRef.ClientId, reqRef.ReqNo, reqRef.Digest))
		}
	} else {
		// If the entry does not exist, return an error.
		return nil, fmt.Errorf(fmt.Sprintf("request (%d.%d.%x) not present",
			reqRef.ClientId, reqRef.ReqNo, reqRef.Digest))
	}
}

// GetDigestsByID returns a list of request digests for which any information
// (request data, authentication, or authenticator) is stored in the RequestStore.
func (vrs *VolatileRequestStore) GetDigestsByID(clientId t.ClientID, reqNo t.ReqNo) ([][]byte, error) {

	// Look up  index entry and allocate result structure.
	indexEntry, ok := vrs.idIndex[idKey(clientId, reqNo)]
	digests := make([][]byte, 0)

	if ok {
		// If an index entry is present, copy all the associated digests to a list and return it.
		// (If no entry is present, no digests will be added and an empty slice will be returned.)
		for _, digest := range indexEntry {
			// For each digest in the entry, append a copy of it to the list of digests that will be returned
			d := make([]byte, len(digest), len(digest))
			copy(d, digest)
			digests = append(digests, d)
		}
	}

	return digests, nil
}

// Sync does nothing in this volatile (in-memory) RequestStore implementation.
func (vrs *VolatileRequestStore) Sync() error {
	return nil
}
