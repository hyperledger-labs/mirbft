/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// The RequestStore store persistently stores the payloads and authentication attributes of received requests.
// Each request is referenced using a msgs.RequestRef structure that contains the following fields:
// - ClientID: The numeric ID of the client that submitted the request.
// -   ReqNo: The client-local sequence number of the request (how many request the client issued before this request).
// -  Digest: A hash of the request payload.
// Along the request data (stored and retrieved using PutRequest() and GetRequest(), respectively),
// the RequestStore stores the authentication status of each request.
// A Boolean value (set and queried using SetAuthenticated() and IsAuthenticated(), respectively)
// signifies whether the request is authenticated to the local node.
// I.e, whether the local node believes that the request has indeed been sent by the client.
// This does not necessarily mean, however, that the local node can convince other nodes
// about the request's authenticity (e.g. if the local node received the request over an authenticated channel).
// Note that the authenticated flag cannot be cleared, as this would not be meaningful given its semantics.
//
// For the purpose of convincing the rest of the system about a request's authenticity,
// an authenticator (e.g. a cryptographic signature) may be attached to the request.
// The authenticator is stored and retrieved using PutAuthenticator() and GetAuthenticator(), respectively.
// Note in particular that, in the context of a BFT TOB system, a node may accept a proposed request
// if the request is authenticated, even though the node does not have an authenticator
// (e.g. if the request has ben obtained directly from the client, but is not signed).
// For proposing a request, an authenticator is necessary to make sure that other correct nodes will accept the request.
//
// All effects of method invocations that change the state of the RequestStore can only be guaranteed to be persisted
// When a subsequent invocation of Sync() returns. Without a call to Sync(), the effects may or may not be persisted.
//
// TODO: Define functions for removal of old data.
type RequestStore interface {

	// PutRequest stores request the passed request data associated with the request reference.
	PutRequest(reqRef *requestpb.RequestRef, data []byte) error

	// GetRequest returns the stored request data associated with the passed request reference.
	// If no data is stored under the given reference, the returned error will be non-nil.
	GetRequest(reqRef *requestpb.RequestRef) ([]byte, error)

	// SetAuthenticated marks the referenced request as authenticated.
	// A request being authenticated means that the local node believes that
	// the request has indeed been sent by the client. This does not necessarily mean, however,
	// that the local node can convince other nodes about the request's authenticity
	// (e.g. if the local node received the request over an authenticated channel but the request is not signed).
	SetAuthenticated(reqRef *requestpb.RequestRef) error

	// IsAuthenticated returns true if the request is authenticated, false otherwise.
	IsAuthenticated(reqRef *requestpb.RequestRef) (bool, error)

	// PutAuthenticator stores an authenticator associated with the referenced request.
	// If an authenticator is already stored under the same reference, it will be overwritten.
	PutAuthenticator(reqRef *requestpb.RequestRef, auth []byte) error

	// GetAuthenticator returns the stored authenticator associated with the passed request reference.
	// If no authenticator is stored under the given reference, the returned error will be non-nil.
	GetAuthenticator(reqRef *requestpb.RequestRef) ([]byte, error)

	// GetDigestsByID returns a list of request Digests for which any information
	// (request data, authentication, or authenticator) is stored in the RequestStore.
	GetDigestsByID(clientID t.ClientID, reqNo t.ReqNo) ([][]byte, error)

	// Sync blocks until the effects of all preceding method invocations have been persisted.
	Sync() error
}
