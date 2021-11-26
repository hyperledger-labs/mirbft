/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import t "github.com/hyperledger-labs/mirbft/pkg/types"

// TODO: Augment to support threshold signatures.

// The Crypto module is responsible for producing and verifying cryptographic signatures.
// It internally stores information about which clients and nodes are associated with which public keys.
// This information can be updated by the Node through appropriate events.
// Both clients and nodes are identified only by their numeric IDs.
type Crypto interface {

	// Sign signs the provided data and returns the resulting signature.
	// The data to be signed is the concatenation of all the passed byte slices.
	// A signature produced by Sign is verifiable using VerifyNodeSig or VerifyClientSig,
	// if, respectively, RegisterNodeKey or RegisterClientKey has been invoked with the corresponding public key.
	// Note that the private key used to produce the signature cannot be set ("registered") through this interface.
	// Storing and using the private key is completely implementation-dependent.
	Sign(data [][]byte) ([]byte, error)

	// RegisterNodeKey associates a public key with a numeric node ID.
	// The representation of the key is implementation-dependent.
	// Calls to VerifyNodeSig will fail until RegisterNodeKey is successfully called with the corresponding node ID.
	// Returns nil on success, a non-nil error on failure.
	RegisterNodeKey(pubKey []byte, nodeID t.NodeID) error

	// RegisterClientKey associates a public key with a numeric client ID.
	// The representation of the key is implementation-dependent.
	// Calls to VerifyClientSig will fail until RegisterClientKey is successfully called with the corresponding client ID.
	// Returns nil on success, a non-nil error on failure.
	RegisterClientKey(pubKey []byte, clientID t.ClientID) error

	// DeleteNodeKey removes the public key associated with nodeID from the module's state.
	// Any subsequent call to VerifyNodeSig(..., nodeID) will fail.
	DeleteNodeKey(nodeID t.NodeID)

	// DeleteClientKey removes the public key associated with clientID from the module's state.
	// Any subsequent call to VerifyClientSig(..., clientID) will fail.
	DeleteClientKey(clientID t.ClientID)

	// VerifyNodeSig verifies a signature produced by the node with numeric ID nodeID over data.
	// Returns nil on success (i.e., if the given signature is valid) and a non-nil error otherwise.
	// Note that RegisterNodeKey must be used to register the node's public key before calling VerifyNodeSig,
	// otherwise VerifyNodeSig will fail.
	VerifyNodeSig(data [][]byte, signature []byte, nodeID t.NodeID) error

	// VerifyClientSig verifies a signature produced by the client with numeric ID clientID over data.
	// Returns nil on success (i.e., if the given signature is valid) and a non-nil error otherwise.
	// Note that RegisterClientKey must be used to register the client's public key before calling VerifyClientSig,
	// otherwise VerifyClientSig will fail.
	VerifyClientSig(data [][]byte, signature []byte, clientID t.ClientID) error
}
