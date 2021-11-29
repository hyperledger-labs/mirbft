/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"bytes"
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// TODO: Write comments.

// DummyCrypto represents a dummy Crypto module that
// always produces the same dummy byte slice specified at instantiation as signature.
// Verification of this dummy signature always succeeds.
// This is intended as a stub for testing purposes.
type DummyCrypto struct {

	// The only accepted signature
	DummySig []byte
}

// Sign always returns the dummy signature DummySig, regardless of the data.
func (dc *DummyCrypto) Sign(data [][]byte) ([]byte, error) {
	return dc.DummySig, nil
}

// RegisterNodeKey does nothing, as no public keys are used.
func (dc *DummyCrypto) RegisterNodeKey(pubKey []byte, nodeID t.NodeID) error {
	return nil
}

// DeleteNodeKey does nothing, as no public keys are used.
func (dc *DummyCrypto) DeleteNodeKey(nodeID t.NodeID) {
}

// VerifyNodeSig returns nil (i.e. success) only if signature equals DummySig.
// Both data and nodeID are ignored.
func (dc *DummyCrypto) VerifyNodeSig(data [][]byte, signature []byte, nodeID t.NodeID) error {
	if bytes.Equal(signature, dc.DummySig) {
		return nil
	} else {
		return fmt.Errorf("dummy signature mismatch")
	}
}

// RegisterClientKey does nothing, as no public keys are used.
func (dc *DummyCrypto) RegisterClientKey(pubKey []byte, clientID t.ClientID) error {
	return nil
}

// DeleteClientKey does nothing, as no public keys are used.
func (dc *DummyCrypto) DeleteClientKey(clientID t.ClientID) {
}

// VerifyClientSig returns nil (i.e. success) only if signature equals DummySig.
// Both data and nodeID are ignored.
func (dc *DummyCrypto) VerifyClientSig(data [][]byte, signature []byte, clientID t.ClientID) error {
	if bytes.Equal(signature, dc.DummySig) {
		return nil
	} else {
		return fmt.Errorf("dummy signature mismatch")
	}
}
