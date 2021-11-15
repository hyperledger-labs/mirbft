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

// DummyCrypto represents a dummy cryptographic module that
// always produces the same dummy byte slice specified at instantiation as signature.
// Verification of this dummy signature always succeeds.
type DummyCrypto struct {
	DummySig []byte
}

func (dc *DummyCrypto) Sign(data []byte) ([]byte, error) {
	return dc.DummySig, nil
}

func (dc *DummyCrypto) RegisterNodeKey(pubKey []byte, nodeID t.NodeID) error {
	return nil
}

func (dc *DummyCrypto) DeleteNodeKey(nodeID t.NodeID) {
}

func (dc *DummyCrypto) VerifyNodeSig(data []byte, signature []byte, nodeID t.NodeID) error {
	if bytes.Equal(signature, dc.DummySig) {
		return nil
	} else {
		return fmt.Errorf("dummy signature mismatch")
	}
}

func (dc *DummyCrypto) RegisterClientKey(pubKey []byte, clientID t.ClientID) error {
	return nil
}

func (dc *DummyCrypto) DeleteClientKey(clientID t.ClientID) {
}

func (dc *DummyCrypto) VerifyClientSig(data []byte, signature []byte, clientID t.ClientID) error {
	if bytes.Equal(signature, dc.DummySig) {
		return nil
	} else {
		return fmt.Errorf("dummy signature mismatch")
	}
}
