/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import t "github.com/hyperledger-labs/mirbft/pkg/types"

// TODO: Augment to support threshold signatures.

// TODO: Write comments.

type Crypto interface {
	Sign(data []byte) ([]byte, error)

	RegisterNodeKey(pubKey []byte, nodeID t.NodeID) error
	RegisterClientKey(pubKey []byte, clientID t.ClientID) error

	DeleteNodeKey(nodeID t.NodeID)
	DeleteClientKey(clientID t.ClientID)

	VerifyNodeSig(data []byte, signature []byte, nodeID t.NodeID) error
	VerifyClientSig(data []byte, signature []byte, clientID t.ClientID) error
}
