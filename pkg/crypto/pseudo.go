/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"io"
	prand "math/rand"
)

var (
	DefaultPseudoSeed int64 = 12345
)

func NodePseudo(nodes []t.NodeID, clients []t.ClientID, ownID t.NodeID, seed int64) (*Crypto, error) {
	randomness := prand.New(prand.NewSource(seed))

	nodePrivKeys, nodePubKeys, err := generateKeys(len(nodes), randomness)
	if err != nil {
		return nil, err
	}

	_, clientPubKeys, err := generateKeys(len(clients), randomness)
	if err != nil {
		return nil, err
	}

	var c *Crypto = nil
	for i, id := range nodes {
		if id == ownID {
			if c, err = New(nodePrivKeys[i]); err != nil {
				return nil, err
			}
		}
	}
	if c == nil {
		return nil, fmt.Errorf("ownID (%d) not found among nodes", ownID)
	}

	if err != nil {
		return nil, err
	}

	if err := registerPubKeys(c, nodePubKeys, clientPubKeys); err != nil {
		return nil, err
	}

	return c, nil
}

func ClientPseudo(nodes []t.NodeID, clients []t.ClientID, ownID t.ClientID, seed int64) (*Crypto, error) {
	randomness := prand.New(prand.NewSource(seed))

	_, nodePubKeys, err := generateKeys(len(nodes), randomness)
	if err != nil {
		return nil, err
	}

	clientPrivKeys, clientPubKeys, err := generateKeys(len(clients), randomness)
	if err != nil {
		return nil, err
	}

	var c *Crypto = nil
	for i, id := range clients {
		if id == ownID {
			if c, err = New(clientPrivKeys[i]); err != nil {
				return nil, err
			}
		}
	}
	if c == nil {
		return nil, fmt.Errorf("ownID (%d) not found among nodes", ownID)
	}

	if err := registerPubKeys(c, nodePubKeys, clientPubKeys); err != nil {
		return nil, err
	}

	return c, nil
}

func generateKeys(numKeys int, randomness io.Reader) (privKeys [][]byte, pubKeys [][]byte, err error) {
	privKeys = make([][]byte, numKeys, numKeys)
	pubKeys = make([][]byte, numKeys, numKeys)
	for i := 0; i < numKeys; i++ {

		privKey, pubKey, err := GenerateKeyPair(randomness)
		if err != nil {
			return nil, nil, fmt.Errorf("error generating key pair: %w", err)
		}
		if privKeys[i], err = SerializePrivKey(privKey); err != nil {
			return nil, nil, fmt.Errorf("error serializing private key: %w", err)
		}
		if pubKeys[i], err = SerializePubKey(pubKey); err != nil {
			return nil, nil, fmt.Errorf("error serializing private key: %w", err)
		}
	}

	return
}

func registerPubKeys(c *Crypto, nodePubKeys [][]byte, clientPubKeys [][]byte) error {

	for id, key := range nodePubKeys {
		if err := c.RegisterNodeKey(key, t.NodeID(id)); err != nil {
			return err
		}
	}

	for id, key := range clientPubKeys {
		if err := c.RegisterClientKey(key, t.ClientID(id)); err != nil {
			return err
		}
	}

	return nil
}
