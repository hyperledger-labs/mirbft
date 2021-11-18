/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	cstd "crypto"
	"crypto/ecdsa"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"io"
	"io/ioutil"
	"strings"
)

// TODO: ATTENTION: This package is not tested at all! Perform some basic testing before using.

// TODO: Write comments.

type Crypto struct {
	privKey    interface{}
	nodeKeys   map[t.NodeID]interface{}
	clientKeys map[t.ClientID]interface{}
}

func New(privKey []byte) (*Crypto, error) {
	if key, err := PrivKeyFromBytes(privKey); err == nil {
		return &Crypto{
			privKey:    key,
			nodeKeys:   make(map[t.NodeID]interface{}),
			clientKeys: make(map[t.ClientID]interface{}),
		}, nil
	} else {
		return nil, fmt.Errorf("error parsing private key: %w", err)
	}

}

func (c *Crypto) Sign(data [][]byte) ([]byte, error) {
	switch key := c.privKey.(type) {
	case *rsa.PrivateKey:
		return key.Sign(crand.Reader, hash(data), cstd.SHA256)
	case *ecdsa.PrivateKey:
		return signEcdsa(key, hash(data))
	default:
		return nil, fmt.Errorf("unsupported private key type: %T", key)
	}
}

func (c *Crypto) RegisterNodeKey(pubKey []byte, nodeID t.NodeID) error {
	if key, err := PubKeyFromBytes(pubKey); err == nil {
		c.nodeKeys[nodeID] = key
		return nil
	} else {
		return fmt.Errorf("error parsing node public key: %w", err)
	}
}

func (c *Crypto) DeleteNodeKey(nodeID t.NodeID) {
	delete(c.nodeKeys, nodeID)
}

func (c *Crypto) VerifyNodeSig(data [][]byte, signature []byte, nodeID t.NodeID) error {

	pubKey, ok := c.nodeKeys[nodeID]
	if !ok {
		return fmt.Errorf("no public key for node with ID %d", nodeID)
	}

	return c.verifySig(data, signature, pubKey)
}

func (c *Crypto) RegisterClientKey(pubKey []byte, clientID t.ClientID) error {
	if key, err := PubKeyFromBytes(pubKey); err == nil {
		c.clientKeys[clientID] = key
		return nil
	} else {
		return fmt.Errorf("error parsing client public key: %w", err)
	}
}

func (c *Crypto) DeleteClientKey(clientID t.ClientID) {
	delete(c.clientKeys, clientID)
}

func (c *Crypto) VerifyClientSig(data [][]byte, signature []byte, clientID t.ClientID) error {

	pubKey, ok := c.clientKeys[clientID]
	if !ok {
		return fmt.Errorf("no public key for client with ID %d", clientID)
	}

	return c.verifySig(data, signature, pubKey)
}

func (c *Crypto) verifySig(data [][]byte, signature []byte, pubKey interface{}) error {
	switch key := pubKey.(type) {
	case *ecdsa.PublicKey:
		return verifyEcdsaSignature(key, hash(data), signature)
	case *rsa.PublicKey:
		return rsa.VerifyPKCS1v15(key, cstd.SHA256, hash(data), signature)
	default:
		return fmt.Errorf("unsupported public key type: %T", key)
	}
}

func GenerateKeyPair(randomness io.Reader) (interface{}, interface{}, error) {
	return generateEcdsaKeyPair(randomness)
}

func hash(data [][]byte) []byte {
	h := sha256.New()
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

func PrivKeyFromBytes(raw []byte) (interface{}, error) {
	pk, err := x509.ParsePKCS8PrivateKey(raw)
	if err != nil {
		return nil, err
	}
	switch p := pk.(type) {
	case *ecdsa.PrivateKey, *rsa.PrivateKey:
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported private key type: %T", p)
	}
}

func PubKeyFromBytes(raw []byte) (interface{}, error) {
	pk, err := x509.ParsePKIXPublicKey(raw)
	if err != nil {
		return nil, err
	}
	switch p := pk.(type) {
	case *ecdsa.PublicKey, *rsa.PublicKey:
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported public key type: %T", p)
	}
}

func BytesToStr(h []byte) string {
	return base64.RawStdEncoding.EncodeToString(h)
}

func SrtToBytes(s string) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(s)
}

func SerializePubKey(pubKey interface{}) (pubKeyBytes []byte, err error) {
	switch key := pubKey.(type) {
	case *ecdsa.PublicKey, *rsa.PublicKey:
		return x509.MarshalPKIXPublicKey(key)
	default:
		return nil, fmt.Errorf("unsupported public key type: %T", key)
	}
}

func SerializePrivKey(privKey interface{}) (privKeyBytes []byte, err error) {
	switch key := privKey.(type) {
	case *ecdsa.PrivateKey, *rsa.PrivateKey:
		return x509.MarshalPKCS8PrivateKey(key)
	default:
		return nil, fmt.Errorf("unsupported private key type: %T", key)
	}
}

func PubKeyFromFile(file string) (interface{}, error) {
	certBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	if block.Type == "PUBLIC KEY" {
		return PubKeyFromBytes(block.Bytes)
	}
	if block.Type == "CERTIFICATE" {
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}

		switch p := cert.PublicKey.(type) {
		case *ecdsa.PublicKey, *rsa.PublicKey:
			return p, nil
		default:
			return nil, fmt.Errorf("unsupported public key type: %T", p)
		}
	}
	return nil, fmt.Errorf("failed to find public key in the PEM block")
}

func PrivKeyFromFile(file string) (interface{}, error) {
	certBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(certBytes)
	for block != nil {
		key, err := privKeyFromPEMBlock(block)
		if err == nil {
			return key, nil
		} else {
			block, rest = pem.Decode(rest)
		}
	}
	return nil, fmt.Errorf("no valid key PEM block found")
}

func privKeyFromPEMBlock(block *pem.Block) (interface{}, error) {
	if block == nil {
		return nil, fmt.Errorf("PEM block is nil")
	} else if !strings.Contains(block.Type, "PRIVATE KEY") {
		return nil, fmt.Errorf("wrong PEM block type: %s", block.Type)
	} else {
		return PrivKeyFromBytes(block.Bytes)
	}
}
