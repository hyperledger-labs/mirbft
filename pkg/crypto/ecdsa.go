/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
)

// TODO: Write comments.

type ecdsaSignature struct {
	R, S *big.Int
}

func ecdsaSignatureToBytes(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(ecdsaSignature{r, s})
}

func ecdsaSignatureFromBytes(raw []byte) (*big.Int, *big.Int, error) {
	// Unmarshal
	sig := new(ecdsaSignature)
	_, err := asn1.Unmarshal(raw, sig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed unmashalling signature [%s]", err)
	}

	// Validate sig
	if sig.R == nil {
		return nil, nil, errors.New("invalid signature, R must be different from nil")
	}
	if sig.S == nil {
		return nil, nil, errors.New("invalid signature, S must be different from nil")
	}

	if sig.R.Sign() != 1 {
		return nil, nil, errors.New("invalid signature, R must be larger than zero")
	}
	if sig.S.Sign() != 1 {
		return nil, nil, errors.New("invalid signature, S must be larger than zero")
	}

	return sig.R, sig.S, nil
}

func signEcdsa(sk *ecdsa.PrivateKey, hash []byte) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, sk, hash)
	if err != nil {
		return nil, err
	}
	return ecdsaSignatureToBytes(r, s)
}

func verifyEcdsaSignature(pk *ecdsa.PublicKey, hash []byte, signature []byte) error {
	r, s, err := ecdsaSignatureFromBytes(signature)
	if err != nil {
		return err
	}
	ok := ecdsa.Verify(pk, hash, r, s)
	if !ok {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

func generateEcdsaKeyPair(randomness io.Reader) (*ecdsa.PrivateKey, *ecdsa.PublicKey, error) {

	if randomness == nil {
		randomness = rand.Reader
	}

	// TODO: No clue which curve to use, picked P256 because it was in the documentation example.
	//       Check whether this is OK.
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), randomness)
	if err != nil {
		return nil, nil, err
	}

	return privKey, &privKey.PublicKey, nil
}
