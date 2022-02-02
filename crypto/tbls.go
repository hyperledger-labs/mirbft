// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"go.dedis.ch/kyber"
	"go.dedis.ch/kyber/pairing/bn256"
	"go.dedis.ch/kyber/share"
	"go.dedis.ch/kyber/sign/bls"
	"go.dedis.ch/kyber/sign/tbls"
)

type TBLSPubKey struct {
	pubKey  kyber.Point
	pubPoly *share.PubPoly
}

type TBLSPrivKey struct {
	privKey kyber.Scalar
}

type TBLSPubKeyShare struct {
	pubKeyShare *share.PubShare
}

type TBLSPrivKeyShare struct {
	privKeyShare *share.PriShare
}

func TBLSKeyGeneration(t, n int) (*TBLSPubKey, []*TBLSPrivKeyShare) {
	suite := bn256.NewSuite()
	sk, pk := bls.NewKeyPair(suite, suite.RandomStream())

	privShares := make([]*TBLSPrivKeyShare, 0, 0)

	g2 := suite.G2()

	privPoly := share.NewPriPoly(g2, t, sk, suite.RandomStream())
	pubPoly := privPoly.Commit(g2.Point().Base())

	pub := &TBLSPubKey{pubKey: pk, pubPoly: pubPoly}

	for _, x := range privPoly.Shares(n) {
		privShares = append(privShares, &TBLSPrivKeyShare{privKeyShare: x})
	}

	return pub, privShares
}

func TBLSSigShare(privShare *TBLSPrivKeyShare, msg []byte) ([]byte, error) {
	suite := bn256.NewSuite()
	return tbls.Sign(suite, privShare.privKeyShare, msg)
}

func TBLSSigShareVerification(pubKey *TBLSPubKey, msg []byte, sigShare []byte) error {
	suite := bn256.NewSuite()
	return tbls.Verify(suite, pubKey.pubPoly, msg, sigShare)
}

func TBLSVerifySharesAndRecoverSignature(pubKey *TBLSPubKey, msg []byte, sigShares [][]byte, t, n int) ([]byte, error) {
	suite := bn256.NewSuite()
	return tbls.Recover(suite, pubKey.pubPoly, msg, sigShares, t, n)
}

func TBLSRecoverSignature(pubKey *TBLSPubKey, msg []byte, sigShares [][]byte, t, n int) ([]byte, error) {
	suite := bn256.NewSuite()
	public := pubKey.pubPoly
	pubShares := make([]*share.PubShare, 0)
	for _, sig := range sigShares {
		s := tbls.SigShare(sig)
		i, err := s.Index()
		if err != nil {
			return nil, err
		}
		if err = bls.Verify(suite, public.Eval(i).V, msg, s.Value()); err != nil {
			return nil, err
		}
		point := suite.G1().Point()
		if err := point.UnmarshalBinary(s.Value()); err != nil {
			return nil, err
		}
		pubShares = append(pubShares, &share.PubShare{I: i, V: point})
		if len(pubShares) >= t {
			break
		}
	}
	commit, err := share.RecoverCommit(suite.G1(), pubShares, t, n)
	if err != nil {
		return nil, err
	}
	sig, err := commit.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return sig, nil
}

func TBLSVerifySingature(pubKey *TBLSPubKey, msg []byte, signature []byte) error {
	suite := bn256.NewSuite()
	return bls.Verify(suite, pubKey.pubKey, msg, signature)
}

type serializablePubKey struct {
	PubKey        []byte
	PubPolyBase   []byte
	PubPoyCommits [][]byte
}

func TBLSPubKeyToBytes(pub *TBLSPubKey) ([]byte, error) {
	serializedPubKey, err := pub.pubKey.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not serialize TBLS public key")
	}
	polyBase, polyCommits := pub.pubPoly.Info()
	serializedPolyBase, err := polyBase.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not serialize TBLS public polynomial base")
	}
	serializedCommits := make([][]byte, len(polyCommits))
	for i, commit := range polyCommits {
		serializedCommits[i], err = commit.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("could not serialize TBLS public polynomial commits")
		}
	}
	spk := serializablePubKey{
		PubKey:        serializedPubKey,
		PubPolyBase:   serializedPolyBase,
		PubPoyCommits: serializedCommits,
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err = enc.Encode(spk)
	if err != nil {
		return nil, fmt.Errorf("could not not encode public key: %s", err.Error())
	}
	return buffer.Bytes(), nil
}

func TBLSPubKeyFromBytes(pub []byte) (*TBLSPubKey, error) {
	suite := bn256.NewSuite()
	g2 := suite.G2()

	var buffer bytes.Buffer
	buffer.Write(pub)

	var spk serializablePubKey
	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(&spk)
	if err != nil {
		return nil, fmt.Errorf("could not not decode public key: %s", err.Error())
	}
	serializedBase := spk.PubPolyBase

	base := kyber.Group.Point(g2)
	err = base.UnmarshalBinary(serializedBase)
	if err != nil {
		return nil, fmt.Errorf("could not deserialize TBLS public polynomial base")
	}
	commits := make([]kyber.Point, len(spk.PubPoyCommits))
	for i, serializedCommit := range spk.PubPoyCommits {
		commits[i] = kyber.Group.Point(g2)
		err = commits[i].UnmarshalBinary(serializedCommit)

	}
	pubPoly := share.NewPubPoly(g2, base, commits)

	pubKey := kyber.Group.Point(g2)
	pubKey.UnmarshalBinary(spk.PubKey)

	return &TBLSPubKey{pubKey: pubKey, pubPoly: pubPoly}, nil
}

type serializablePrivKeyShare struct {
	I int
	V []byte
}

func TBLSPrivKeyShareΤοBytes(priv *TBLSPrivKeyShare) ([]byte, error) {
	serializedPrivKeyShare, err := priv.privKeyShare.V.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not serialize TBLS secret key share")
	}

	spks := serializablePrivKeyShare{
		I: priv.privKeyShare.I,
		V: serializedPrivKeyShare,
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	encErr := enc.Encode(spks)
	if err != nil {
		return nil, fmt.Errorf("could not not encode TBLS secret key share: %s", encErr.Error())
	}
	return buffer.Bytes(), nil
}

func TBLSPrivKeyShareFromBytes(priv []byte) (*TBLSPrivKeyShare, error) {
	suite := bn256.NewSuite()
	g2 := suite.G2()

	var buffer bytes.Buffer
	buffer.Write(priv)

	var spks serializablePrivKeyShare
	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(&spks)
	if err != nil {
		return nil, fmt.Errorf("could not not decode secret key share: %s", err.Error())
	}

	v := kyber.Group.Scalar(g2)
	err = v.UnmarshalBinary(spks.V)
	if err != nil {
		return nil, fmt.Errorf("could not not decode secret key share: %s", err.Error())
	}
	return &TBLSPrivKeyShare{privKeyShare: &share.PriShare{I: spks.I, V: v}}, nil
}

type serializablePubKeyShare struct {
	I int
	V []byte
}

func TBLSPubKeyShareΤοBytes(pub *TBLSPubKeyShare) ([]byte, error) {
	serializedPubKeyShare, err := pub.pubKeyShare.V.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("could not serialize TBLS public key share")
	}

	spks := serializablePubKeyShare{
		I: pub.pubKeyShare.I,
		V: serializedPubKeyShare,
	}
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	encErr := enc.Encode(spks)
	if err != nil {
		return nil, fmt.Errorf("could not not encode TBLS public key share: %s", encErr.Error())
	}
	return buffer.Bytes(), nil
}

func TBLSPubKeyShareFromBytes(pub []byte) (*TBLSPubKeyShare, error) {
	suite := bn256.NewSuite()
	g2 := suite.G2()

	var buffer bytes.Buffer
	buffer.Write(pub)

	var spks serializablePubKeyShare
	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(&spks)
	if err != nil {
		return nil, fmt.Errorf("could not not decode public key share: %s", err.Error())
	}

	v := kyber.Group.Point(g2)
	err = v.UnmarshalBinary(spks.V)
	if err != nil {
		return nil, fmt.Errorf("could not not decode public key share: %s", err.Error())
	}
	return &TBLSPubKeyShare{pubKeyShare: &share.PubShare{I: spks.I, V: v}}, nil
}
