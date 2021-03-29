/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
	"io/ioutil"
	"strings"
)

func ParseCertPEM(certFile string) ([]byte, error) {
	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	var b *pem.Block
	for {
		b, certBytes = pem.Decode(certBytes)
		if b == nil {
			break
		}
		if b.Type == "CERTIFICATE" {
			break
		}
	}

	if b == nil {
		return nil, fmt.Errorf("no certificate found")
	}

	return b.Bytes, nil
}

func Hash2str(h []byte) string {
	return base64.RawStdEncoding.EncodeToString(h)
}

func Srt2raw(s string) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(s)
}

func Hash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func MerkleHashDigests(digests [][]byte) []byte {
	for len(digests) > 1 {
		var nextDigests [][]byte
		var prev []byte
		for _, d := range digests {
			if prev == nil {
				prev = d
			} else {
				h := sha256.New()
				h.Write(prev)
				h.Write(d)
				nextDigests = append(nextDigests, h.Sum(nil))
				prev = nil
			}
		}
		if prev != nil {
			nextDigests = append(nextDigests, prev)
		}
		digests = nextDigests
	}

	if len(digests) == 0 {
		return nil
	}
	return digests[0]
}

func Sign(hash []byte, sk interface{}) ([]byte, error) {
	var sig []byte
	var err error
	switch pvk := sk.(type) {
	case *rsa.PrivateKey:
		sig, err = pvk.Sign(crand.Reader, hash[:], cstd.SHA256)
		if err != nil {
			panic(err)
		}
	case *ecdsa.PrivateKey:
		sig, err = SignECDSASignature(pvk, hash)
		if err != nil {
			panic(err)
		}
	default:
		return nil, fmt.Errorf("unsupported public key type")
	}
	return sig, nil
}

func CheckSig(hash []byte, pk interface{}, sig []byte) error {
	switch p := pk.(type) {
	case *ecdsa.PublicKey:
		return VerifyECDSASignature(p, hash, sig)
	case *rsa.PublicKey:
		err := rsa.VerifyPKCS1v15(p, cstd.SHA256, hash[:], sig)
		return err
	default:
		return fmt.Errorf("unsupported public key type")
	}
}

func PublicKeyToBytes(pk interface{}) (pkBytes []byte, err error) {
	switch p := pk.(type) {
	case *ecdsa.PublicKey:
		return x509.MarshalPKIXPublicKey(p)
	case *rsa.PublicKey:
		return x509.MarshalPKIXPublicKey(p)
	default:
		return nil, fmt.Errorf("unsupported public key type")
	}
}

func PublicKeyFromBytes(raw []byte) (interface{}, error) {
	pk, err := x509.ParsePKIXPublicKey(raw)
	if err != nil {
		return nil, err
	}
	switch p := pk.(type) {
	case *ecdsa.PublicKey:
		return p, nil
	case *rsa.PublicKey:
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported public key type")
	}
}

func PublicKeyFromFile(file string) (interface{}, error) {
	certBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	if block.Type == "PUBLIC KEY" {
		return PublicKeyFromBytes(block.Bytes)
	}
	if block.Type == "CERTIFICATE" {
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		pk, err := cert.PublicKey, nil
		if err != nil {
			return nil, err
		}
		switch p := pk.(type) {
		case *ecdsa.PublicKey:
			return p, nil
		case *rsa.PublicKey:
			return p, nil
		default:
			return nil, fmt.Errorf("unsupported public key type")
		}
	}
	return nil, fmt.Errorf("failed to find public key in the PEM block")
}

func PrivateKeyFromBytes(raw []byte) (interface{}, error) {
	pk, err := x509.ParsePKCS8PrivateKey(raw)
	if err != nil {
		return nil, err
	}
	switch p := pk.(type) {
	case *ecdsa.PublicKey:
		return p, nil
	case *rsa.PublicKey:
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported private key type")
	}
}

func PrivateKeyFromFile(file string) (interface{}, error) {
	certBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(certBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	if strings.Contains(block.Type, "PRIVATE KEY") {
		return PrivateKeyFromBytes(block.Bytes)
	}
	return nil, fmt.Errorf("failed to find private key in the PEM block")
}
