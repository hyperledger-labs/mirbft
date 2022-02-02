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
	"strconv"
	"testing"
)

func TestTBLSSigShareVerification(t *testing.T) {
	f := 3
	n := 10
	pub, privShares := TBLSKeyGeneration(f, n)

	msg := []byte("Hello World!")

	sigShares := make([][]byte, 0, 0)
	for i := 0; i < n; i++ {
		sigShare, err := TBLSSigShare(privShares[i], msg)
		if err != nil {
			t.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
		}
		sigShares = append(sigShares, sigShare)
	}

	for i := 0; i < n; i++ {
		err := TBLSSigShareVerification(pub, msg, sigShares[i])
		if err != nil {
			t.Fatalf("Signature share verification of party %d failed: %s", i, err.Error())
		}
	}
}

func TestTBLSRecoverSignature(t *testing.T) {
	f := 3
	n := 10
	pub, privShares := TBLSKeyGeneration(f, n)

	msg := []byte("Hello World!")

	sigShares := make([][]byte, 0, 0)
	for i := 0; i < n; i++ {
		sigShare, err := TBLSSigShare(privShares[i], msg)
		if err != nil {
			t.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
		}
		sigShares = append(sigShares, sigShare)
	}

	signature, err := TBLSRecoverSignature(pub, msg, sigShares, f, n)
	if err != nil {
		t.Fatalf("Signature recovery failed: %s", err.Error())
	}

	err = TBLSVerifySingature(pub, msg, signature)
	if err != nil {
		t.Fatalf("Signature verification failed: %s", err.Error())
	}
}

func TestSerialization(t *testing.T) {
	f := 3
	n := 10
	pub, privShares := TBLSKeyGeneration(f, n)
	spk, err := TBLSPubKeyToBytes(pub)
	if err != nil {
		t.Fatalf("Public key serialization failed: %s", err.Error())
	}

	recoveredPub, err := TBLSPubKeyFromBytes(spk)
	if err != nil {
		t.Fatalf("Public key deserialization failed: %s", err.Error())
	}

	ssks, err := TBLSPrivKeyShareΤοBytes(privShares[0])
	if err != nil {
		t.Fatalf("Private key share serialization failed: %s", err.Error())
	}

	recoveredPrivShare, err := TBLSPrivKeyShareFromBytes(ssks)
	if err != nil {
		t.Fatalf("Private key share deserialization failed: %s", err.Error())
	}

	msg := []byte("Hello World!")

	sigShare, err := TBLSSigShare(recoveredPrivShare, msg)
	if err != nil {
		t.Fatalf("Could not generate signature share: %s", err.Error())
	}

	err = TBLSSigShareVerification(recoveredPub, msg, sigShare)
	if err != nil {
		t.Fatalf("Signature share verification failed: %s", err.Error())
	}

}

func BenchmarkTBLSSigShare(b *testing.B) {
	batchSize := []int{128, 256, 512, 1024, 2048, 4096}
	msgSize := 512
	f := 1

	for _, bs := range batchSize {
		n := 3*f + 1
		msg := make([]byte, bs*msgSize, bs*msgSize)

		_, privShares := TBLSKeyGeneration(f, n)

		name := "N:" + strconv.Itoa(n) + "_" + "batch:" + strconv.Itoa(int(bs))
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := TBLSSigShare(privShares[0], msg)
				if err != nil {
					b.Fatalf("Could not generate signature share of party %d: %s", 0, err.Error())
				}
			}
		})
	}

}

func BenchmarkTBLSSigShareVerification(b *testing.B) {
	batchSize := []int{128, 256, 512, 1024, 2048, 4096}
	msgSize := 512
	f := 1

	for _, bs := range batchSize {
		n := 3*f + 1
		msg := make([]byte, bs*msgSize, bs*msgSize)

		pub, privShares := TBLSKeyGeneration(f, n)

		sigShare, err := TBLSSigShare(privShares[0], msg)
		if err != nil {
			b.Fatalf("Could not generate signature share of party %d: %s", 0, err.Error())
		}

		name := "N:" + strconv.Itoa(n) + "_" + "batch:" + strconv.Itoa(int(bs))

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := TBLSSigShareVerification(pub, msg, sigShare)
				if err != nil {
					b.Fatalf("Signature share verification of party %d failed: %s", i, err.Error())
				}
			}
		})
	}
}

func BenchmarkTBLSVerifySignature(b *testing.B) {
	batchSize := []int{128, 256, 512, 1024, 2048, 4096}
	msgSize := 512
	f := 1

	for _, bs := range batchSize {
		n := 3*f + 1
		msg := make([]byte, bs*msgSize, bs*msgSize)

		pub, privShares := TBLSKeyGeneration(f, n)

		sigShares := make([][]byte, 0, 0)
		for i := 0; i < n; i++ {
			sigShare, err := TBLSSigShare(privShares[i], msg)
			if err != nil {
				b.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
			}
			sigShares = append(sigShares, sigShare)
		}

		sig, err := TBLSRecoverSignature(pub, msg, sigShares, f, n)
		if err != nil {
			b.Fatalf("Signature recovery failed: %s", err.Error())
		}

		name := "N:" + strconv.Itoa(n) + "_" + "batch:" + strconv.Itoa(int(bs))
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err = TBLSVerifySingature(pub, msg, sig)
				if err != nil {
					b.Fatalf("Signature verification failed: %s", err.Error())
				}
			}
		})
	}
}

func BenchmarkTBLSRecoverSignature(b *testing.B) {
	batchSize := []int{128, 256, 512, 1024, 2048, 4096}
	msgSize := 512

	for f := 1; f <= 42; f++ {
		for _, bs := range batchSize {
			n := 3*f + 1
			msg := make([]byte, bs*msgSize, bs*msgSize)

			pub, privShares := TBLSKeyGeneration(f, n)

			sigShares := make([][]byte, 0, 0)
			for i := 0; i < n; i++ {
				sigShare, err := TBLSSigShare(privShares[i], msg)
				if err != nil {
					b.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
				}
				sigShares = append(sigShares, sigShare)
			}

			name := "N:" + strconv.Itoa(n) + "_" + "batch:" + strconv.Itoa(int(bs))
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := TBLSRecoverSignature(pub, msg, sigShares, f, n)
					if err != nil {
						b.Fatalf("Signature recovery failed: %s", err.Error())
					}
					if err != nil {
						b.Fatalf(err.Error())
					}
				}
			})
		}
	}
}

func BenchmarkTBLSVerifySharesAndRecover(b *testing.B) {
	batchSize := []int{128, 256, 512, 1024, 2048, 4096}
	msgSize := 512

	for f := 1; f <= 42; f++ {
		for _, bs := range batchSize {
			n := 3*f + 1
			msg := make([]byte, bs*msgSize, bs*msgSize)

			pub, privShares := TBLSKeyGeneration(f, n)

			sigShares := make([][]byte, 0, 0)
			for i := 0; i < n; i++ {
				sigShare, err := TBLSSigShare(privShares[i], msg)
				if err != nil {
					b.Fatalf("Could not generate signature share of party %d: %s", i, err.Error())
				}
				sigShares = append(sigShares, sigShare)
			}

			name := "N:" + strconv.Itoa(n) + "_" + "batch:" + strconv.Itoa(int(bs))
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := TBLSVerifySharesAndRecoverSignature(pub, msg, sigShares, f, n)
					if err != nil {
						b.Fatalf("Signature recovery failed: %s", err.Error())
					}
					if err != nil {
						b.Fatalf(err.Error())
					}
				}
			})
		}
	}
}
