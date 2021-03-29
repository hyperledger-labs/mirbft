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
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"testing"
)

const cert = `
-----BEGIN CERTIFICATE-----
MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT
MRYwFAYDVQQKEw1HZW9UcnVzdCBJbmMuMRswGQYDVQQDExJHZW9UcnVzdCBHbG9i
YWwgQ0EwHhcNMTMwNDA1MTUxNTU1WhcNMTUwNDA0MTUxNTU1WjBJMQswCQYDVQQG
EwJVUzETMBEGA1UEChMKR29vZ2xlIEluYzElMCMGA1UEAxMcR29vZ2xlIEludGVy
bmV0IEF1dGhvcml0eSBHMjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJwqBHdc2FCROgajguDYUEi8iT/xGXAaiEZ+4I/F8YnOIe5a/mENtzJEiaB0C1NP
VaTOgmKV7utZX8bhBYASxF6UP7xbSDj0U/ck5vuR6RXEz/RTDfRK/J9U3n2+oGtv
h8DQUB8oMANA2ghzUWx//zo8pzcGjr1LEQTrfSTe5vn8MXH7lNVg8y5Kr0LSy+rE
ahqyzFPdFUuLH8gZYR/Nnag+YyuENWllhMgZxUYi+FOVvuOAShDGKuy6lyARxzmZ
EASg8GF6lSWMTlJ14rbtCMoU/M4iarNOz0YDl5cDfsCx3nuvRTPPuj5xt970JSXC
DTWJnZ37DhF5iR43xa+OcmkCAwEAAaOB+zCB+DAfBgNVHSMEGDAWgBTAephojYn7
qwVkDBF9qn1luMrMTjAdBgNVHQ4EFgQUSt0GFhu89mi1dvWBtrtiGrpagS8wEgYD
VR0TAQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAQYwOgYDVR0fBDMwMTAvoC2g
K4YpaHR0cDovL2NybC5nZW90cnVzdC5jb20vY3Jscy9ndGdsb2JhbC5jcmwwPQYI
KwYBBQUHAQEEMTAvMC0GCCsGAQUFBzABhiFodHRwOi8vZ3RnbG9iYWwtb2NzcC5n
ZW90cnVzdC5jb20wFwYDVR0gBBAwDjAMBgorBgEEAdZ5AgUBMA0GCSqGSIb3DQEB
BQUAA4IBAQA21waAESetKhSbOHezI6B1WLuxfoNCunLaHtiONgaX4PCVOzf9G0JY
/iLIa704XtE7JW4S615ndkZAkNoUyHgN7ZVm2o6Gb4ChulYylYbc3GrKBIxbf/a/
zG+FA1jDaFETzf3I93k9mTXwVqO94FntT0QJo544evZG0R0SnU++0ED8Vf4GXjza
HFa9llF7b1cq26KqltyMdMKVvvBulRP/F/A8rLIQjcxz++iPAsbw+zOzlTvjwsto
WHPbqCRiOwY1nQ2pM714A5AuTHhdUDqB1O6gyHA43LL5Z/qHQF1hwFGPa4NrzQU6
yuGnBXj8ytqU0CwIPX4WecigUCAkVDNx
-----END CERTIFICATE-----`

func TestPeerInfoWithWrongCert(t *testing.T) {
	f := "/tmp/dat1"
	err := ioutil.WriteFile(f, []byte(cert), 0644)
	if err != nil {
		panic("Failed to write certificate into file.")
	}
	c, cerr := ParseCertPEM(f)
	if cerr != nil {
		t.Fatalf("Certificate parsing error. (1)")
	}
	if len(c) == 0 {
		t.Fatalf("Certificate parsing error. (2)")
	}
}

func TestSignVerify(t *testing.T) {
	certFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/client.pem"
	keyFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/client.key"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("could not load client key pair: %s", err)
	}
	sk := cert.PrivateKey

	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certBytes)
	certX509, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pk := certX509.PublicKey

	msg := "Hello World"
	hashed := Hash([]byte(msg))

	sigBytes, err := SignECDSASignature(sk.(*ecdsa.PrivateKey), hashed)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = VerifyECDSASignature(pk.(*ecdsa.PublicKey), hashed, sigBytes)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func BenchmarkVerifyECDSASignature(b *testing.B) {
	certFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/client.pem"
	keyFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/client.key"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		b.Fatalf("could not load client key pair: %s", err)
	}
	sk := cert.PrivateKey

	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		b.Fatal(err)
	}
	block, _ := pem.Decode(certBytes)
	certX509, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		b.Fatal(err)
	}
	pk := certX509.PublicKey

	msg := "Hello World"
	hashed := Hash([]byte(msg))

	sigBytes, err := Sign(hashed, sk)
	if err != nil {
		b.Fatalf(err.Error())
	}

	for i := 0; i < b.N; i++ {
		err = CheckSig(hashed, pk, sigBytes)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

func BenchmarkVerifyRSASignature(b *testing.B) {
	certFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/rsa/client.pem"
	keyFile := "/opt/gopath/src/github.ibm.com/sbft/sampleconfig/certs/rsa/client.key"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		b.Fatal(err)
	}
	sk := cert.PrivateKey

	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		b.Fatal(err)
	}
	block, _ := pem.Decode(certBytes)
	certX509, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		b.Fatal(err)
	}
	pk := certX509.PublicKey

	msg := "Hello World"
	hashed := Hash([]byte(msg))

	sigBytes, err := Sign(hashed, sk)
	if err != nil {
		b.Fatalf(err.Error())
	}

	for i := 0; i < b.N; i++ {
		err = CheckSig(hashed, pk, sigBytes)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}
