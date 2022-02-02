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

package messenger

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
)

// Configures a TLS connection.
// This function is used for both the client and the server part of the connection.
// Returns a TLS configuration to be used when creating a network connection (direct or through gRPC).
func ConfigureTLS(certFile string, keyFile string) *tls.Config {

	// Load key pair / certificate of this node.
	// This is the certificate this node will be presenting to the other side of the connection.
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		logger.Fatal().
			Err(err).
			Str("certFile", certFile).
			Str("keyFile", keyFile).
			Msg("Failed to load keys.")
	}

	// Pre-parse the certificate. This is just an optimization.
	// From the Go tls package documentation:
	// Leaf is the parsed form of the leaf certificate, which may be initialized
	// using x509.ParseCertificate to reduce per-handshake processing. If nil,
	// the leaf certificate will be parsed as needed.
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		logger.Fatal().
			Err(err).
			Str("certFile", certFile).
			Str("keyFile", keyFile).
			Msg("Error parsing certificate.")
	}

	// Load the certificate of the CA (certificate authority).
	// The CA signs all other certificates used in this code.
	// All certificates are verified against this.
	certpool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(config.Config.CACertFile)
	if err != nil {
		logger.Fatal().
			Err(err).
			Str("certFile", certFile).
			Str("keyFile", keyFile).
			Msg("Failed to read certificate authority.")
	}
	if !certpool.AppendCertsFromPEM(pem) {
		logger.Fatal().
			Str("certFile", certFile).
			Str("keyFile", keyFile).
			Msg("Cannot parse certificate authority.")
	}

	return &tls.Config{
		Rand:         rand.Reader,
		Certificates: []tls.Certificate{cert},        // Node's own certificate
		ClientAuth:   tls.RequireAndVerifyClientCert, // Mutual authentication
		ClientCAs:    certpool,                       // For the server side of the TLS connection, for verifying client certificates.
		RootCAs:      certpool,                       // For the client side of the TLS connection, for verifying server certificates.
		//InsecureSkipVerify: true,                   // Uncomment this to skip verification of certificate against the CA.
	}
}
