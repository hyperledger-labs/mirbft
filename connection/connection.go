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

package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"crypto/sha256"
	"github.com/op/go-logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var log = logging.MustGetLogger("connection")

const maxMessageSize int = 1073741824

type PeerInfo struct {
	addr string
	cert *x509.Certificate
	cp   *x509.CertPool
}

type Manager struct {
	Server    *grpc.Server
	Listener  net.Listener
	Self      PeerInfo
	tlsConfig *tls.Config
	// dialOpts []grpc.DialOption
	Cert *tls.Certificate
}

// func New(addr string, certFile string, keyFile string, caCertFile string, useTLS bool) (_ *Manager, err error) {
func New(addr string, certFile string, keyFile string) (_ *Manager, err error) {
	c := &Manager{}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}

	//// Load the certificate of the CA (certificate authority).
	//// The CA signs all other certificates used in this code.
	//// All certificates are verified against this.
	//certpool := x509.NewCertPool()
	//pem, err := ioutil.ReadFile(caCertFile)
	//if err != nil {
	//	log.Fatal("Failed to read certificate authority.")
	//}
	//if !certpool.AppendCertsFromPEM(pem) {
	//	log.Fatal("Cannot parse certificate authority.")
	//}

	c.Cert = &cert
	c.Self, _ = NewPeerInfo(addr, cert.Certificate[0])
	log.Infof("New connection for %s\n", c.Self.Fingerprint())

	//c.tlsConfig = ConfigureServerTLS(certFile, keyFile, caCertFile)
	c.tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequestClientCert,
		InsecureSkipVerify: true,
	}

	//// Set general gRPC dial options.
	//c.dialOpts = []grpc.DialOption{
	//	grpc.WithBlock(),
	//	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	//}
	//
	//// Add TLS-specific gRPC dial options, depending on configuration
	//if useTLS {
	//	c.dialOpts = append(c.dialOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certpool, "")))
	//} else {
	//	c.dialOpts = append(c.dialOpts, grpc.WithInsecure())
	//}

	c.Listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	serverTls := c.tlsConfig
	serverTls.ServerName = addr
	c.Server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTls)))
	//c.Server = grpc.NewServer(grpc.Creds(credentials.NewTLS(c.tlsConfig)))
	return c, nil
}

//func (c *Manager) DialPeer(peer PeerInfo) (*grpc.ClientConn, error) {
//	return dialPeer(peer, c.dialOpts)
//}

func (c *Manager) DialPeer(peer PeerInfo, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return dialPeer(&c.tlsConfig.Certificates[0], peer, opts...)
}

// to check client: credentials.FromContext() -> AuthInfo

type patchedAuthenticator struct {
	credentials.TransportCredentials
	pinnedCert    *x509.Certificate
	tunneledError error
}

//func dialPeer(peer PeerInfo, opts []grpc.DialOption) (*grpc.ClientConn, error) {
//	// Set up a gRPC connection.
//
//	conn, err := grpc.Dial(peer.addr, opts...)
//	if err != nil {
//		log.Error("Couldn't connect to peer")
//		return nil, err
//	}
//
//	return conn, nil
//}

func dialPeer(cert *tls.Certificate, peer PeerInfo, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	clientTLS := &tls.Config{InsecureSkipVerify: true}
	if cert != nil {
		clientTLS.Certificates = []tls.Certificate{*cert}
	}

	creds := credentials.NewTLS(clientTLS)
	patchedCreds := &patchedAuthenticator{
		TransportCredentials: creds,
		pinnedCert:           peer.cert,
	}
	opts = append(opts, grpc.WithTransportCredentials(patchedCreds))
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize)))
	conn, err := grpc.Dial(peer.addr, opts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func GetPeerInfo(s grpc.ServerStream) PeerInfo {
	var pi PeerInfo

	ctx := s.Context()
	p, ok := peer.FromContext(ctx)
	if ok {
		pi.addr = p.Addr.String()
	}
	switch creds := p.AuthInfo.(type) {
	case credentials.TLSInfo:
		state := creds.State
		if len(state.PeerCertificates) > 0 {
			pi.cert = state.PeerCertificates[0]
		}
	}

	return pi
}

func NewPeerInfo(addr string, cert []byte) (_ PeerInfo, err error) {
	var p PeerInfo

	p.addr = addr
	p.cert, err = x509.ParseCertificate(cert)
	if err != nil {
		return
	}
	p.cp = x509.NewCertPool()
	p.cp.AddCert(p.cert)
	return p, nil
}

func (pi *PeerInfo) Fingerprint() string {
	return fmt.Sprintf("%x", sha256.Sum256(pi.cert.Raw))
}

func (pi *PeerInfo) Cert() *x509.Certificate {
	cert := *pi.cert
	return &cert
}

func (pi *PeerInfo) Address() string {
	return pi.addr
}

func (pi PeerInfo) String() string {
	return fmt.Sprintf("%.6s [%s]", pi.Fingerprint(), pi.addr)
}
