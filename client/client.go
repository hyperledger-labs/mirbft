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

package main

import (
	crand "crypto/rand"
	"crypto/x509"
	"io/ioutil"
	"math/big"
	"sync"
	"time"

	"github.com/IBM/mirbft/crypto"
	"github.com/IBM/mirbft/mir"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sync/atomic"
)

const limit = 10                                                                                       // trials to generate a random number
const hspace string = "115792089237316195423570985008687907853269984665640564039457584007913129639936" //2^256
const maxMessageSize int = 1073741824
const L = 256 // security parameter

type Client struct {
	inOrderDeliveryLock sync.Mutex
	registrationsLock sync.Mutex
	submitTimestampsLock sync.RWMutex
	sentTimestampsLock sync.RWMutex
	id        int
	n         int // ordering service size
	f         int
	receivers int
	period    int
	pk        interface{} // pem cert public key
	sk        interface{} // pem private key

	registrations map[uint64]bool // to how many nodes the client has registered
	registered    chan bool       // blocking channel until the client has registered to all nodes

	queue         []*requestInfo
	next          chan *requestInfo
	delivered     []int32 // counting how many nodes have delivered a request, by request sequence number
	lastDelivered int64 // last in order delivered request sequence number

	servers          []pb.ConsensusClient
	dst              int
	leaders          []uint64
	numRequests      int64
	b                int // the number of buckets per leader in stable mode
	buckets          map[uint64][]*big.Int
	bucketSize       *big.Int
	bucketsToLeaders map[uint64]uint64
	stream           []pb.Consensus_RequestClient

	dialOpts []grpc.DialOption

	totalSent int32 // number of estimated transactions sent by all clients in total
	stop int32 // Set to a non-zero value to stop submitting requests.

	trace tracing.Trace

	// For each request, stores the timestamp of request submission (in microseconds), for calculating latency.
	sentTimestamps   map[uint64]int64
	submitTimestamps map[uint64]int64
}

type requestInfo struct {
	request *pb.Request
	digest  []byte
}

func New(id, n, f, receivers, b, period, dst int, numRequests int64,
	servers []pb.ConsensusClient, serverCaCertFile string, useTLS bool) (*Client, error) {
	// Generate public and private client key
	pk, sk, err := crypto.ECDSAKeyPair()
	if err != nil {
		log.Fatalf("Could not generate ECDSA key pair: %s", err.Error())
	}

	c := &Client{
		id:               id,
		n:                n,
		f:                f,
		receivers:        receivers,
		period:           period,
		pk:               pk,
		sk:               sk,
		b:                b,
		servers:          servers,
		numRequests:      numRequests,
		leaders:          make([]uint64, n),
		bucketsToLeaders: make(map[uint64]uint64),
		stream:           make([]pb.Consensus_RequestClient, n),
		dst:              dst,
		registrations:    make(map[uint64]bool),
		queue:            make([]*requestInfo, numRequests),
		registered:       make(chan bool),
		delivered:        make([]int32, numRequests, numRequests),
		lastDelivered:    -1,
		totalSent: 		  0,
		trace: &tracing.BufferedTrace{
			Sampling:              tracing.TraceSampling,
			BufferCapacity:        tracing.EventBufferSize,
			ProtocolEventCapacity: tracing.EventBufferSize,
			RequestEventCapacity:  tracing.EventBufferSize,
		},
		sentTimestamps:   make(map[uint64]int64, numRequests),
		submitTimestamps: make(map[uint64]int64, numRequests),
	}

	// Set general gRPC dial options.
	c.dialOpts = []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	}

	// All servers have the same CA
	certpool := x509.NewCertPool()
	pem, err := ioutil.ReadFile(serverCaCertFile)
	if err != nil {
		log.Fatalf("Failed to read certificate authority from %s: %s", serverCaCertFile, err.Error())
	}
	if !certpool.AppendCertsFromPEM(pem) {
		log.Fatal("Cannot parse certificate authority.")
	}
	// Add TLS-specific gRPC dial options, depending on configuration
	if useTLS {
		c.dialOpts = append(c.dialOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certpool, "")))
	} else {
		c.dialOpts = append(c.dialOpts, grpc.WithInsecure())
	}

	c.initBuckets()
	for i := 0; i < n; i++ {
		c.leaders[i] = uint64(i)
		for j := 0; j < b; j++ {
			c.bucketsToLeaders[uint64(i*b+j)] = uint64(i)
		}
	}
	for i := uint64(0); i < uint64(b*n); i++ {
		log.Infof("Buckets %d:  Leader: %d, [%d,%d)", i, c.bucketsToLeaders[i], c.buckets[i][0], c.buckets[i][1])
	}
	return c, nil
}

func (c *Client) AddOSN(addr string) (*grpc.ClientConn, error) {
	// Set up a gRPC connection.
	conn, err := grpc.Dial(addr, c.dialOpts...)
	if err != nil {
		log.Error("Couldn't connect to orderer")
		return nil, err
	}

	grpcConn := pb.NewConsensusClient(conn)
	c.servers = append(c.servers, grpcConn)
	return conn, nil
}

func (c *Client) GetOSNs() []pb.ConsensusClient {
	return c.servers
}

func (c *Client) broadcast(msg *pb.RequestMessage, dst int) {
	log.Infof("SENDING %d from %d", msg.Request.Seq, msg.Request.Nonce)
	c.sentTimestampsLock.Lock()
	c.sentTimestamps[msg.Request.Seq] = time.Now().UnixNano() / 1000 // In us
	c.sentTimestampsLock.Unlock()
	c.trace.Event(tracing.REQ_SEND, int64(msg.Request.Seq), 0)
	for inc := dst + 1; inc > dst-c.receivers+1; inc-- {
		dst := (c.n + inc) % c.n
		c.send(msg, dst)
	}
}

func (c *Client) send(msg *pb.RequestMessage, dst int) {
	if err := c.stream[dst].Send(msg) ; err != nil {
		log.Errorf("Failed sending request to %d", dst)
	}
}

func (c *Client) makeRequest(payload []byte, seq uint64, signed bool) *requestInfo {
	var signature []byte
	pkRaw, err := crypto.PublicKeyToBytes(c.pk)
	fatal(err)
	if signed {
		var err error
		hash := mir.RequestHash(payload, seq, pkRaw)
		signature, err = crypto.Sign(hash, c.sk)
		fatal(err)
	}

	log.Debugf("CREATING %d", seq)

	request := &pb.Request{
		Client:    uint64(c.id),
		Seq:       seq,
		Payload:   payload,
		PubKey:    pkRaw,
		Signature: signature,
		Nonce:     uint64(c.dst),
	}
	digest := mir.RequestHash(payload, seq, pkRaw)

	return &requestInfo{request: request, digest: digest}
}

func (c *Client) sendRequest(request *requestInfo) {
	log.Debugf("Sending request %d with size %d", request.request.Seq, len(request.request.Payload))
	m := &pb.RequestMessage{Request: request.request}
	c.send(m, c.dst)
}

func (c *Client) broadcastRequest(request *requestInfo) {
	log.Debugf("Sending request %d with size %d", request.request.Seq, len(request.request.Payload))
	m := &pb.RequestMessage{Request: request.request}
	bucket := c.getBucket(request.digest)
	dst := c.bucketsToLeaders[bucket]
	c.broadcast(m, int(dst))
}

// Registering by sending the first request to all
func (c *Client) register() {
	request := c.queue[0]

	m := &pb.RequestMessage{Request: request.request}

	log.Infof("SENDING %d from %d", m.Request.Seq, m.Request.Nonce)
	c.sentTimestampsLock.Lock()
	c.sentTimestamps[m.Request.Seq] = time.Now().UnixNano() / 1000 // In us
	c.sentTimestampsLock.Unlock()
	c.trace.Event(tracing.REQ_SEND, int64(m.Request.Seq), 0)

	for dst := 0; dst < c.n; dst++ {
		log.Infof("REGISTERING %d to %d", request.request.Seq, dst)
		c.send(m, dst)
	}
}

func RandomBytes(c int) ([]byte, error) {
	b := make([]byte, c)
	var err error
	for i := 0; i < limit; i++ {
		_, err = crand.Read(b)
		if err == nil {
			return b, nil
		}
	}
	return nil, err
}

func (c *Client) initBuckets() {
	c.buckets = make(map[uint64][]*big.Int)
	L := new(big.Int)
	L.SetString(hspace, 10)
	bucketNum := uint64(c.b * c.n)
	size := new(big.Int).Div(L, big.NewInt(int64(bucketNum)))
	c.bucketSize = size
	for i := uint64(0); i < bucketNum; i++ {
		c.buckets[i] = make([]*big.Int, 2, 2)
		c.buckets[i][0] = new(big.Int).Mul(big.NewInt(int64(i)), size)
		c.buckets[i][1] = new(big.Int).Add(c.buckets[i][0], size)
	}
	c.buckets[bucketNum-uint64(1)][1] = L
	log.Debug(c.buckets)
}

// Returns the bucket the digest of a request maps to
func (c *Client) getBucket(digest []byte) uint64 {

	keyInt := new(big.Int)
	keyInt.SetBytes(digest)

	log.Debugf("Request digest as integer %d", keyInt)
	bucket := new(big.Int).Div(keyInt, c.bucketSize)

	i := bucket.Uint64()

	if i >= uint64(len(c.buckets)) {
		log.Panicf("getBucket -- going beyond bucket limits: %d", i)
	}

	b := c.buckets[i]

	if keyInt.Cmp(b[0]) != 1 || keyInt.Cmp(b[1]) == 1 {
		panic("Wrong result in getBucket")
	}

	return i
}

// estimate rotation
func (c *Client) rotateBuckets() {

	max := c.n * c.b

	temp := make([]uint64, c.b, c.b)
	for i := 0; i < c.b; i++ {
		temp[i] = c.bucketsToLeaders[uint64(max-c.b+i)]
	}

	for i := max - 1; i > c.b-1; i-- {
		c.bucketsToLeaders[uint64(i)] = c.bucketsToLeaders[uint64(i-c.b)]
	}

	for i := 0; i < c.b; i++ {
		c.bucketsToLeaders[uint64(i)] = temp[i]
	}

	for i := uint64(0); i < uint64(c.b*c.n); i++ {
		log.Infof("Buckets %d:  Leader: %d, [%d,%d)", i, c.bucketsToLeaders[i], c.buckets[i][0], c.buckets[i][1])
	}
}

func (c *Client) checkInOrderDelivery(seq int64) {
	log.Infof("Checking in-order delivery for: %d", seq)
	if atomic.LoadInt32(&c.delivered[seq]) >= int32(c.f+1) {
		atomic.StoreInt64(&c.lastDelivered, seq)
		log.Infof("DELIVERED: %d", seq)
		c.submitTimestampsLock.RLock()
		c.trace.Event(tracing.REQ_DELIVERED, int64(seq), time.Now().UnixNano()/1000-c.submitTimestamps[uint64(seq)])
		c.submitTimestampsLock.RUnlock()
		if seq+1 < int64(c.numRequests) {
			c.checkInOrderDelivery(seq + 1)
		}
	}
}
