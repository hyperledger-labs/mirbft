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

package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/IBM/mirbft/connection"
	"github.com/IBM/mirbft/crypto"
	"github.com/IBM/mirbft/mir"
	"github.com/IBM/mirbft/persist"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

type Backend struct {
	maxReqCount uint64

	lock     sync.RWMutex
	streamLock sync.RWMutex

	batchMux sync.Mutex

	messages    []chan<- *pb.MultiChainMsg
	persistence *persist.Persist
	reqs        []*pb.Request // replacing consenter support

	self *PeerInfo

	peerInfo map[string]*PeerInfo // address to PeerInfo mapping

	// TODO: make this map thread safe
	clientLock sync.RWMutex
	clientInfo map[string]*clientInfo // address to ClientInfo mapping

	peerIdToAddress map[uint64]string // src to address mapping

	// chainId to instance mapping
	receivers map[string]mir.Receiver
	batches   map[string][]*pb.Batch
	ledger    map[string]map[string]*pb.Batch

	wg sync.WaitGroup	// used to wait until all peer connections are established
}

type clientInfo struct {
	id     uint64
	stream pb.Consensus_RequestServer
}

func NewBackend(N int, persist *persist.Persist, maxReqCount uint64, certFile string, keyFile string) (*Backend, error) {
	log.Critical("Mir backend")
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	c := &Backend{
		maxReqCount:     maxReqCount,
		messages:        make([]chan<- *pb.MultiChainMsg, N, N),
		persistence:     persist,
		reqs:            make([]*pb.Request, 0),
		peerInfo:        make(map[string]*PeerInfo),
		clientInfo:      make(map[string]*clientInfo),
		peerIdToAddress: make(map[uint64]string),
		receivers:       make(map[string]mir.Receiver),
		batches:         make(map[string][]*pb.Batch),
		ledger:          make(map[string]map[string]*pb.Batch),
		self:            &PeerInfo{sk: cert.PrivateKey},
	}
	return c, nil
}

func (c *Backend) AddConnection(peers map[string][]byte, peersToId map[string]uint64, conn *connection.Manager) error {
	var peerInfo []*PeerInfo
	for addr, cert := range peers {
		pi, err := connection.NewPeerInfo(addr, cert)
		if err != nil {
			return err
		}
		cpi := &PeerInfo{info: pi}
		cpi.id = peersToId[addr]
		if pi.Fingerprint() == conn.Self.Fingerprint() {
			c.self.info = cpi.info
			c.self.id = cpi.id
		}
		peerInfo = append(peerInfo, cpi)
		c.peerInfo[pi.Fingerprint()] = cpi
		c.peerIdToAddress[cpi.id] = pi.Fingerprint()
	}

	for _, peer := range c.peerInfo {
		if peer == c.self {
			continue
		}
		go c.connectWorker(peer, conn)
	}

	log.Infof("we are replica %d (%s)", c.self.id, c.self.info)
	pb.RegisterConsensusServer(conn.Server, (*Backend)(c))
	log.Info("registered service")
	return nil
}

func (b *Backend) AddClient(clientPk string, clientId uint64, stream pb.Consensus_RequestServer) {
	b.clientLock.Lock()
	b.clientInfo[clientPk] = &clientInfo{id: clientId, stream: stream}
	b.clientLock.Unlock()
}

// Order enqueues an Envelope for a chainId for ordering, marshalling it first
func (b *Backend) Order(chainID string, msg *pb.Msg, configSeq uint64) bool {
	_, err := proto.Marshal(msg)
	if err != nil {
		return false
	}
	b.enqueueRequest("chain", msg, b.self.id)
	return true
}

// Broadcast sends to all external SBFT peers
func (b *Backend) Broadcast(msg *pb.MultiChainMsg) error {
	b.lock.RLock()
	for _, ch := range b.messages {
		ch <- msg
	}
	b.lock.RUnlock()
	return nil
}

// Unicast sends to a specific external SBFT peer identified by chainId and dest
func (b *Backend) Unicast(chainID string, msg *pb.Msg, dest uint64) error {
	b.messages[dest] <- &pb.MultiChainMsg{Msg: msg, ChainID: chainID}
	return nil
}

func (b *Backend) enqueueConnection(recvType string, peerid uint64) {
	b.receivers["manager"].Connection(peerid)
	b.receivers["protocol"].Connection(peerid)
}

func (b *Backend) enqueueRequest(recvType string, msg *pb.Msg, src uint64) (bool, uint64, error) {
	return b.receivers["request"].Request(msg, src)
}

func (b *Backend) enqueueForReceive(recvType string, msg *pb.Msg, src uint64) {
	b.receivers[recvType].Receive(msg, src)
}

func (b *Backend) connectWorker(peer *PeerInfo, conn *connection.Manager) {
	timeout := 1 * time.Second
	log.Info(peer.id)
	delay := time.After(1 * timeout)
	for {
		// pace reconnect attempts
		<-delay

		// set up for next
		delay = time.After(timeout)

		log.Infof("connecting to replica %d (%s)", peer.id, peer.info)
		conn, err := conn.DialPeer(peer.info, grpc.WithBlock(), grpc.WithTimeout(timeout))
		//conn, err := b.conn.DialPeer(peer.info)
		if err != nil {
			log.Warningf("could not connect to replica %d (%s): %s", peer.id, peer.info, err)
			continue
		}

		ctx := context.TODO()

		client := pb.NewConsensusClient(conn)
		consensus, err := client.Consensus(ctx, &pb.Handshake{})
		if err != nil {
			log.Warningf("could not establish consensus stream with replica %d (%s): %s", peer.id, peer.info, err)
			continue
		}
		log.Noticef("connection to replica %d (%s) established", peer.id, peer.info)
		b.wg.Done()

		for {
			msg, err := consensus.Recv()
			if err == io.EOF {
				log.Warningf("consensus stream with replica %d (%s) broke: %v", peer.id, peer.info, err)
				break
			}
			if err != nil {
				log.Warningf("consensus stream with replica %d (%s) broke: %v", peer.id, peer.info, err)
				break
			}
			b.enqueueForReceive(msg.ChainID, msg.Msg, peer.id)
		}
	}

}

// ========================================================================================

// Implementation of the System Interface
func (t Backend) AddReceiver(recvType string, recv mir.Receiver) {
	if t.receivers == nil {
		t.receivers = make(map[string]mir.Receiver)
	}
	t.receivers[recvType] = recv
	go recv.Run()
}

func (t Backend) Send(chainID string, msg *pb.Msg, dest uint64) {
	if dest == t.self.id {
		t.enqueueForReceive(chainID, msg, t.self.id)
		return
	}
	t.Unicast(chainID, msg, dest)
}

func (t Backend) Deliver(chainId string, batch *pb.Batch) {
	t.batches[chainId][batch.DecodeHeader().Seq] = batch
}

func (t Backend) Persist(chainId string, key string, data proto.Message) {
	compk := fmt.Sprintf("chain-%s-%s", chainId, key)
	if data == nil {
		t.persistence.DelState(compk)
	} else {
		bytes, err := proto.Marshal(data)
		if err != nil {
			panic(err)
		}
		t.persistence.StoreState(compk, bytes)
	}
}

func (t Backend) Restore(chainId string, key string, out proto.Message) bool {
	compk := fmt.Sprintf("chain-%s-%s", chainId, key)
	val, err := t.persistence.ReadState(compk)
	if err != nil {
		return false
	}
	err = proto.Unmarshal(val, out)
	if err != nil {
		log.Error(err)
	}
	return (err == nil)
}

func (t Backend) LastBatch(chainId string) *pb.Batch {
	t.batchMux.Lock()
	defer t.batchMux.Unlock()
	if len(t.batches[chainId]) == 0 {
		return t.receivers[chainId].(*mir.Dispatcher).GetSBFT().MakeBatch(0, nil, nil, nil, nil, 0)
	}
	return t.batches[chainId][len(t.batches[chainId])-1]
}

func (t Backend) Sign(data []byte) []byte {
	hash := crypto.Hash(data)
	privateKey := t.self.sk
	var sig []byte
	sig, err := crypto.Sign(hash, privateKey)
	if err != nil {
		panic(err)
	}
	return sig
}

func (t Backend) CheckSig(data []byte, src uint64, sig []byte) error {
	hash := crypto.Hash(data)
	cert := t.peerInfo[t.peerIdToAddress[src]].info.Cert()
	publicKey := cert.PublicKey
	return crypto.CheckSig(hash, publicKey, sig)
}

func (t Backend) GetPeerInfo(src uint64) *connection.PeerInfo {
	return &t.peerInfo[t.peerIdToAddress[src]].info
}

func (t Backend) DeleteAll() {
	// this will delete all state regardless of the chainid
	t.persistence.DelAllState()
}

func (t Backend) Reconnect(chainId string, replica uint64) {
	t.enqueueConnection(chainId, replica)
}

func (t Backend) Validate(chainID string, req *pb.Request) ([][]*pb.Request, bool) {
	panic("Not using the system validate method anymore")
	return nil, true
}

func (t Backend) Cut(chainID string) []*pb.Request {
	panic("Not using the system cut method anymore")
	return nil
}

// Consensus implements the SBFT consensus gRPC interface
func (c *Backend) Consensus(_ *pb.Handshake, srv pb.Consensus_ConsensusServer) error {
	pi := connection.GetPeerInfo(srv)
	peer, ok := c.peerInfo[pi.Fingerprint()]

	if !ok || !peer.info.Cert().Equal(pi.Cert()) {
		log.Infof("rejecting connection from unknown replica %s", pi)
		return fmt.Errorf("unknown peer certificate")
	}

	ch := make(chan *pb.MultiChainMsg, msgChanCapacity)
	c.messages[peer.id] = ch

	var err error
	for msg := range ch {
		err = srv.Send(msg)
		if err != nil {
			log.Infof("lost connection from replica %d (%s): %s", peer.id, pi, err)
		}
	}
	return nil
}

func (c *Backend) Request(stream pb.Consensus_RequestServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// Register client if new
		id := crypto.Hash2str(msg.Request.PubKey)
		c.clientLock.RLock()
		_, ok := c.clientInfo[id]
		c.clientLock.RUnlock()
		if !ok {
			log.Infof("New client %x", id)
			c.AddClient(id, msg.Request.Client, stream)
			log.Critical(c.clientInfo)
		}

		log.Info("received message from client")
		correctReplica, target, err2 := c.enqueueRequest("request", &pb.Msg{Type: &pb.Msg_Request{Request: msg.Request}}, c.self.id)
		if err2 != nil {
			c.streamLock.Lock()
			if err := stream.Send(&pb.ResponseMessage{Src: c.self.id, Request: &pb.Request{Seq: msg.Request.Seq}, Response: []byte("NACK")}); err != nil {
				c.streamLock.Unlock()
				return err
			}
			c.streamLock.Unlock()
		} else {
			if correctReplica {
				c.streamLock.Lock()
				if err := stream.Send(&pb.ResponseMessage{Src: c.self.id, Request: nil, Response: []byte("ACK")}); err != nil {
					c.streamLock.Unlock()
					return err
				}
				c.streamLock.Unlock()
			} else {
				c.streamLock.Lock()
				if err := stream.Send(&pb.ResponseMessage{Src: c.self.id, Request: &pb.Request{Seq: msg.Request.Seq, Client: target}, Response: []byte("BUCKETS")}); err != nil {
					c.streamLock.Unlock()
					return err
				}
				c.streamLock.Unlock()
			}

		}
	}
	return nil
}

// Sends a response to a client request.
func (c *Backend) Response(id string, response *pb.ResponseMessage) {
	// Check whether the client connection is present.
	c.clientLock.RLock()
	client, ok := c.clientInfo[id]
	c.clientLock.RUnlock()
	if ok {
		// Try sending the response.
		log.Infof("Responding to client %x for %d", id, response.Request.Seq)
		c.streamLock.Lock()
		if err := client.stream.(pb.Consensus_RequestServer).Send(response); err != nil {
			// Log sending error.
			log.Infof("Error responding to client %d. No connection present.", id)
			c.streamLock.Unlock()
			return
		}
		c.streamLock.Unlock()
		tracing.MainTrace.Event(tracing.RESP_SEND, int64(client.id), int64(response.Request.Seq))
	} else {
		// Log error if connection is not present.
		log.Infof("Error responding to client %x. No client not registered.", id)
		return
	}
}
