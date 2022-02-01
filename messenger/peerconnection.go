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
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/membership"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/tracing"
	"github.com/hyperledger-labs/mirbft/util"
)

const (
	// TODO: Remove this ugly ad-hoc sampling and use the mechanism provided by the tracing package
	msgBatchTraceSampling = 128
)

type PeerConnection interface {
	Send(msg *pb.ProtocolMessage)
	SendPriority(msg *pb.ProtocolMessage)
	Close()
}

// ============================================================
// SimpleConnection
// ============================================================

type LockedSimpleConnection struct {
	lock            *sync.Mutex
	priorityLock    *sync.Mutex
	msgSink         pb.Messenger_ListenClient
	priorityMsgSink pb.Messenger_ListenClient
	closed          bool
}

func NewLockedSimpleConnection(msgSink pb.Messenger_ListenClient,
	priorityMsgSink pb.Messenger_ListenClient) *LockedSimpleConnection {
	if priorityMsgSink != nil {
		commonLock := &sync.Mutex{}
		return &LockedSimpleConnection{
			lock:            commonLock,
			priorityLock:    commonLock,
			msgSink:         msgSink,
			priorityMsgSink: priorityMsgSink,
			closed:          false,
		}
	} else {
		return &LockedSimpleConnection{
			lock:            &sync.Mutex{},
			priorityLock:    &sync.Mutex{},
			msgSink:         msgSink,
			priorityMsgSink: msgSink,
			closed:          false,
		}
	}
}

func (lsc *LockedSimpleConnection) Send(msg *pb.ProtocolMessage) {
	lsc.lock.Lock()
	defer lsc.lock.Unlock()

	if lsc.closed {
		return
	}

	if err := lsc.msgSink.Send(msg); err != nil {
		logger.Error().Err(err).Msg("Failed to send protocol message. Dropping all future outgoing protocol messages.")
		lsc.Close()
	}
}

func (lsc *LockedSimpleConnection) SendPriority(msg *pb.ProtocolMessage) {
	lsc.priorityLock.Lock()
	defer lsc.priorityLock.Unlock()

	if lsc.closed {
		return
	}

	if err := lsc.priorityMsgSink.Send(msg); err != nil {
		logger.Error().Err(err).Msg("Failed to send protocol message. Dropping all future outgoing protocol messages.")
		lsc.Close()
	}
}

func (lsc *LockedSimpleConnection) Close() {
	if err := lsc.msgSink.CloseSend(); err != nil {
		logger.Error().Err(err).Msg("Failed to close connection.")
	}
	if lsc.priorityMsgSink != lsc.msgSink {
		if err := lsc.priorityMsgSink.CloseSend(); err != nil {
			logger.Error().Err(err).Msg("Failed to close priority connection.")
		}
	}
	lsc.closed = true
}

// ============================================================
// LockingMultiConnection
// ============================================================

type LockingMultiConnection struct {
	msgSinks         []pb.Messenger_ListenClient
	priorityMsgSinks []pb.Messenger_ListenClient
	locks            []sync.Mutex
	priorityLocks    []sync.Mutex
	nextSink         int32
	nextPrioritySink int32
	closed           bool
}

func NewLockingMultiConnection(msgSinks []pb.Messenger_ListenClient,
	priorityMsgSinks []pb.Messenger_ListenClient) *LockingMultiConnection {
	return &LockingMultiConnection{
		msgSinks:         msgSinks,
		priorityMsgSinks: priorityMsgSinks,
		locks:            make([]sync.Mutex, len(msgSinks), len(msgSinks)),
		priorityLocks:    make([]sync.Mutex, len(priorityMsgSinks), len(priorityMsgSinks)),
		nextSink:         0,
		nextPrioritySink: 0,
		closed:           false,
	}
}

func (lmc *LockingMultiConnection) Send(msg *pb.ProtocolMessage) {

	// The atomic access to the message sink index is required for avoiding a data race.
	// Note that the current implementation does not guarantee an atomic rotation of the index.
	// Multiple concurrent invocations of this function may not result in the corresponding number of
	// index rotations. This is not a problem, however, since it leads only to small unfairness in the
	// utilisation of the underlying connections.
	next := atomic.LoadInt32(&lmc.nextSink)

	lock := &lmc.locks[next]
	lock.Lock()

	if lmc.closed {
		lock.Unlock()
		return
	}

	// Send message to the connection at the current index.
	if err := lmc.msgSinks[next].Send(msg); err != nil {
		logger.Error().Err(err).Msg("Failed to send protocol message. Dropping all future outgoing protocol messages.")
		lmc.Close()
	}

	lock.Unlock()

	// Rotate connection index
	atomic.StoreInt32(&lmc.nextSink, (lmc.nextSink+1)%int32(len(lmc.msgSinks)))

}

func (lmc *LockingMultiConnection) SendPriority(msg *pb.ProtocolMessage) {

	if len(lmc.priorityMsgSinks) == 0 {
		lmc.Send(msg)
		return
	}

	// The atomic access to the message sink index is required for avoiding a data race.
	// Note that the current implementation does not guarantee an atomic rotation of the index.
	// Multiple concurrent invocations of this function may not result in the corresponding number of
	// index rotations. This is not a problem, however, since it leads only to small unfairness in the
	// utilisation of the underlying connections.
	next := atomic.LoadInt32(&lmc.nextPrioritySink)

	lock := &lmc.priorityLocks[next]
	lock.Lock()

	if lmc.closed {
		lock.Unlock()
		return
	}

	// Send message to the connection at the current index.
	err := lmc.priorityMsgSinks[next].Send(msg)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to send protocol message. Dropping all future outgoing protocol messages.")
		lmc.Close()
	}

	lock.Unlock()

	// Rotate connection index
	atomic.StoreInt32(&lmc.nextPrioritySink, (lmc.nextPrioritySink+1)%int32(len(lmc.priorityMsgSinks)))

}

func (lmc *LockingMultiConnection) Close() {
	for _, msgSink := range lmc.msgSinks {
		if err := msgSink.CloseSend(); err != nil {
			logger.Error().Err(err).Msg("Failed to close connection to peer.")
		}
	}
	for _, priorityMsgSink := range lmc.priorityMsgSinks {
		if err := priorityMsgSink.CloseSend(); err != nil {
			logger.Error().Err(err).Msg("Failed to close priority connection to peer.")
		}
	}
	lmc.closed = true
}

// ============================================================
// BufferedMultiConnection
// ============================================================

type BufferedMultiConnection struct {
	msgSinks         []pb.Messenger_ListenClient
	priorityMsgSinks []pb.Messenger_ListenClient

	nextChan         int32
	nextPriorityChan int32

	sendChans         []chan *pb.ProtocolMessage
	sendPriorityChans []chan *pb.ProtocolMessage
}

func NewBufferedMultiConnection(msgSinks []pb.Messenger_ListenClient,
	priorityMsgSinks []pb.Messenger_ListenClient,
	bufSize int) *BufferedMultiConnection {

	bmc := &BufferedMultiConnection{
		msgSinks:          msgSinks,
		priorityMsgSinks:  priorityMsgSinks,
		nextChan:          0,
		nextPriorityChan:  0,
		sendChans:         make([]chan *pb.ProtocolMessage, len(msgSinks), len(msgSinks)),
		sendPriorityChans: make([]chan *pb.ProtocolMessage, len(priorityMsgSinks), len(priorityMsgSinks)),
	}

	for i, msgSink := range bmc.msgSinks {
		msgChan := make(chan *pb.ProtocolMessage, bufSize)
		bmc.sendChans[i] = msgChan
		go bmc.sendMessages(msgChan, msgSink)
	}
	for i, msgSink := range bmc.priorityMsgSinks {
		msgChan := make(chan *pb.ProtocolMessage, bufSize)
		bmc.sendPriorityChans[i] = msgChan
		go bmc.sendMessages(msgChan, msgSink)
	}

	return bmc
}

func (bmc *BufferedMultiConnection) Send(msg *pb.ProtocolMessage) {

	// The atomic access to the channel index is required for avoiding a data race.
	// Note that the current implementation does not guarantee an atomic rotation of channel index.
	// Multiple concurrent invocations of this function may not result in the corresponding number of
	// index rotations. This is not a problem, however, since it leads only to small unfairness in the
	// utilisation of the underlying connections.
	next := atomic.LoadInt32(&bmc.nextChan)

	// Setting next to -1 means that the connection should be considered closed after a failure and all outgoing
	// messages should be ignored.
	if next == -1 {
		return
	}

	// Write message to the corresponding channel
	bmc.sendChans[next] <- msg
	// Rotate channel index
	atomic.StoreInt32(&bmc.nextChan, (next+1)%int32(len(bmc.sendChans)))
}

func (bmc *BufferedMultiConnection) SendPriority(msg *pb.ProtocolMessage) {

	if len(bmc.sendPriorityChans) == 0 {
		bmc.Send(msg)
	} else {

		// The atomic access to the channel index is required for avoiding a data race.
		// Note that the current implementation does not guarantee an atomic rotation of channel index.
		// Multiple concurrent invocations of this function may not result in the corresponding number of
		// index rotations. This is not a problem, however, since it leads only to small unfairness in the
		// utilisation of the underlying connections.
		next := atomic.LoadInt32(&bmc.nextPriorityChan)

		// Setting next to -1 means that the connection should be considered closed after a failure and all outgoing
		// messages should be ignored.
		if next == -1 {
			return
		}

		// Write message to the corresponding channel
		bmc.sendPriorityChans[next] <- msg
		// Rotate channel index
		atomic.StoreInt32(&bmc.nextPriorityChan, (next+1)%int32(len(bmc.sendPriorityChans)))
	}
}

func (bmc *BufferedMultiConnection) Close() {

	for _, ch := range bmc.sendChans {
		close(ch)
	}
	for _, ch := range bmc.sendPriorityChans {
		close(ch)
	}
}

// Reads messages from input channel and submits them to the underlying message sink.
// Closes underlying message sink when any of input channel is closed.
func (bmc *BufferedMultiConnection) sendMessages(msgChan chan *pb.ProtocolMessage, msgSink pb.Messenger_ListenClient) {
	for msg := range msgChan {
		checkForHotStuffProposal(msg, "Sending HotStuff proposal.")
		if err := msgSink.Send(msg); err != nil {
			logger.Error().Err(err).Msg("Failed to send protocol message. Dropping all future outgoing protocol messages.")
			atomic.StoreInt32(&bmc.nextChan, -1)
			atomic.StoreInt32(&bmc.nextPriorityChan, -1)
		}
	}

	if err := msgSink.CloseSend(); err != nil {
		logger.Error().Err(err).Msg("Failed to close connection.")
	}
}

// DEBUG
func checkForHotStuffProposal(msg *pb.ProtocolMessage, logOutput string) {
	switch m := msg.Msg.(type) {
	case *pb.ProtocolMessage_Proposal:
		n := m.Proposal.Node
		logger.Debug().
			Int32("senderId", msg.SenderId).
			Int32("sn", msg.Sn).
			Int32("height", n.Height).
			Int32("view", n.View).
			Int("nReq", len(n.Batch.Requests)).
			Msg(logOutput)
	}
}

// ============================================================
// BatchedConnection
// ============================================================

type BatchedConnection struct {
	peerConnection PeerConnection
	msgBuffer      *util.ChannelBuffer
}

func NewBatchedConnection(pc PeerConnection, period time.Duration) *BatchedConnection {

	bc := &BatchedConnection{
		peerConnection: pc,
		msgBuffer:      util.NewChannelBuffer(0),
	}

	batchNr := 0

	bc.msgBuffer.PeriodicFunc(period, func(msgs []interface{}) {

		// Do nothing if batch is empty
		if len(msgs) == 0 {
			return
		}

		// Allocate message batch
		batchMsg := &pb.ProtocolMessageBatch{
			Msgs: make([]*pb.ProtocolMessage, len(msgs), len(msgs)),
		}

		// Fill batch with messages
		for i, msg := range msgs {
			batchMsg.Msgs[i] = msg.(*pb.ProtocolMessage)
		}

		// Wrap batch in a ProtocolMessage
		multiMsg := &pb.ProtocolMessage{
			SenderId: membership.OwnID,
			Sn:       0,
			Msg:      &pb.ProtocolMessage_Multi{Multi: batchMsg},
		}

		// Log message batch size.
		// TODO: Remove ugly ad-hoc sampling
		if batchNr%msgBatchTraceSampling == 0 {
			tracing.MainTrace.Event(tracing.MSG_BATCH, int64(batchNr), int64(len(msgs)))
		}
		batchNr++

		// Send message on the underlying connection
		pc.Send(multiMsg)
	})

	return bc
}

func (bc *BatchedConnection) Send(msg *pb.ProtocolMessage) {
	bc.msgBuffer.Add(msg)
}

func (bc *BatchedConnection) SendPriority(msg *pb.ProtocolMessage) {
	bc.peerConnection.SendPriority(msg)
}

func (bc *BatchedConnection) Close() {
	bc.msgBuffer.StopFunc()
}
