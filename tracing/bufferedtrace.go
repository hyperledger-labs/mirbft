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

package tracing

import (
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
)

type BufferedTrace struct {
	events []GenericEvent
	//protocolEvents []ProtocolEvent
	//requestEvents  []RequestEvent
	//ethereumEvents []EthereumEvent
	outFileName    string
	nodeID         int32
	offset         int32
	protocolOffset int32
	requestOffset  int32
	ethereumOffset int32

	// Only trace events whose SampledVal is is a multiple of Sampling.
	// (E.g., client sequence number for request events, sequence number for protocol events is meant to be stored there)
	Sampling int

	BufferCapacity        int
	ProtocolEventCapacity int
	RequestEventCapacity  int
	EthereumEventCapacity int
}

func (bt *BufferedTrace) Start(outFileName string, nodeID int32) {

	// If Sampling parameter was not set assume 1 (i.e. trace every event).
	if bt.Sampling == 0 {
		bt.Sampling = 1
	}

	bt.outFileName = outFileName
	bt.nodeID = nodeID

	// Allocate the whole buffer for all events
	// The size is directly initialized to the capacity.
	// We track the size separately using offset variables.
	bt.events = make([]GenericEvent, bt.BufferCapacity, bt.BufferCapacity)
	//bt.protocolEvents = make([]ProtocolEvent, bt.ProtocolEventCapacity, bt.ProtocolEventCapacity)
	//bt.requestEvents = make([]RequestEvent, bt.RequestEventCapacity, bt.RequestEventCapacity)
	//bt.ethereumEvents = make([]EthereumEvent, bt.EthereumEventCapacity, bt.EthereumEventCapacity)

	bt.offset = 0
	bt.protocolOffset = 0
	bt.requestOffset = 0
	bt.ethereumOffset = 0
}

// Create a new protocol trace event.
func (bt *BufferedTrace) Event(e EventType, sampledVal int64, val0 int64) {

	// TODO: Clean up this ad-hock removal of REQ_SEND and RESP_RECEIVE events.
	if e == REQ_RECEIVE || e == RESP_RECEIVE || e == RESP_SEND {
		return
	}

	// Discard event if it is not in the sampling set.
	if sampledVal%int64(bt.Sampling) != 0 {
		return
	}

	// Assign trace index to the event. Atomic increment is necessary, as many threads use the trace concurrently
	// The offset variable points to the next free slot, but needs to be incremented before data is written. (thus -1)
	index := atomic.AddInt32(&bt.offset, 1) - 1

	// Check bounds.
	// TODO: Try removing the bound check (while making sure the buffer is big enough)
	//       and see impact on performance.
	if int(index) >= bt.BufferCapacity {
		logger.Error().
			Int32("index", index).
			Int("capacity", config.Config.EventBufferSize).
			Msg("Trace event index exceeds capacity.")
	}

	// Add event to trace.
	// Tried to test whether assigning each value separately to the array (at the specified index) is faster than
	// assigning a whole struct. It is not, unless the definition (not necessarily the assigned instance) of the struct
	// contains string or pointer types. Anyway, the biggest time consumer is querying the time.
	bt.events[index] = GenericEvent{
		EventType:  e,
		Timestamp:  time.Now().UnixNano() / 1000,
		NodeId:     bt.nodeID,
		SampledVal: sampledVal,
		Val0:       val0,
	}
}

//// Create a new protocol trace event.
//func (bt *BufferedTrace) Protocol(e EventType, seqNr int32) {
//
//	// Discard event if it is not in the sampling set.
//	if int64(seqNr)%int64(bt.Sampling) != 0 {
//		return
//	}
//
//	// Assign trace index to the event. Atomic increment is necessary, as many threads use the trace concurrently
//	// The offset variable points to the next free slot, but needs to be incremented before data is written. (thus -1)
//	index := atomic.AddInt32(&bt.protocolOffset, 1) - 1
//
//	// Check bounds.
//	// TODO: Try removing the bound check (while making sure the buffer is big enough)
//	//       and see impact on performance.
//	if int(index) >= bt.ProtocolEventCapacity {
//		logger.Error().
//			Int32("index", index).
//			Int("capacity", config.Config.EventBufferSize).
//			Msg("Trace event index exceeds capacity.")
//	}
//
//	// Add event to trace.
//	bt.protocolEvents[index] = ProtocolEvent{
//		EventType: e,
//		Timestamp: time.Now().UnixNano() / 1000,
//		PeerId:    bt.peerID,
//		SeqNr:     seqNr,
//	}
//}

//// Create a new request trace event.
//func (bt *BufferedTrace) Request(e EventType, clID int32, clSN int32) {
//
//	// TODO: Clean up this ad-hock removal of REQ_SEND and RESP_RECEIVE events.
//	if e == REQ_RECEIVE || e == RESP_RECEIVE || e == RESP_SEND {
//		return
//	}
//
//	// Discard event if it is not in the sampling set.
//	if int64(clSN)%int64(bt.Sampling) != 0 {
//		return
//	}
//
//	// Assign trace index to the event. Atomic increment is necessary, as many threads use the trace concurrently
//	// The offset variable points to the next free slot, but needs to be incremented before data is written. (thus -1)
//	index := atomic.AddInt32(&bt.requestOffset, 1) - 1
//
//	// Check bounds.
//	// TODO: Try removing the bound check (while making sure the buffer is big enough)
//	//       and see impact on performance.
//	if int(index) >= bt.RequestEventCapacity {
//		logger.Error().
//			Int32("index", index).
//			Int("capacity", config.Config.EventBufferSize).
//			Msg("Trace event index exceeds capacity.")
//	}
//
//	// Add event to trace.
//	// Tried to test whether assigning each value separately to the array (at the specified index) is faster than
//	// assigning a whole struct. It is not, unless the definition (not necessarily the assigned instance) of the struct
//	// contains string or pointer types. Anyway, the biggest time consumer is querying the time.
//	bt.requestEvents[index] = RequestEvent{
//		EventType: e,
//		Timestamp: time.Now().UnixNano() / 1000,
//		ClId:      clID,
//		ClSn:      clSN,
//		PeerId:    bt.peerID,
//	}
//}

//func (bt *BufferedTrace) Ethereum(e EventType, configNr int64, gasCost int64) { // Discard event if it is not in the sampling set.
//
//	// Assign trace index to the event. Atomic increment is necessary, as many threads use the trace concurrently
//	// The offset variable points to the next free slot, but needs to be incremented before data is written. (thus -1)
//	index := atomic.AddInt32(&bt.ethereumOffset, 1) - 1
//
//	// Check bounds.
//	// TODO: Try removing the bound check (while making sure the buffer is big enough)
//	//       and see impact on performance.
//	if int(index) >= bt.EthereumEventCapacity {
//		logger.Error().
//			Int32("index", index).
//			Int("capacity", config.Config.EventBufferSize).
//			Msg("Trace event index exceeds capacity.")
//	}
//
//	// Add event to trace.
//	bt.ethereumEvents[index] = EthereumEvent{
//		EventType: e,
//		Timestamp: time.Now().UnixNano() / 1000,
//		PeerId:    bt.peerID,
//		ConfigNr:  configNr,
//		GasCost:   gasCost,
//	}
//}

// Writes the in-memory traces to a file specified at the call to Start().
// Stop() must be called in order to obtain meaningful output.
func (bt *BufferedTrace) Stop() {

	outFile, err := os.Create(bt.outFileName)
	if err != nil {
		logger.Fatal().Err(err).Str("fileName", bt.outFileName).Msg("Could not create trace output file.")
	}
	defer outFile.Close()

	traceLogger := zerolog.New(outFile)

	nEvents := atomic.LoadInt32(&bt.offset)
	//nProtocolEvents := atomic.LoadInt32(&bt.protocolOffset)
	//nRequestEvents := atomic.LoadInt32(&bt.requestOffset)
	//nEthereumEvents := atomic.LoadInt32(&bt.ethereumOffset)

	logger.Info().
		Int32("events", nEvents).
		//Int32("protocolEvents", nProtocolEvents).
		//Int32("requestEvents", nRequestEvents).
		//Int32("ethereumEvents", nEthereumEvents).
		Str("fileName", bt.outFileName).
		Msg("Stopping Tracer.")

	for i := 0; i < int(nEvents); i++ {
		event := bt.events[i]
		traceLogger.Log().
			Int64("time", event.Timestamp).
			Int32("nodeId", bt.nodeID).
			Int64("sampledVal", event.SampledVal).
			Int64("val0", event.Val0).
			Msg(event.EventType.String())
	}

	//for i := 0; i < int(nProtocolEvents); i++ {
	//	event := bt.protocolEvents[i]
	//	traceLogger.Log().
	//		Int64("time", event.Timestamp).
	//		Int32("peerId", bt.peerID).
	//		Str("type", "protocol").
	//		Int32("seqNr", event.SeqNr).
	//		Msg(event.EventType.String())
	//}
	//
	//for i := 0; i < int(nRequestEvents); i++ {
	//	event := bt.requestEvents[i]
	//	traceLogger.Log().
	//		Int64("time", event.Timestamp).
	//		Int32("peerId", bt.peerID).
	//		Str("type", "request").
	//		Int32("clId", event.ClId).
	//		Int32("clSn", event.ClSn).
	//		Msg(event.EventType.String())
	//}
	//
	//for i := 0; i < int(nEthereumEvents); i++ {
	//	event := bt.ethereumEvents[i]
	//	traceLogger.Log().
	//		Int64("time", event.Timestamp).
	//		Int32("peerId", bt.peerID).
	//		Str("type", "ethereum").
	//		Int64("configNr", event.ConfigNr).
	//		Int64("gasCost", event.GasCost).
	//		Msg(event.EventType.String())
	//}

	logger.Info().Msg("Trace data written.")
}

func (bt *BufferedTrace) StopOnSignal(sig os.Signal, exit bool) {

	logger.Info().Bool("exit", exit).Msg("Tracing will stop on signal.")

	// Allocate chanel to receive notification on.
	sigChan := make(chan os.Signal)

	// Set up signal handler
	signal.Notify(sigChan, sig)

	// Start background goroutine waiting for the signal
	go func() {

		// Wait for signal
		<-sigChan

		// Stop profiler
		bt.Stop()

		// Exit if requested
		if exit {
			os.Exit(1)
		}
	}()
}
