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

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
)

type DirectTrace struct {
	// Instance of the zerolog logger used to output traces.
	// Will be initialized with default values when Start() is called.
	traceLogger zerolog.Logger

	// File pointer to the open file to which the trace should be written
	outFile *os.File

	// Only trace events whose sequence number (clSn for request events, seqNr for protocol events)
	// is is a multiple of Sampling.
	Sampling int
}

// Starts the tracing of this DirectTrace.
// No call to Event(), Protocol(), Request() or Ethereum() must be made before Start() returned.
// If Start() has been called, then Stop() must be called as well
// to perform the necessary cleanup before the process finishes.
func (t *DirectTrace) Start(outFileName string, nodeID int32) {

	// If Sampling parameter was not set assume 1 (i.e. trace every event).
	if t.Sampling == 0 {
		t.Sampling = 1
	}

	// Open output file.
	var err error
	t.outFile, err = os.Create(outFileName)
	if err != nil {
		logger.Fatal().Err(err).Str("fileName", outFileName).Msg("Could not create trace output file.")
	}

	// Create a logger instance writing to the opened output file.
	t.traceLogger = zerolog.New(t.outFile).With().Timestamp().Int32("nodeId", nodeID).Logger()
}

// Create a new trace event.
func (t *DirectTrace) Event(e EventType, sampledVal int64, val0 int64) {

	// Discard event if it is not in the sampling set.
	if sampledVal%int64(t.Sampling) != 0 {
		return
	}

	t.traceLogger.Log().
		Int64("sampledVal", sampledVal).
		Int64("val0", val0).
		Msg(e.String())
}

// Create a new protocol trace event.
func (t *DirectTrace) Protocol(e EventType, seqNr int32) {

	// Discard event if it is not in the sampling set.
	if int64(seqNr)%int64(t.Sampling) != 0 {
		return
	}

	t.traceLogger.Log().Str("type", "protocol").Int32("seqNr", seqNr).Msg(e.String())
}

// Create a new request trace event.
func (t *DirectTrace) Request(e EventType, clID int32, clSN int32) {

	// Discard event if it is not in the sampling set.
	if int64(clSN)%int64(t.Sampling) != 0 {
		return
	}

	t.traceLogger.Log().Str("type", "request").Int32("clId", clID).Int32("clSn", clSN).Msg(e.String())
}

// Create a new request trace event.
func (t *DirectTrace) Ethereum(e EventType, configNr int64, gasCost int64) {

	t.traceLogger.Log().Str("type", "ethereum").Int64("configNr", configNr).Int64("gasCost", gasCost).Msg(e.String())
}

// Stops the tracing and performs the necessary cleanup (closes the output file).
// Stop() must be called in order to obtain meaningful output.
func (t *DirectTrace) Stop() {
	if t.outFile != nil {
		t.outFile.Close()
	}
}

// Sets up a signal handler that calls Stop() when the specified OS signal occurs.
// This is useful for tracing if the program does not terminate gracefully, but is stopped using an OS signal.
// StopOnSignal() can be used in this case to still stop the tracer (flushing buffer contents).
// If the exit flag is set to true, the signal handler will exit the process after stopping the tracer.
func (t *DirectTrace) StopOnSignal(sig os.Signal, exit bool) {

	// Allocate chanel to receive notification on.
	sigChan := make(chan os.Signal)

	// Set up signal handler
	signal.Notify(sigChan, sig)

	// Start background goroutine waiting for the signal
	go func() {

		// Wait for signal
		<-sigChan

		// Stop profiler
		t.Stop()

		// Exit if requested
		if exit {
			os.Exit(1)
		}
	}()
}
