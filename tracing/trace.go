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
)

type Trace interface {
	// Starts the tracing of this Trace.
	// No call to Event(), Protocol() or Request() must be made before Start() returned.
	// If Start() has been called, then Stop() must be called as well
	// to perform the necessary cleanup before the process finishes.
	Start(outFileName string, nodeID int32)

	// Create a new trace event
	// sampledVal and val0 are integers whose interpretation depends on the event type.
	// However, sampledVal is also used for sampling
	Event(e EventType, sampledVal int64, val0 int64)

	//// Create a new protocol trace event.
	//Protocol(e EventType, seqNr int32)
	//
	// Create a new request trace event.
	//Request(e EventType, clID int32, clSN int32)
	//
	//// Create new Ethereum trace event
	//Ethereum(e EventType, configNr int64, gasCost int64)

	// Stops the tracing and performs the necessary cleanup (e.g. closes the output file).
	// Stop() must be called in order to obtain meaningful output.
	Stop()

	// Sets up a signal handler that calls Stop() when the specified OS signal occurs.
	// This is useful for tracing if the program does not terminate gracefully, but is stopped using an OS signal.
	// StopOnSignal() can be used in this case to still stop the tracer (flushing buffer contents).
	// If the exit flag is set to true, the signal handler will exit the process after stopping the tracer.
	StopOnSignal(sig os.Signal, exit bool)
}
