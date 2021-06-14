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

package tracing

import "github.com/op/go-logging"

// TODO: Use https://github.com/c9s/goprocinfo for CPU load tracing.
const EventBufferSize = 1048576 //(2^20) Capacity of the tracing event buffer, in number of events.
const TraceSampling = 100         // Only trace one out of TraceSampling events.

// A global instance of a trace object to be used from anywhere in the code,
// without having to keep around a reference and dereference it.
var (
	MainTrace Trace
)

var log = logging.MustGetLogger("bftserver")
var format = logging.MustStringFormatter(
	`%{time:15:04:05.000000} %{shortfunc} %{message}`,
)

// Initializes the main trace.
// Cannot be part of the init() function, as the configuration file is not yet loaded when init() is executed.
func init() {
	log.Infof("Initializing main trace")
	MainTrace = &BufferedTrace{
		Sampling:       TraceSampling,
		BufferCapacity: EventBufferSize,
		//ProtocolEventCapacity: config.Config.EventBufferSize,
		//RequestEventCapacity:  config.Config.EventBufferSize,
		EthereumEventCapacity: 1024,
	}
}
