package tracing

import "github.ibm.com/mir-modular/config"

// TODO: Use https://github.com/c9s/goprocinfo for CPU load tracing.

// A global instance of a trace object to be used from anywhere in the code,
// without having to keep around a reference and dereference it.
var (
	MainTrace Trace
)

// Initializes the main trace.
// Cannot be part of the init() function, as the configuration file is not yet loaded when init() is executed.
func Init() {
	MainTrace = &BufferedTrace{
		Sampling:       config.Config.TraceSampling,
		BufferCapacity: config.Config.EventBufferSize,
		//ProtocolEventCapacity: config.Config.EventBufferSize,
		//RequestEventCapacity:  config.Config.EventBufferSize,
		EthereumEventCapacity: 1024,
	}
}
