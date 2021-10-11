/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import "github.com/hyperledger-labs/mirbft/pkg/logger"

const (
	defaultMsgBufferSize = 2 * 1024 * 1024 // 2 MB
)

// The NodeConfig struct represents configuration parameters of the node
// that are independent of the protocol the Node is executing.
// These parameters include various buffer sizes, the choice of the logger, etc.
// NodeConfig only contains protocol-independent parameters. Protocol-specific parameters
// should be specified when instantiating the protocol implementation as one of the Node's modules.
type NodeConfig struct {
	// Logger provides the logging functions.
	Logger logger.Logger

	// BufferSize is the total size of messages which can be held by the protocol state
	// machine, pending application, for each node. This is necessary because
	// there may be dependencies between messages (for instance, until a checkpoint
	// result is computed, watermarks cannot advance). This should be set
	// to a minimum of a few MB.
	BufferSize uint32

	//// BatchSize determines how large a batch may grow (in number of request)
	//// before it is cut. (Note, batches may be cut earlier, so this is a max size).
	//BatchSize uint32
	//
	//// HeartbeatTicks is the number of ticks before a heartbeat is emitted
	//// by a leader.
	//HeartbeatTicks uint32
	//
	//// SuspectTicks is the number of ticks a bucket may not progress before
	//// the node suspects the epoch has gone bad.
	//SuspectTicks uint32
	//
	//// NewEpochTimeoutTicks is the number of ticks a replica will wait until
	//// it suspects the epoch leader has failed.  This value must be greater
	//// than 1, as rebroadcast ticks are computed as half this value.
	//NewEpochTimeoutTicks uint32
}

// DefaultNodeConfig returns the default node configuration.
// It can be used as a base for creating more specific configurations when instantiating a Node.
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		Logger:     logger.ConsoleInfoLogger,
		BufferSize: defaultMsgBufferSize,
	}
}
