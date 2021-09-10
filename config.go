/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import "github.com/hyperledger-labs/mirbft/pkg/logger"

// The NodeConfig struct represents configuration parameters of the node
// that are independent of the protocol the Node is executing.
// These parameters include various buffer sizes, the choice of the logger, etc.
// NodeConfig only contains protocol-independent parameters. Protocol-specific parameters
// should be specified when instantiating the protocol implementation as one of the Node's modules.
type NodeConfig struct {
	// Logger provides the logging functions.
	Logger logger.Logger

	// BufferSize is the total size of messages which can be held by the state
	// machine, pending application, for each node. This is necessary because
	// there may be dependencies between messages (for instance, until a checkpoint
	// result is computed, watermarks cannot advance). This should be set
	// to a minimum of a few MB.
	BufferSize uint32
}
