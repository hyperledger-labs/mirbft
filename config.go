/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import "github.com/hyperledger-labs/mirbft/pkg/logging"

// The NodeConfig struct represents configuration parameters of the node
// that are independent of the protocol the Node is executing.
// NodeConfig only contains protocol-independent parameters. Protocol-specific parameters
// should be specified when instantiating the protocol implementation as one of the Node's modules.
type NodeConfig struct {
	// Logger provides the logging functions.
	Logger logging.Logger
}

// DefaultNodeConfig returns the default node configuration.
// It can be used as a base for creating more specific configurations when instantiating a Node.
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		Logger: logging.ConsoleInfoLogger,
	}
}
