/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import "github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"

// The Net module provides a simple abstract interface for sending messages to other nodes.
// TODO: Write comments.
type Net interface {
	Send(dest uint64, msg *messagepb.Message)
}
