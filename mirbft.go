/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"github.com/pkg/errors"
)

// Config contains the operational parameters for a MirBFT instance.
type Config struct {
	// ID is the NodeID for this instance.
	ID uint64

	// Logger provides the logging functions.
	Logger Logger
}

type AtomicBroadcast struct {
	Config *Config
}

func (ab *AtomicBroadcast) Propose() error {
	return errors.Errorf("unimplemented")
}
