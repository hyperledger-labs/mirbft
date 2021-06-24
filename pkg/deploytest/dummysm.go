/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package deploytest

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/logger"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
)

// DummySM is a stub for a protocol state machine implementation.
type DummySM struct {
	logger logger.Logger
}

// NewDummySM creates and returns a pointer to a new instance of DummySM.
// Log output generated by this instance will be directed to logger.
func NewDummySM(logger logger.Logger) *DummySM {
	return &DummySM{logger: logger}
}

// ApplyEvent simply creates a debug-level log entry for each incoming event and ignores it.
func (dsm *DummySM) ApplyEvent(stateEvent *state.Event) *statemachine.ActionList {
	dsm.logger.Log(logger.LevelDebug, "Ignoring event",
		"type", fmt.Sprintf("%T", stateEvent.Type))
	return &statemachine.ActionList{}
}

// Status returns an empty state machine state.
func (dsm *DummySM) Status() (s *status.StateMachine, err error) {
	return &status.StateMachine{}, nil
}
