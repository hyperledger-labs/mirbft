package deploytest

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
)

type DummySM struct {
	logger statemachine.Logger
}

func NewDummySM(logger statemachine.Logger) *DummySM {
	return &DummySM{logger: logger}
}

func (dsm *DummySM) ApplyEvent(stateEvent *state.Event) *statemachine.ActionList {
	dsm.logger.Log(statemachine.LevelDebug, "Ignoring event",
		"type", fmt.Sprintf("%T", stateEvent.Type))
	return &statemachine.ActionList{}
}

func (dsm *DummySM) Status() (s *status.StateMachine, err error) {
	return &status.StateMachine{}, nil
}
