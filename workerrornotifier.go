package mirbft

import (
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"sync"
)

// workErrNotifier is used to synchronize the exit of the assorted worker
// go routines.  The first worker to encounter an error should call Fail(err),
// then the other workers will (eventually) read ExitC() to determine that they
// should exit.  The worker thread responsible for the state machine _must_
// call SetExitStatus(status, statusErr) before returning.
type workErrNotifier struct {
	mutex         sync.Mutex
	err           error
	exitC         chan struct{}
	exitStatus    *status.StateMachine
	exitStatusErr error
	exitStatusC   chan struct{}
}

func newWorkErrNotifier() *workErrNotifier {
	return &workErrNotifier{
		exitC:       make(chan struct{}),
		exitStatusC: make(chan struct{}),
	}
}

func (wen *workErrNotifier) Err() error {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	return wen.err
}

func (wen *workErrNotifier) Fail(err error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	if wen.err != nil {
		return
	}
	wen.err = err
	close(wen.exitC)
}

func (wen *workErrNotifier) SetExitStatus(s *status.StateMachine, err error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	wen.exitStatus = s
	wen.exitStatusErr = err
	close(wen.exitStatusC)
}

func (wen *workErrNotifier) ExitStatus() (*status.StateMachine, error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	return wen.exitStatus, wen.exitStatusErr
}

func (wen *workErrNotifier) ExitC() <-chan struct{} {
	return wen.exitC
}

func (wen *workErrNotifier) ExitStatusC() <-chan struct{} {
	return wen.exitStatusC
}

