/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Refactored: 1
*/

package mirbft

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/statuspb"
	"sync"
)

// workErrNotifier is used to synchronize the exit of the assorted worker
// go routines. The first worker to encounter an error should call Fail(err),
// then the other workers will (eventually) read ExitC() to determine that they
// should exit. The worker thread responsible for the protocol state machine _must_
// call SetExitStatus(status, statusErr) before returning.
type workErrNotifier struct {

	// Synchronizes all access to the object.
	mutex sync.Mutex

	// Set to the argument of the first invocation of the Fail() method.
	err error

	// Closed when Fail() is invoked. All workers treat closing of this channel as a stopping condition.
	exitC chan struct{}

	// The final status of the protocol state machine on exit.
	exitStatus *statuspb.NodeStatus

	// Error that might have occurred when obtaining the protocol state machine's exit status.
	exitStatusErr error

	// Closed when exitStatus and exitStatusErr have been set.
	exitStatusC chan struct{}
}

// Creates a new initialized workErrNotifier object.
func newWorkErrNotifier() *workErrNotifier {
	return &workErrNotifier{
		exitC:       make(chan struct{}),
		exitStatusC: make(chan struct{}),
	}
}

// Err returns the error set by the Fail() method.
// If no error has been set yet, returns nil.
func (wen *workErrNotifier) Err() error {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	return wen.err
}

// Fail is called by a worker thread that encounters an error.
// The first invocation of Fail() saves the error that will be returned by all subsequent invocations of Err()
// and closes the exitC to notify all other workers about the error and make them terminate.
func (wen *workErrNotifier) Fail(err error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	if wen.err != nil {
		return
	}
	wen.err = err
	close(wen.exitC)
}

// SetExitStatus saves the final status of the protocol state machine in this workErrorNotifier,
// along with a potential error that might have occurred while obtaining the status.
// SetExitStatus also closes the exitStatusC to notify other threads that the exit status has been set.
func (wen *workErrNotifier) SetExitStatus(s *statuspb.NodeStatus, err error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	wen.exitStatus = s
	wen.exitStatusErr = err
	close(wen.exitStatusC)
}

// ExitStatus returns the status and the error set by the first invocation of SetExitStatus.
// If the exit status has not been set yet, ExitStatus returns (nil, nil)
func (wen *workErrNotifier) ExitStatus() (*statuspb.NodeStatus, error) {
	wen.mutex.Lock()
	defer wen.mutex.Unlock()
	return wen.exitStatus, wen.exitStatusErr
}

// ExitC returns a channel the closing of which indicates
// that the an error has occurred and the Fail() method has been invoked.
func (wen *workErrNotifier) ExitC() <-chan struct{} {
	return wen.exitC
}

// ExitStatusC returns a channel the closing of which indicates
// that the exit status and the corresponding error have been set by SetExitStatus().
func (wen *workErrNotifier) ExitStatusC() <-chan struct{} {
	return wen.exitStatusC
}
