/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
)

// App represents an application this library is used for replicating.
// It consumes Batches of Requests and executes them. This library makes sure that at each node the same sequence of
// Request Batches will be fed to all replicas of the application.
// The consumer of this library is responsible for providing each replica with an instance of the application in the
// same initial state, as well as for ensuring that processing of Requests is deterministic, in order to guarantee
// the state of the application to be consistent across all replicas.
type App interface {

	// Apply a batch of Requests to the current stat of the application.
	// Note that the Batch only contains request references and it is up to the implementation of the App
	// to retrieve the corresponding payloads.
	// This can be done, for example, but having the App access the RequestStore (outside the Node).
	Apply(batch *requestpb.Batch) error

	// Snapshot returns a snapshot of the current application state in form of a byte slice.
	// The application must be able to restore its internal state when the returned value is passed to RestoreState.
	Snapshot() ([]byte, error)

	// RestoreState restores the internal application state based on data returned by Snapshot.
	RestoreState(snapshot []byte) error

	// TODO: Introduce function(s) for interacting with the library. In particular, give the application some control
	//       over the membership (both replicas and clients). Make sure that this is state-based, i.e., one should be
	//       able to query the application for information on requested changes of the client or replica set, without
	//       modifying the application state (which should exclusively be modified by the Apply and RestoreState
	//       methods).
}
