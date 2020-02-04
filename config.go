/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type Config struct {
	// ID is the NodeID for this instance.
	ID uint64

	// Logger provides the logging functions.
	Logger Logger

	// BatchParameters determines under what conditions the queued
	// pieces of data should be converted into a batch and consented on
	BatchParameters BatchParameters

	// HeartbeatTicks is the number of ticks before a heartbeat is emitted
	// by a leader.
	HeartbeatTicks int

	// SuspectTicks is the number of ticks a bucket may not progress before
	// the node suspects the epoch has gone bad.
	SuspectTicks int

	// NewEpochTimeoutTicks is the number of ticks a replica will wait until
	// it suspects the epoch leader has failed.  This value must be greater
	// than 1, as rebroadcast ticks are computed as half this value.
	NewEpochTimeoutTicks int
}

type BatchParameters struct {
	CutSizeBytes int
}
