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
}

type BatchParameters struct {
	CutSizeBytes int
}
