/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

// BucketStatus represents the current
type BucketStatus struct {
	ID        uint64
	Leader    bool
	Sequences []SequenceState
}
