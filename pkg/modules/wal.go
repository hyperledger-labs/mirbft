/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
)

// The WAL (Write-Ahead Log) implements a persistent write-ahead log for the case of crashes and restarts.
// It simply persists (a serialized form of) events that are appended to it.
// The content of the WAL is used by the node to rebuild its state after, for example,
// a restart or a crash/recovery event.
// An integer retentionIndex is specified both when appending entries and when truncating the WAL.
// On truncation, the WAL removes all entries whose retentionIndex is smaller than the specified one.
type WAL interface {

	// Append appends an entry with a retentionIndex to the WAL.
	// When Append returns, its effect on the WAL can, but might not have been persisted.
	// Persistence guarantees are only provided by the WAL after a call to Sync() returns.
	Append(entry *eventpb.Event, retentionIndex t.WALRetIndex) error

	// Truncate removes all entries from the WAL that have been appended
	// with a retentionIndex smaller than the specified one.
	// The effect of Truncate() is only guaranteed to be persisted to stable storage
	// after the next call to Sync() returns.
	Truncate(retentionIndex t.WALRetIndex) error

	// Sync persists the current state of the WAL to stable storage.
	// When Sync() returns, the effect of all previous calls to Append() and Truncate()
	// has been persisted to stable storage.
	Sync() error

	// LoadAll applies the provided forEach function to all WAL entries and their corresponding retentionIndexes
	// in the order in which they have been appended.
	LoadAll(forEach func(retentionIndex t.WALRetIndex, p *eventpb.Event)) error
}
