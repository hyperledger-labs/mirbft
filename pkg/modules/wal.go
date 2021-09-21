/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package modules

import "github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"

// TODO: Write comments.
// TODO: Figure out Write vs. Append
type WAL interface {
	Write(index uint64, entry *eventpb.Event) error
	Append(entry *eventpb.Event) error
	Truncate(index uint64) error
	Sync() error
	LoadAll(forEach func(index uint64, p *eventpb.Event)) error
}
