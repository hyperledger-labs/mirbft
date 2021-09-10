/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: Write comments and factor out the modules in this file in their proper files.

// Package modules provides interfaces of modules that serve as building blocks of a Node.
// Implementations of those interfaces are not contained by this package
// and are expected to be provided by other packages.
package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/eventpb"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"hash"
)

// The Modules structs groups the modules a Node consists of.
type Modules struct {
	Net          Net
	Hasher       Hasher
	App          App
	WAL          WAL
	RequestStore RequestStore
	Protocol     Protocol
	Interceptor  EventInterceptor
}

type Hasher interface {
	New() hash.Hash
}

// The Net module provides a simple abstract interface for sending messages to other nodes.
type Net interface {
	Send(dest uint64, msg *messagepb.Message)
}

type WAL interface {
	Write(index uint64, entry *eventpb.Event) error
	Append(entry *eventpb.Event) error
	Truncate(index uint64) error
	Sync() error
	LoadAll(forEach func(index uint64, p *eventpb.Event)) error
}
