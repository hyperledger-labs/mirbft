/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal

import (
	"go.uber.org/zap"
)

const (
	SeqNoLog    = "SeqNo"
	EpochLog    = "Epoch"
	NodeIDLog   = "NodeID"
	BucketIDLog = "BucketID"
	MsgTypeLog  = "MsgType"
)

func LogBasics(msgType string, source NodeID, seqNo SeqNo, bucket BucketID, epoch uint64) []zap.Field {
	return []zap.Field{
		zap.String(MsgTypeLog, msgType),
		zap.Uint64(SeqNoLog, uint64(seqNo)),
		zap.Uint64(NodeIDLog, uint64(source)),
		zap.Uint64(BucketIDLog, uint64(bucket)),
		zap.Uint64(EpochLog, epoch),
	}
}

// Oddities are events which are not necessarily damaging
// or detrimental to the state machine, but which may represent
// byzantine behavior, misconfiguration, or bugs.
type Oddities struct {
	Nodes map[NodeID]*Oddity
}

type Oddity struct {
	AboveWatermarks uint64
	BelowWatermarks uint64
	WrongEpoch      uint64
	BadBucket       uint64
}

func (o *Oddities) GetNode(nodeID NodeID) *Oddity {
	oddity, ok := o.Nodes[nodeID]
	if !ok {
		oddity = &Oddity{}
		o.Nodes[nodeID] = oddity
	}
	return oddity
}

func (o *Oddities) AboveWatermarks(epochConfig *EpochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.MyConfig.Logger.Warn("received message above watermarks", LogBasics(msgType, source, seqNo, bucket, epochConfig.Number)...)
	o.GetNode(source).AboveWatermarks++
}

func (o *Oddities) AlreadyProcessed(epochConfig *EpochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.MyConfig.Logger.Warn("already processed message", LogBasics(msgType, source, seqNo, bucket, epochConfig.Number)...)
	o.GetNode(source).AboveWatermarks++
}

func (o *Oddities) BelowWatermarks(epochConfig *EpochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.MyConfig.Logger.Warn("received message below watermarks", LogBasics(msgType, source, seqNo, bucket, epochConfig.Number)...)
	o.GetNode(source).BelowWatermarks++
}

func (o *Oddities) BadBucket(epochConfig *EpochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.MyConfig.Logger.Warn("received message for bad bucket", LogBasics(msgType, source, seqNo, bucket, epochConfig.Number)...)
	o.GetNode(source).BadBucket++
}

func (o *Oddities) InvalidMessage(epochConfig *EpochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.MyConfig.Logger.Error("invalid message", LogBasics(msgType, source, seqNo, bucket, epochConfig.Number)...)
	o.GetNode(source).AboveWatermarks++
}
