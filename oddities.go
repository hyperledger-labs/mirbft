/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

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

func logBasics(msgType string, source NodeID, seqNo SeqNo, bucket BucketID, epoch uint64) []zap.Field {
	return []zap.Field{
		zap.String(MsgTypeLog, msgType),
		zap.Uint64(SeqNoLog, uint64(seqNo)),
		zap.Uint64(NodeIDLog, uint64(source)),
		zap.Uint64(BucketIDLog, uint64(bucket)),
		zap.Uint64(EpochLog, epoch),
	}
}

// oddities are events which are not necessarily damaging
// or detrimental to the state machine, but which may represent
// byzantine behavior, misconfiguration, or bugs.
type oddities struct {
	nodes map[NodeID]*oddity
}

type oddity struct {
	aboveWatermarks uint64
	belowWatermarks uint64
	wrongEpoch      uint64
	badBucket       uint64
}

func (o *oddities) getNode(nodeID NodeID) *oddity {
	od, ok := o.nodes[nodeID]
	if !ok {
		od = &oddity{}
		o.nodes[nodeID] = od
	}
	return od
}

func (o *oddities) aboveWatermarks(epochConfig *epochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.myConfig.Logger.Warn("received message above watermarks", logBasics(msgType, source, seqNo, bucket, epochConfig.number)...)
	o.getNode(source).aboveWatermarks++
}

func (o *oddities) AlreadyProcessed(epochConfig *epochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.myConfig.Logger.Warn("already processed message", logBasics(msgType, source, seqNo, bucket, epochConfig.number)...)
	o.getNode(source).aboveWatermarks++
}

func (o *oddities) belowWatermarks(epochConfig *epochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.myConfig.Logger.Warn("received message below watermarks", logBasics(msgType, source, seqNo, bucket, epochConfig.number)...)
	o.getNode(source).belowWatermarks++
}

func (o *oddities) badBucket(epochConfig *epochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.myConfig.Logger.Warn("received message for bad bucket", logBasics(msgType, source, seqNo, bucket, epochConfig.number)...)
	o.getNode(source).badBucket++
}

func (o *oddities) InvalidMessage(epochConfig *epochConfig, msgType string, source NodeID, seqNo SeqNo, bucket BucketID) {
	epochConfig.myConfig.Logger.Error("invalid message", logBasics(msgType, source, seqNo, bucket, epochConfig.number)...)
	o.getNode(source).aboveWatermarks++
}
