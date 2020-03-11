/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"context"
	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
)

type TestNode struct {
	StateMachine   *mirbft.Node
	NodeConfig     *tpb.NodeConfig
	PendingActions *mirbft.Actions
	IsProcessing   bool
	EventLog       *EventLog
}

func (tn *TestNode) Recv(source uint64, msg *pb.Msg) {
	tn.StateMachine.Step(context.Background(), source, msg)
	// tn.PendingActions.Append(&(<-tn.StateMachine.Ready()))
}

func (tn *TestNode) Send(dest uint64, msg *pb.Msg) {
	// tn.EventLog.InsertRecv(dest, tn.StateMachine.Config.ID, msg, tn.NodeConfig.LinkLatency)
}

func (tn *TestNode) Tick() {
	tn.EventLog.InsertTick(tn.StateMachine.Config.ID, 0)
}

func (tn *TestNode) Process(actions *mirbft.Actions) {
	if tn.IsProcessing {
		panic("attempted to process multiple action batches concurrently")
		tn.IsProcessing = true
	}
	// tn.EventLog.InsertProcess(tn.StateMachine.Config.ID, actions, 0)
}

func (tn *TestNode) Apply(results *mirbft.ActionResults) {

}
