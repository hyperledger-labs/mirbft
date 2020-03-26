/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"context"
	"fmt"
	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	tpb "github.com/IBM/mirbft/testengine/testenginepb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type PlaybackNode struct {
	Node       *mirbft.Node
	Processing *mirbft.Actions
	Actions    *mirbft.Actions
	Status     *mirbft.Status
}

type Player struct {
	LastEvent *tpb.Event
	EventLog  *EventLog
	Nodes     []*PlaybackNode
	DoneC     chan struct{}
}

func NewPlayer(el *EventLog, logger *zap.Logger) (*Player, error) {
	doneC := make(chan struct{})
	var nodes []*PlaybackNode
	for i, nodeConfig := range el.NodeConfigs {
		if uint64(i) != nodeConfig.Id {
			return nil, errors.Errorf("nodeConfig.Id did not appear in order, expected %d, got %d", i, nodeConfig.Id)
		}

		node, err := mirbft.StartNewNode(
			&mirbft.Config{
				ID:     nodeConfig.Id,
				Logger: logger.Named(fmt.Sprintf("node%d", nodeConfig.Id)),
				BatchParameters: mirbft.BatchParameters{
					CutSizeBytes: 1,
				},
				SuspectTicks:         int(nodeConfig.SuspectTicks),
				NewEpochTimeoutTicks: int(nodeConfig.NewEpochTimeoutTicks),
				HeartbeatTicks:       int(nodeConfig.HeartbeatTicks),
			},
			doneC,
			el.InitialConfig,
		)
		if err != nil {
			return nil, errors.WithMessagef(err, "could not create mir node %d", nodeConfig.Id)
		}

		status, err := node.Status(context.Background())
		if err != nil {
			return nil, errors.WithMessagef(err, "could no get initial status for mir node %d", nodeConfig.Id)
		}

		nodes = append(nodes, &PlaybackNode{
			Node:    node,
			Actions: &mirbft.Actions{},
			Status:  status,
		})
	}

	return &Player{
		EventLog: el,
		Nodes:    nodes,
		DoneC:    doneC,
	}, nil
}

func (p *Player) Step() error {
	event := p.EventLog.ConsumeAndAdvance()
	if event == nil {
		return errors.Errorf("event log has no more events")
	}
	p.LastEvent = event

	if event.Dropped {
		// We allow the log to encode events which were set to be processed, but were
		// deliberatley dropped by some mangler.  This makes it easier to review event logs
		// identifying why tests failed.
		return nil
	}

	if event.Target >= uint64(len(p.Nodes)) {
		return errors.Errorf("event log referenced a node %d which does not exist", event.Target)
	}

	node := p.Nodes[int(event.Target)]

	switch et := event.Type.(type) {
	case *tpb.Event_Apply_:
		if node.Processing == nil {
			return errors.Errorf("node %d is not currently processing but got an apply event", event.Target)
		}

		node.Processing = nil

		apply := et.Apply
		actionResults := &mirbft.ActionResults{
			Preprocessed: make([]*mirbft.PreprocessResult, len(apply.Preprocessed)),
			Processed:    make([]*mirbft.ProcessResult, len(apply.Processed)),
			Checkpoints:  make([]*mirbft.CheckpointResult, len(apply.Checkpoints)),
		}

		for i, pp := range apply.Preprocessed {
			actionResults.Preprocessed[i] = &mirbft.PreprocessResult{
				Digest: pp.Digest,
				RequestData: &pb.RequestData{
					ClientId:  pp.ClientId,
					ReqNo:     pp.ReqNo,
					Data:      pp.Data,
					Signature: pp.Signature,
				},
			}
		}

		for i, pr := range apply.Processed {
			actionResults.Processed[i] = &mirbft.ProcessResult{
				SeqNo:  pr.SeqNo,
				Epoch:  pr.Epoch,
				Digest: pr.Digest,
			}
		}

		for i, cr := range apply.Checkpoints {
			actionResults.Checkpoints[i] = &mirbft.CheckpointResult{
				SeqNo: cr.SeqNo,
				Value: cr.Value,
			}
		}

		node.Node.AddResults(*actionResults)
	case *tpb.Event_Receive_:
		receive := et.Receive
		err := node.Node.Step(context.Background(), receive.Source, receive.Msg)
		if err != nil {
			return errors.WithMessagef(err, "node %d could not step msg from %d", event.Target, receive.Source)
		}
	case *tpb.Event_Process_:
		if node.Processing != nil {
			return errors.Errorf("node %d is currently processing but got a second process event", event.Target)
		}

		for _, msg := range node.Actions.Broadcast {
			err := node.Node.Step(context.Background(), event.Target, msg)
			if err != nil {
				return errors.WithMessagef(err, "node %d could not step message to self", event.Target)
			}
		}

		for _, unicast := range node.Actions.Unicast {
			if unicast.Target != event.Target {
				continue
			}
			// It's a bit weird to unicast to ourselves, but let's handle it.
			err := node.Node.Step(context.Background(), event.Target, unicast.Msg)
			if err != nil {
				return errors.WithMessagef(err, "node %d could not step message to self", event.Target)
			}
		}

		node.Processing = node.Actions
		node.Actions = &mirbft.Actions{}
		return nil
	case *tpb.Event_Propose_:
		request := et.Propose.RequestData
		err := node.Node.Propose(context.Background(), false, request)
		if err != nil {
			return errors.WithMessagef(err, "node %d could not propose msg from client %x with reqno %d", event.Target, request.ClientId, request.ReqNo)
		}
	case *tpb.Event_Tick_:
		node.Node.Tick()
	}

	select {
	case actions := <-node.Node.Ready():
		node.Actions.Append(&actions)
	case <-node.Node.Err():
		_, err := node.Node.Status(context.Background())
		return errors.WithMessagef(err, "node %d is in an errored state -- last event: %T", event.Target, event.Type)
	}

	status, _ := node.Node.Status(context.Background())
	node.Status = status

	return nil
}
