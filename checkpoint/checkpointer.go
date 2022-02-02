// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"sync"

	"github.com/hyperledger-labs/mirbft/manager"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

// The Checkpointer is responsible for running the checkpointing protocol.
// Whenever instructed by the Manager, the Checkpointer creates a new instance of the checkpointing protocol.
type Checkpointer interface {

	// Initializes the Checkpointer and attaches a Manager to it.
	// The Manager informs the Checkpointer about the sequence numbers at which a checkpoint should occur.
	// AttachManager must be called before the Manager is started, so the Checkpointer does not miss any checkpoints.
	// Init() must be called before the Manager is started, so the Orderer does not miss any checkpoints.
	// Init() must be called before Strat() is called.
	// After Init() returns, the Checkpointer mus be ready to process incoming messages, but must not send any itself
	// (sending messages only can start after the call to Start()).
	Init(mngr manager.Manager)

	// Message handler function. To be assigned to the messenger.CheckpointMsgHandler variable, which the messenger
	// calls on reception of a message belonging to the checkpointing subprotocol.
	HandleMessage(checkpoint *pb.CheckpointMsg, senderID int32)

	// Satrts the Checkpointer. Before starting the Checkpointer, it must be attached to the Manager and its message
	// handler must be registered with the messanger.
	// Meant to be run as a separate goroutine.
	// Decrements the passed wait group on termination.
	Start(group *sync.WaitGroup)

	// Returns all checkpoints that are not stable yet.
	GetPendingCheckpoints() []*pb.CheckpointMsg
}
