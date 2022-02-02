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
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

const (
	// Buffer for the message serializer.
	// Up to this many unprocessed checkpoint messages can be stored before the message receiver thread blocks.
	// This can happen if the peer is a straggler and gets stuck waiting for the local log to reach a checkpoint,
	// while already having received enough checkpoint messages for a stable checkpoint.
	// In the current implementation no further checkpoint messages can be processed until the local log advances.
	messageSerializerBuffer = 4096
)

// Wraps a received checkpoint message.
// Only used in conjunction with the messageSerializer channel.
type receivedMessage struct {

	// The received checkpoint message.
	msg *pb.CheckpointMsg

	// ID of the sender of the received checkpoint message.
	senderID int32
}
