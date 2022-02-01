package checkpoint

import (
	pb "github.ibm.com/mir-modular/protobufs"
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
