package orderer

import (
	pb "github.ibm.com/mir-modular/protobufs"
)

// TODO limit backlog size?

// The backlog of a pbft instance
// Holds a map entry for each view
type pbftBacklog struct {
	pi          *pbftInstance
	backlogMsgs map[int32][]*pb.ProtocolMessage
}

func newPbftBacklog(pi *pbftInstance) *pbftBacklog {
	b := &pbftBacklog{
		pi:          pi,
		backlogMsgs: make(map[int32][]*pb.ProtocolMessage),
	}
	return b
}

func (b *pbftBacklog) addMessage(msg *pb.ProtocolMessage, view int32) {
	if _, ok := b.backlogMsgs[view]; !ok {
		b.backlogMsgs[view] = make([]*pb.ProtocolMessage, 0, 0)
	}
	b.backlogMsgs[view] = append(b.backlogMsgs[view], msg)
}

// Returns the messages of the new view to their queues
func (b *pbftBacklog) process(view int32) {
	for _, msg := range b.backlogMsgs[view] {
		b.pi.serializer.channel <- msg
	}
}

// Delete all entries from previous views
func (b *pbftBacklog) prune(view int32) {
	for backlogView, _ := range b.backlogMsgs {
		if backlogView < view {
			delete(b.backlogMsgs, backlogView)
		}
	}
}
