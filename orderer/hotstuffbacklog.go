package orderer

// TODO limit backlog size

import (
	logger "github.com/rs/zerolog/log"
	pb "github.ibm.com/mir-modular/protobufs"
)

// Backlogs messages by height
type hotStuffBacklog struct {
	hi          *hotStuffInstance
	backlogMsgs map[int32][]*pb.ProtocolMessage
}

func newHotStuffBacklog(hi *hotStuffInstance) *hotStuffBacklog {
	b := &hotStuffBacklog{
		hi:          hi,
		backlogMsgs: make(map[int32][]*pb.ProtocolMessage),
	}
	return b
}

func (b *hotStuffBacklog) add(height int32, msg *pb.ProtocolMessage) {
	logger.Debug().
		Int32("height", height).
		Int32("sn", msg.Sn).
		Int32("senderId", msg.SenderId).
		Msg("Adding message to HotStuff backlog")
	if _, ok := b.backlogMsgs[height]; !ok {
		b.backlogMsgs[msg.Sn] = make([]*pb.ProtocolMessage, 0, 0)
	}
	b.backlogMsgs[height] = append(b.backlogMsgs[height], msg)
}

// Returns the messages of the height  to their queues
func (b *hotStuffBacklog) process(height int32) {
	for _, msg := range b.backlogMsgs[height] {
		b.hi.serializer.channel <- msg
	}
}
