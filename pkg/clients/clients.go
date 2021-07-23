package clients

import (
	"encoding/binary"
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
)

type ClientTracker struct {
	Hasher modules.Hasher
}

func (ct *ClientTracker) ApplyEvent(event *state.Event) *statemachine.EventList {
	switch e := event.Type.(type) {
	case *state.Event_Request:
		req := e.Request
		fmt.Println("Received request.")
		digest := ct.computeReqHash(req.ClientId, req.ReqNo, req.Data)
		_ = digest
		return &statemachine.EventList{}
	default:
		panic(fmt.Sprintf("unknown event: %T", event.Type))
	}
}

func (ct *ClientTracker) Status() (s *status.StateMachine, err error) {
	return nil, nil
}

func (ct *ClientTracker) computeReqHash(clientID uint64, reqNo uint64, data []byte) []byte {
	// Initialize auxiliary data structures
	h := ct.Hasher.New()
	buf := make([]byte, 8)

	// Encode and write all data to the hash
	binary.LittleEndian.PutUint64(buf, clientID)
	h.Write(buf)
	binary.LittleEndian.PutUint64(buf, reqNo)
	h.Write(buf)
	h.Write(data)

	// Return hash value
	return h.Sum(nil)
}
