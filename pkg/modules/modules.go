package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
	"github.com/hyperledger-labs/mirbft/pkg/statemachine"
	"github.com/hyperledger-labs/mirbft/pkg/status"
	"hash"
)

type Modules struct {
	Net          Net
	Hasher       Hasher
	App          App
	WAL          WAL
	RequestStore RequestStore
	StateMachine StateMachine
	Interceptor  EventInterceptor
}

type Hasher interface {
	New() hash.Hash
}

// The Net
type Net interface {
	Send(dest uint64, msg *msgs.Msg)
}

type App interface {
	Apply(*msgs.QEntry) error
	Snap(networkConfig *msgs.NetworkState_Config, clientsState []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error)
	TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error)
}

type WAL interface {
	Write(index uint64, entry *msgs.Persistent) error
	Truncate(index uint64) error
	Sync() error
	LoadAll(forEach func(index uint64, p *msgs.Persistent)) error
}

type RequestStore interface {
	GetAllocation(clientID, reqNo uint64) ([]byte, error)
	PutAllocation(clientID, reqNo uint64, digest []byte) error
	GetRequest(requestAck *msgs.RequestAck) ([]byte, error)
	PutRequest(requestAck *msgs.RequestAck, data []byte) error
	Sync() error
}

type Clients interface {
	Client(clientID uint64) Client
}

type Client interface {

	// TODO: Potentially refactor to not export these methods.
	AddCorrectDigest(reqNo uint64, digest []byte) error
	Allocate(reqNo uint64) ([]byte, error)
	StateApplied(state *msgs.NetworkState_Client)

	// TODO: Make sure this function is useful. If not, remove it.
	NextReqNo() (uint64, error)
	Propose(reqNo uint64, data []byte) (*statemachine.EventList, error)
}

type StateMachine interface {
	// TODO: move the ActionList type out of the statemachine package.
	ApplyEvent(stateEvent *state.Event) *statemachine.ActionList
	Status() (s *status.StateMachine, err error)
}

// EventInterceptor provides a way for a consumer to gain insight into
// the internal operation of the state machine.  And is usually not
// interesting outside of debugging or testing scenarios.  Note, this
// is applied inside the serializer, so any blocking will prevent the
// event from arriving at the state machine until it returns.
type EventInterceptor interface {
	// Intercept is invoked prior to passing each state event to
	// the state machine.  If Intercept returns an error, the
	// state machine halts.
	Intercept(s *state.Event) error
}
