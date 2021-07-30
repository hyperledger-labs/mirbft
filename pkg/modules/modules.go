package modules

import (
	"github.com/hyperledger-labs/mirbft/pkg/events"
	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
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
	Snapshot(networkConfig *msgs.NetworkState_Config, clientsState []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error)
	TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error)
}

type WAL interface {
	Write(index uint64, entry *msgs.Persistent) error
	Truncate(index uint64) error
	Sync() error
	LoadAll(forEach func(index uint64, p *msgs.Persistent)) error
}

// The RequestStore store persistently stores the payloads and authentication attributes of received requests.
// Each request is referenced using a msgs.RequestRef structure that contains the following fields:
// - ClientID: The numeric ID of the client that submitted the request.
// -   ReqNo: The client-local sequence number of the request (how many request the client issued before this request).
// -  Digest: A hash of the request payload.
// Along the request data (stored and retrieved using PutRequest() and GetRequest(), respectively),
// the RequestStore stores the authentication status of each request.
// A Boolean value (set and queried using SetAuthenticated() and IsAuthenticated(), respectively)
// signifies whether the request is authenticated to the local node.
// I.e, whether the local node believes that the request has indeed been sent by the client.
// This does not necessarily mean, however, that the local node can convince other nodes
// about the request's authenticity (e.g. if the local node received the request over an authenticated channel).
// Note that the authenticated flag cannot be cleared, as this would not be meaningful given its semantics.
//
// For purpose of convincing the rest of the system about a request's authenticity,
// an authenticator (e.g. a cryptographic signature) may be attached to the request.
// The authenticator is stored and retrieved using PutAuthenticator() and GetAuthenticator(), respectively.
// Note in particular that, in the context of a BFT TOB system, a node may accept a proposed request
// if the request is authenticated, even though the node does not have an authenticator
// (e.g. if the request has ben obtained directly from the client, but is not signed).
// For proposing a request, an authenticator is necessary to make sure that other correct nodes will accept the request.
// All effects of method invocations that change the state of the RequestStore can only be guaranteed to be persisted
// When a subsequent invocation of Sync() returns. Without a call to Sync(), the effects may or may not be persisted.
//
// TODO: Define functions for removal of old data.
type RequestStore interface {

	// PutRequest stores request the passed request data associated with the request reference.
	PutRequest(reqRef *msgs.RequestRef, data []byte) error

	// GetRequest returns the stored request data associated with the passed request reference.
	// If no data is stored under the given reference, the returned error will be non-nil.
	GetRequest(reqRef *msgs.RequestRef) ([]byte, error)

	// SetAuthenticated marks the referenced request as authenticated.
	// A request being authenticated means that the local node believes that
	// the request has indeed been sent by the client. This does not necessarily mean, however,
	// that the local node can convince other nodes about the request's authenticity
	// (e.g. if the local node received the request over an authenticated channel but the request is not signed).
	SetAuthenticated(reqRef *msgs.RequestRef) error

	// IsAuthenticated returns true if the request is authenticated, false otherwise.
	IsAuthenticated(reqRef *msgs.RequestRef) (bool, error)

	// PutAuthenticator stores an authenticator associated with the referenced request.
	// If an authenticator is already stored under the same reference, it will be overwritten.
	PutAuthenticator(reqRef *msgs.RequestRef, auth []byte) error

	// GetAuthenticator returns the stored authenticator associated with the passed request reference.
	// If no authenticator is stored under the given reference, the returned error will be non-nil.
	GetAuthenticator(reqRef *msgs.RequestRef) ([]byte, error)

	// GetDigestsByID returns a list of request Digests for which any information
	// (request data, authentication, or authenticator) is stored in the RequestStore.
	GetDigestsByID(clientID, reqNo uint64) ([][]byte, error)

	// Sync blocks until the effects of all preceding method invocations have been persisted.
	Sync() error

	// DEBUG
	PutAllocation(clientID, reqNo uint64, digest []byte) error
	GetAllocation(clientID, reqNo uint64) ([]byte, error)
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
	Propose(reqNo uint64, data []byte) (*events.EventList, error)
}

type StateMachine interface {

	// ApplyEvent applies an event to the state machine, making it advance its state
	// and potentially output a list of actions that the application of this event results in.
	// TODO: move the ActionList type out of the statemachine package.
	ApplyEvent(stateEvent *state.Event) *events.EventList

	// Status returns the current state of the state machine.
	// TODO: Make the data type protocol-independent,
	//       as we aim for the possibility to use different state machines implementing different protocols.
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
