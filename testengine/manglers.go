/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"
	"reflect"

	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/pkg/eventlog/recorderpb"

	"google.golang.org/protobuf/proto"
)

type Mangler interface {
	Mangle(random int, event *rpb.RecordedEvent) []MangleResult
}

type MangleResult struct {
	Event    *rpb.RecordedEvent
	Remangle bool
}

// EventMangling is meant to be an easy way to construct test descriptions.
// Each method of a Mangling returns itself or another Mangling to make them easy
// to concatenate.  For instance:
//   MangleMessages().FromNodes(1,3).AtPercent(10).Drop()
// will for all messages from nodes 1 and 3, ten percent of the time, drop them.
// Note that order is important here.  Another perfectly valid string would be:
//   MangleMessages().AtPercent(10).FromNodes(1,3).Drop()
// But here, because filters are applied first to last, on 10 percent of messages,
// if they are from nodes 1 and 3, they will be dropped.

type MangleMatcher interface {
	Matches(random int, event *rpb.RecordedEvent) bool
}

// Until is useful to perform a mangling until some condition is complete.  This is useful
// especially for delaying an event until after a condition occurs, or for fixing a fault
// after some period of time.
func Until(matcher MangleMatcher) *Mangling {
	matched := false
	return &Mangling{
		Filter: InlineMatcher(func(random int, event *rpb.RecordedEvent) bool {
			if matched || matcher.Matches(random, event) {
				matched = true
				return false
			}

			return true
		}),
	}
}

// After is useful to begin a mangling after some action occurs.  This is useful especially
// for allowing the network to get into a desired state before injecting a fault.
func After(matcher MangleMatcher) *Mangling {
	matched := false
	return &Mangling{
		Filter: InlineMatcher(func(random int, event *rpb.RecordedEvent) bool {
			if matched || matcher.Matches(random, event) {
				matched = true
				return true
			}

			return false
		}),
	}
}

// For is a simple way to apply a mangler whenever a condition is satisfied.
func For(matcher MangleMatcher) *Mangling {
	return &Mangling{
		Filter: matcher,
	}
}

// Mangling is usually constructed via For/After/Until and is used to
// conditionally apply a Mangler.
type Mangling struct {
	Filter MangleMatcher
}

func (m *Mangling) Do(mangler Mangler) Mangler {
	return InlineMangler(func(random int, event *rpb.RecordedEvent) []MangleResult {
		if !m.Filter.Matches(random, event) {
			return []MangleResult{
				{
					Event: event,
				},
			}
		}

		return mangler.Mangle(random, event)
	})
}

func (m *Mangling) Drop() Mangler {
	return m.Do(DropMangler{})
}

func (m *Mangling) Jitter(maxDelay int) Mangler {
	return m.Do(&JitterMangler{MaxDelay: maxDelay})
}

func (m *Mangling) Duplicate(maxDelay int) Mangler {
	return m.Do(&DuplicateMangler{MaxDelay: maxDelay})
}

func (m *Mangling) Delay(delay int) Mangler {
	return m.Do(&DelayMangler{Delay: delay})
}

func (m *Mangling) CrashAndRestartAfter(delay int64, initParms *pb.StateEvent_InitialParameters) Mangler {
	return m.Do(&CrashAndRestartAfterMangler{
		InitParms: initParms,
		Delay:     delay,
	})
}

func MatchMsgs() *MsgMatching {
	return newMsgMatching()
}

func MatchNodeStartup() *StartupMatching {
	return newStartupMatching()
}

func MatchClientProposal() *ClientMatching {
	cm := &ClientMatching{}

	cm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Propose)
				return ok
			},
		},
	}
	initializeMatching(cm)

	return cm
}

type InlineMatcher func(random int, event *rpb.RecordedEvent) bool

func (im InlineMatcher) Matches(random int, event *rpb.RecordedEvent) bool {
	return im(random, event)
}

type InlineMangler func(random int, event *rpb.RecordedEvent) []MangleResult

func (im InlineMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	return im(random, event)
}

type mangleFilter struct {
	clientID    func(id uint64) bool
	msgContents func(msg *pb.Msg) bool
	msgSeqNo    func(seqNo uint64) bool
	msgEpoch    func(seqNo uint64) bool
	msgSource   func(target, source uint64) bool
	stateEvent  func(event *pb.StateEvent) bool
	target      func(target uint64) bool
	blind       func(random int) bool
}

func (mf mangleFilter) apply(random int, event *rpb.RecordedEvent) bool {
	switch {
	case mf.clientID != nil:
		return mf.clientID(event.StateEvent.Type.(*pb.StateEvent_Propose).Propose.Request.ClientId)
	case mf.msgContents != nil:
		return mf.msgContents(event.StateEvent.Type.(*pb.StateEvent_Step).Step.Msg)
	case mf.msgEpoch != nil:
		var epoch uint64
		switch c := event.StateEvent.Type.(*pb.StateEvent_Step).Step.Msg.Type.(type) {
		case *pb.Msg_Preprepare:
			epoch = c.Preprepare.Epoch
		case *pb.Msg_Prepare:
			epoch = c.Prepare.Epoch
		case *pb.Msg_Commit:
			epoch = c.Commit.Epoch
		case *pb.Msg_Suspect:
			epoch = c.Suspect.Epoch
		case *pb.Msg_EpochChange:
			epoch = c.EpochChange.NewEpoch
		case *pb.Msg_EpochChangeAck:
			epoch = c.EpochChangeAck.EpochChange.NewEpoch
		case *pb.Msg_NewEpoch:
			epoch = c.NewEpoch.NewConfig.Config.Number
		case *pb.Msg_NewEpochEcho:
			epoch = c.NewEpochEcho.Config.Number
		case *pb.Msg_NewEpochReady:
			epoch = c.NewEpochReady.Config.Number
		default:
			return false
		}

		return mf.msgEpoch(epoch)
	case mf.msgSeqNo != nil:
		var seqNo uint64
		switch c := event.StateEvent.Type.(*pb.StateEvent_Step).Step.Msg.Type.(type) {
		case *pb.Msg_Preprepare:
			seqNo = c.Preprepare.SeqNo
		case *pb.Msg_Prepare:
			seqNo = c.Prepare.SeqNo
		case *pb.Msg_Commit:
			seqNo = c.Commit.SeqNo
		case *pb.Msg_Checkpoint:
			seqNo = c.Checkpoint.SeqNo
		case *pb.Msg_FetchBatch:
			seqNo = c.FetchBatch.SeqNo
		case *pb.Msg_ForwardBatch:
			seqNo = c.ForwardBatch.SeqNo
		default:
			return false
		}

		return mf.msgSeqNo(seqNo)
	case mf.msgSource != nil:
		return mf.msgSource(event.NodeId, event.StateEvent.Type.(*pb.StateEvent_Step).Step.Source)
	case mf.stateEvent != nil:
		return mf.stateEvent(event.StateEvent)
	case mf.target != nil:
		return mf.target(event.NodeId)
	case mf.blind != nil:
		return mf.blind(random)
	default:
		panic("no function set in manglefilter")
	}
}

func initializeMatching(mangling interface{}) {
	value := reflect.ValueOf(mangling)
	if value.Kind() != reflect.Ptr {
		panic("expected mangling to be a pointer")
	}

	structValue := value.Elem()
	if structValue.Kind() != reflect.Struct {
		panic("expected mangling to point to a struct")
	}

	filtersField := structValue.FieldByName("Filters")
	if filtersField.Kind() != reflect.Slice {
		panic("expected filters to be of type Slice")
	}

	structType := structValue.Type()
	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		if structField.Name == "Filters" || structField.Name == "matching" {
			continue
		}

		if structField.Type.Kind() != reflect.Func {
			panic("expected mangling members to be functions")
		}

		baseStruct := reflect.ValueOf(baseMangling{})
		baseMethod, ok := baseStruct.Type().MethodByName(structField.Name)
		if !ok {
			panic(fmt.Sprintf("implemention for %s not found in base mangling", structField.Name))
		}

		f := reflect.MakeFunc(structField.Type, func(args []reflect.Value) []reflect.Value {
			var result []reflect.Value
			argsWithReceiver := append([]reflect.Value{baseStruct}, args...)
			if structField.Type.IsVariadic() {
				result = baseMethod.Func.CallSlice(argsWithReceiver)
			} else {
				result = baseMethod.Func.Call(argsWithReceiver)
			}

			if len(result) != 1 {
				panic(fmt.Sprintf("expected only one result but got %d", len(result)))
			}

			mf, ok := result[0].Interface().(mangleFilter)
			if !ok {
				panic(fmt.Sprintf("expected result of type mangleFilter but got %T", mf))
			}

			if structField.Type.NumOut() != 1 {
				panic(fmt.Sprintf("expected field to only output 1 result but got %d", structField.Type.NumOut()))
			}

			outType := structField.Type.Out(0)
			if outType.Kind() != reflect.Ptr {
				panic(fmt.Sprintf("expected return type kind to be a ptr, but got %v", outType.Kind()))
			}

			newValue := reflect.New(outType.Elem())
			newValue.Elem().FieldByName("Filters").Set(reflect.Append(filtersField, result[0]))
			initializeMatching(newValue.Interface())

			return []reflect.Value{newValue}
		})

		structValue.Field(i).Set(f)
	}
}

type MsgTypeMatching struct {
	matching

	FromSelf     func() *MsgTypeMatching
	FromNode     func(nodeID uint64) *MsgTypeMatching
	FromNodes    func(nodeIDs ...uint64) *MsgTypeMatching
	ToNode       func(nodeID uint64) *MsgTypeMatching
	ToNodes      func(nodeIDs ...uint64) *MsgTypeMatching
	AtPercent    func(percent int) *MsgTypeMatching
	WithSequence func(seqNo uint64) *MsgTypeMatching
	WithEpoch    func(epochNo uint64) *MsgTypeMatching
}

type MsgMatching struct {
	matching

	FromSelf             func() *MsgMatching
	FromNode             func(nodeID uint64) *MsgMatching
	FromNodes            func(nodeIDs ...uint64) *MsgMatching
	ToNode               func(nodeID uint64) *MsgMatching
	ToNodes              func(nodeIDs ...uint64) *MsgMatching
	AtPercent            func(percent int) *MsgMatching
	WithSequence         func(seqNo uint64) *MsgMatching
	OfTypePreprepare     func() *MsgTypeMatching
	OfTypePrepare        func() *MsgTypeMatching
	OfTypeCommit         func() *MsgTypeMatching
	OfTypeCheckpoint     func() *MsgTypeMatching
	OfTypeSuspect        func() *MsgTypeMatching
	OfTypeEpochChange    func() *MsgTypeMatching
	OfTypeEpochChangeAck func() *MsgTypeMatching
	OfTypeNewEpoch       func() *MsgTypeMatching
	OfTypeNewEpochEcho   func() *MsgTypeMatching
	OfTypeNewEpochReady  func() *MsgTypeMatching
	OfTypeFetchBatch     func() *MsgTypeMatching
	OfTypeForwardBatch   func() *MsgTypeMatching
	OfTypeRequestAck     func() *MsgTypeMatching
}

func newMsgMatching() *MsgMatching {
	mm := &MsgMatching{}

	mm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Step)
				return ok
			},
		},
	}
	initializeMatching(mm)

	return mm
}

type StartupMatching struct {
	matching

	ForNode  func(nodeID uint64) *StartupMatching
	ForNodes func(nodeIDs ...uint64) *StartupMatching
}

func newStartupMatching() *StartupMatching {
	sm := &StartupMatching{}

	sm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Initialize)
				return ok
			},
		},
	}
	initializeMatching(sm)

	return sm
}

type ClientMatching struct {
	matching

	ToNode     func(nodeID uint64) *ClientMatching
	ToNodes    func(nodeIDs ...uint64) *ClientMatching
	AtPercent  func(percent int) *ClientMatching
	FromClient func(clientId uint64) *ClientMatching
}

// XXX tmp staticcheck hack
var _ = newClientMatching

func newClientMatching() *ClientMatching {
	cm := &ClientMatching{}

	cm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Propose)
				return ok
			},
		},
	}
	initializeMatching(cm)

	return cm
}

type matching struct {
	Filters []mangleFilter
}

func (m matching) Matches(random int, event *rpb.RecordedEvent) bool {
	for _, filter := range m.Filters {
		if !filter.apply(random, event) {
			return false
		}
	}

	return true
}

type baseMangling struct{}

// FromSelf may only be safely bound into a mangling if
// the mangling ensures all events are messages.  Note,
// it is generally unsafe to modify these events, as
// safety requires reliable links to ourselves.  But, this
// can often be used as a useful trigger for other events.
func (baseMangling) FromSelf() mangleFilter {
	return mangleFilter{
		msgSource: func(target, actualSource uint64) bool {
			return target == actualSource
		},
	}
}

// FromNode may only be safely bound into a mangling if
// the mangling ensures all events are messages.  Note,
// this mangling ignores self-referential messages
func (baseMangling) FromNode(source uint64) mangleFilter {
	return mangleFilter{
		msgSource: func(target, actualSource uint64) bool {
			return actualSource == source && target != actualSource
		},
	}
}

// FromNodes may only be safely bound into a mangling if
// the mangling ensures all events are messages.  Note,
// this mangling ignores self-referential messages
func (baseMangling) FromNodes(sources ...uint64) mangleFilter {
	return mangleFilter{
		msgSource: func(target, actualSource uint64) bool {
			if target == actualSource {
				return false
			}
			for _, source := range sources {
				if source == actualSource {
					return true
				}
			}
			return false
		},
	}
}

// ForNode is a synonymn for ToNode
func (b baseMangling) ForNode(target uint64) mangleFilter {
	return b.ToNode(target)
}

// ForNodes is a synonymn for ToNodes
func (b baseMangling) ForNodes(targets ...uint64) mangleFilter {
	return b.ToNodes(targets...)
}

// ToNode may be safely bound into all manglings.
func (baseMangling) ToNode(target uint64) mangleFilter {
	return mangleFilter{
		target: func(actualNode uint64) bool {
			return target == actualNode
		},
	}
}

// ToNodes may be safely bound into all manglings.
func (baseMangling) ToNodes(targets ...uint64) mangleFilter {
	return mangleFilter{
		target: func(actualNode uint64) bool {
			for _, target := range targets {
				if target == actualNode {
					return true
				}
			}
			return false
		},
	}
}

// AtPercent may be safely bound into all manglings.
func (baseMangling) AtPercent(percent int) mangleFilter {
	return mangleFilter{
		blind: func(random int) bool {
			return random%100 <= percent
		},
	}
}

// WithSequence may only be safely bound into a mangling if
// the mangling ensures all events are messages.
func (baseMangling) WithSequence(seqNo uint64) mangleFilter {
	return mangleFilter{
		msgSeqNo: func(actualSeqNo uint64) bool {
			return seqNo == actualSeqNo
		},
	}
}

// WithEpoch may only be safely bound into a mangling if
// the mangling ensures all events are messages.
func (baseMangling) WithEpoch(epochNo uint64) mangleFilter {
	return mangleFilter{
		msgEpoch: func(actualEpoch uint64) bool {
			return epochNo == actualEpoch
		},
	}
}

// FromClient may only be safely bound into a mangling if
// the mangling ensures all events are client proposals.
func (baseMangling) FromClient(clientID uint64) mangleFilter {
	return mangleFilter{
		clientID: func(actualClientID uint64) bool {
			return clientID == actualClientID
		},
	}
}

func ofType(msgType reflect.Type) mangleFilter {
	return mangleFilter{
		msgContents: func(msg *pb.Msg) bool {
			return reflect.TypeOf(msg.Type).AssignableTo(msgType)
		},
	}
}

// OfTypePreprepare may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypePreprepare() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_Preprepare{}))
}

// OfTypePrepare may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypePrepare() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_Prepare{}))
}

// OfTypeCommit may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeCommit() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_Commit{}))
}

// OfTypeCheckpoint may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeCheckpoint() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_Checkpoint{}))
}

// OfTypeSuspect may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeSuspect() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_Suspect{}))
}

// OfTypeEpochChange may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeEpochChange() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_EpochChange{}))
}

// OfTypeEpochChangeAck may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeEpochChangeAck() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_EpochChangeAck{}))
}

// OfTypeNewEpoch may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeNewEpoch() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_NewEpoch{}))
}

// OfTypeNewEpochEcho may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeNewEpochEcho() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_NewEpochEcho{}))
}

// OfTypeNewEpochReady may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeNewEpochReady() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_NewEpochReady{}))
}

// OfTypeFetchBatch may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeFetchBatch() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_FetchBatch{}))
}

// OfTypeForwardBatch may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeForwardBatch() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_ForwardBatch{}))
}

// OfTypeFetchRequest may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeFetchRequest() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_FetchRequest{}))
}

// OfTypeForwardRequest may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeForwardRequest() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_ForwardRequest{}))
}

// OfTypeRequestAck may only be safely bound to mangling if
// the mangling ensures all events are step messages.
func (baseMangling) OfTypeRequestAck() mangleFilter {
	return ofType(reflect.TypeOf(&pb.Msg_RequestAck{}))
}

type DropMangler struct{}

func (DropMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	return nil
}

type DuplicateMangler struct {
	MaxDelay int
}

func (dm *DuplicateMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	clone := proto.Clone(event).(*rpb.RecordedEvent)
	delay := int64(random % dm.MaxDelay)
	clone.Time += delay
	return []MangleResult{
		{
			Event: event,
		},
		{
			Event: clone,
		},
	}
}

// JitterMangler will delay events a random amount of time, up to MaxDelay
type JitterMangler struct {
	MaxDelay int
}

func (jm *JitterMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	delay := int64(random % jm.MaxDelay)
	event.Time += delay
	return []MangleResult{
		{
			Event: event,
		},
	}
}

// DelayMangler will delay events a specified amount of time
type DelayMangler struct {
	Delay int
}

func (dm *DelayMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	event.Time += int64(dm.Delay)
	return []MangleResult{
		{
			Event:    event,
			Remangle: true,
		},
	}
}

type CrashAndRestartAfterMangler struct {
	InitParms *pb.StateEvent_InitialParameters
	Delay     int64
}

func (cm CrashAndRestartAfterMangler) Mangle(random int, event *rpb.RecordedEvent) []MangleResult {
	return []MangleResult{
		{
			Event: event,
		},
		{
			Event: &rpb.RecordedEvent{
				Time:   event.Time + cm.Delay,
				NodeId: cm.InitParms.Id,
				StateEvent: &pb.StateEvent{
					Type: &pb.StateEvent_Initialize{
						Initialize: cm.InitParms,
					},
				},
			},
		},
	}
}
