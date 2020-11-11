/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testengine

import (
	"fmt"
	"reflect"

	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"

	"google.golang.org/protobuf/proto"
)

// EventMangling is meant to be an easy way to construct test descriptions.
// Each method of a Mangling returns itself or another Mangling to make them easy
// to concatenate.  For instance:
//   MangleMessages().FromNodes(1,3).AtPercent(10).Drop()
// will for all messages from nodes 1 and 3, ten percent of the time, drop them.
// Note that order is important here.  Another perfectly valid string would be:
//   MangleMessages().AtPercent(10).FromNodes(1,3).Drop()
// But here, because filters are applied first to last, on 10 percent of messages,
// if they are from nodes 1 and 3, they will be dropped.

func MangleMsgs() *MsgMangling {
	mm := &MsgMangling{}

	mm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Step)
				return ok
			},
		},
	}
	initializeMangling(mm)

	return mm
}

func ClientProposal() *ClientMangling {
	cm := &ClientMangling{}

	cm.Filters = []mangleFilter{
		{
			stateEvent: func(event *pb.StateEvent) bool {
				_, ok := event.Type.(*pb.StateEvent_Propose)
				return ok
			},
		},
	}
	initializeMangling(cm)

	return cm
}

type InlineMangler func(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent

func (im InlineMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	return im(random, event)
}

type termination struct {
	Filters []mangleFilter
}

func (t termination) MangleWith(mangler Mangler) Mangler {
	return InlineMangler(func(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
		for _, filter := range t.Filters {
			if !filter.apply(random, event) {
				return []*rpb.RecordedEvent{event}
			}
		}

		return mangler.Mangle(random, event)
	})
}

func (t termination) Drop() Mangler {
	return t.MangleWith(DropMangler{})
}

func (t termination) Jitter(maxDelay int) Mangler {
	return t.MangleWith(&JitterMangler{MaxDelay: maxDelay})
}

func (t termination) Duplicate(maxDelay int) Mangler {
	return t.MangleWith(&DuplicateMangler{MaxDelay: maxDelay})
}

func (t termination) CrashAndRestartAfter(delay int64, initParms *pb.StateEvent_InitialParameters) Mangler {
	return t.MangleWith(&CrashAndRestartAfterMangler{
		InitParms: initParms,
		Delay:     delay,
	})
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

func initializeMangling(mangling interface{}) {
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
		if structField.Name == "Filters" || structField.Name == "termination" {
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

			filtersField.Set(reflect.Append(filtersField, result[0]))
			return []reflect.Value{value}
		})

		structValue.Field(i).Set(f)
	}
}

type MsgTypeMangling struct {
	termination

	FromSelf     func() *MsgTypeMangling
	FromNode     func(nodeID uint64) *MsgTypeMangling
	FromNodes    func(nodeIDs ...uint64) *MsgTypeMangling
	ToNode       func(nodeID uint64) *MsgTypeMangling
	ToNodes      func(nodeIDs ...uint64) *MsgTypeMangling
	AtPercent    func(percent int) *MsgTypeMangling
	WithSequence func(seqNo uint64) *MsgTypeMangling
	WithEpoch    func(epochNo uint64) *MsgTypeMangling
}

type MsgMangling struct {
	termination

	FromSelf     func() *MsgMangling
	FromNode     func(nodeID uint64) *MsgMangling
	FromNodes    func(nodeIDs ...uint64) *MsgMangling
	ToNode       func(nodeID uint64) *MsgMangling
	ToNodes      func(nodeIDs ...uint64) *MsgMangling
	AtPercent    func(percent int) *MsgMangling
	WithSequence func(seqNo uint64) *MsgMangling
}

type ClientMangling struct {
	termination

	ToNode     func(nodeID uint64) *ClientMangling
	ToNodes    func(nodeIDs ...uint64) *ClientMangling
	AtPercent  func(percent int) *ClientMangling
	FromClient func(clientId uint64) *ClientMangling
}

func (mm *MsgMangling) ofType(msgType reflect.Type) *MsgTypeMangling {
	mm.termination.Filters = append(mm.termination.Filters, mangleFilter{
		msgContents: func(msg *pb.Msg) bool {
			return reflect.TypeOf(msg.Type).AssignableTo(msgType)
		},
	})

	ppm := &MsgTypeMangling{
		termination: mm.termination,
	}

	initializeMangling(ppm)
	return ppm
}

func (mm *MsgMangling) OfTypePreprepare() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_Preprepare{}))
}

func (mm *MsgMangling) OfTypePrepare() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_Prepare{}))
}

func (mm *MsgMangling) OfTypeCommit() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_Commit{}))
}

func (mm *MsgMangling) OfTypeCheckpoint() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_Checkpoint{}))
}

func (mm *MsgMangling) OfTypeSuspect() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_Suspect{}))
}

func (mm *MsgMangling) OfTypeEpochChange() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_EpochChange{}))
}

func (mm *MsgMangling) OfTypeEpochChangeAck() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_EpochChangeAck{}))
}

func (mm *MsgMangling) OfTypeNewEpoch() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_NewEpoch{}))
}

func (mm *MsgMangling) OfTypeNewEpochEcho() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_NewEpochEcho{}))
}

func (mm *MsgMangling) OfTypeNewEpochReady() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_NewEpochReady{}))
}

func (mm *MsgMangling) OfTypeFetchBatch() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_FetchBatch{}))
}

func (mm *MsgMangling) OfTypeForwardBatch() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_ForwardBatch{}))
}

func (mm *MsgMangling) OfTypeFetchRequest() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_FetchRequest{}))
}

func (mm *MsgMangling) OfTypeForwardRequest() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_ForwardRequest{}))
}

func (mm *MsgMangling) OfTypeRequestAck() *MsgTypeMangling {
	return mm.ofType(reflect.TypeOf(&pb.Msg_RequestAck{}))
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

type DropMangler struct{}

func (DropMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	return nil
}

type DuplicateMangler struct {
	MaxDelay int
}

func (dm *DuplicateMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	clone := proto.Clone(event).(*rpb.RecordedEvent)
	delay := int64(random % dm.MaxDelay)
	clone.Time += delay
	return []*rpb.RecordedEvent{event, clone}
}

// JitterMangler will delay events a random amount of time, up to MaxDelay
type JitterMangler struct {
	MaxDelay int
}

func (jm *JitterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	delay := int64(random % jm.MaxDelay)
	event.Time += delay
	return []*rpb.RecordedEvent{event}
}

type CrashAndRestartAfterMangler struct {
	InitParms *pb.StateEvent_InitialParameters
	Delay     int64
}

func (cm CrashAndRestartAfterMangler) Mangle(random int, event *rpb.RecordedEvent) []*rpb.RecordedEvent {
	return []*rpb.RecordedEvent{
		event,
		{
			Time:   event.Time + cm.Delay,
			NodeId: cm.InitParms.Id,
			StateEvent: &pb.StateEvent{
				Type: &pb.StateEvent_Initialize{
					Initialize: cm.InitParms,
				},
			},
		},
	}
}
