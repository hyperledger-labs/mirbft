// Code generated by protoc-gen-go. DO NOT EDIT.
// source: eventpb/eventpb.proto

package eventpb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	isspb "github.com/hyperledger-labs/mirbft/pkg/pb/isspb"
	messagepb "github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	requestpb "github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Event represents a state event to be injected into the state machine
type Event struct {
	// TODO, normalize naming, for instance Complete/Completed
	//
	// Types that are valid to be assigned to Type:
	//	*Event_Tick
	//	*Event_WalAppend
	//	*Event_WalEntry
	//	*Event_Request
	//	*Event_HashRequest
	//	*Event_HashResult
	//	*Event_RequestReady
	//	*Event_SendMessage
	//	*Event_MessageReceived
	//	*Event_Deliver
	//	*Event_Iss
	//	*Event_PersistDummyBatch
	//	*Event_AnnounceDummyBatch
	//	*Event_StoreDummyRequest
	Type isEvent_Type `protobuf_oneof:"type"`
	// A list of follow-up events to process after this event has been processed.
	// This field is used if events need to be processed in a particular order.
	// For example, a message sending event must only be processed
	// after the corresponding entry has been persisted in the write-ahead log (WAL).
	// In this case, the WAL append event would be this event
	// and the next field would contain the message sending event.
	Next                 []*Event `protobuf:"bytes,100,rep,name=next,proto3" json:"next,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{0}
}

func (m *Event) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Event.Unmarshal(m, b)
}
func (m *Event) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Event.Marshal(b, m, deterministic)
}
func (m *Event) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event.Merge(m, src)
}
func (m *Event) XXX_Size() int {
	return xxx_messageInfo_Event.Size(m)
}
func (m *Event) XXX_DiscardUnknown() {
	xxx_messageInfo_Event.DiscardUnknown(m)
}

var xxx_messageInfo_Event proto.InternalMessageInfo

type isEvent_Type interface {
	isEvent_Type()
}

type Event_Tick struct {
	Tick *Tick `protobuf:"bytes,1,opt,name=tick,proto3,oneof"`
}

type Event_WalAppend struct {
	WalAppend *WALAppend `protobuf:"bytes,2,opt,name=wal_append,json=walAppend,proto3,oneof"`
}

type Event_WalEntry struct {
	WalEntry *WALEntry `protobuf:"bytes,3,opt,name=wal_entry,json=walEntry,proto3,oneof"`
}

type Event_Request struct {
	Request *requestpb.Request `protobuf:"bytes,4,opt,name=request,proto3,oneof"`
}

type Event_HashRequest struct {
	HashRequest *HashRequest `protobuf:"bytes,5,opt,name=hash_request,json=hashRequest,proto3,oneof"`
}

type Event_HashResult struct {
	HashResult *HashResult `protobuf:"bytes,6,opt,name=hash_result,json=hashResult,proto3,oneof"`
}

type Event_RequestReady struct {
	RequestReady *RequestReady `protobuf:"bytes,7,opt,name=request_ready,json=requestReady,proto3,oneof"`
}

type Event_SendMessage struct {
	SendMessage *SendMessage `protobuf:"bytes,8,opt,name=send_message,json=sendMessage,proto3,oneof"`
}

type Event_MessageReceived struct {
	MessageReceived *MessageReceived `protobuf:"bytes,9,opt,name=message_received,json=messageReceived,proto3,oneof"`
}

type Event_Deliver struct {
	Deliver *Deliver `protobuf:"bytes,10,opt,name=deliver,proto3,oneof"`
}

type Event_Iss struct {
	Iss *isspb.ISSEvent `protobuf:"bytes,11,opt,name=iss,proto3,oneof"`
}

type Event_PersistDummyBatch struct {
	PersistDummyBatch *PersistDummyBatch `protobuf:"bytes,101,opt,name=persist_dummy_batch,json=persistDummyBatch,proto3,oneof"`
}

type Event_AnnounceDummyBatch struct {
	AnnounceDummyBatch *AnnounceDummyBatch `protobuf:"bytes,102,opt,name=announce_dummy_batch,json=announceDummyBatch,proto3,oneof"`
}

type Event_StoreDummyRequest struct {
	StoreDummyRequest *StoreDummyRequest `protobuf:"bytes,103,opt,name=store_dummy_request,json=storeDummyRequest,proto3,oneof"`
}

func (*Event_Tick) isEvent_Type() {}

func (*Event_WalAppend) isEvent_Type() {}

func (*Event_WalEntry) isEvent_Type() {}

func (*Event_Request) isEvent_Type() {}

func (*Event_HashRequest) isEvent_Type() {}

func (*Event_HashResult) isEvent_Type() {}

func (*Event_RequestReady) isEvent_Type() {}

func (*Event_SendMessage) isEvent_Type() {}

func (*Event_MessageReceived) isEvent_Type() {}

func (*Event_Deliver) isEvent_Type() {}

func (*Event_Iss) isEvent_Type() {}

func (*Event_PersistDummyBatch) isEvent_Type() {}

func (*Event_AnnounceDummyBatch) isEvent_Type() {}

func (*Event_StoreDummyRequest) isEvent_Type() {}

func (m *Event) GetType() isEvent_Type {
	if m != nil {
		return m.Type
	}
	return nil
}

func (m *Event) GetTick() *Tick {
	if x, ok := m.GetType().(*Event_Tick); ok {
		return x.Tick
	}
	return nil
}

func (m *Event) GetWalAppend() *WALAppend {
	if x, ok := m.GetType().(*Event_WalAppend); ok {
		return x.WalAppend
	}
	return nil
}

func (m *Event) GetWalEntry() *WALEntry {
	if x, ok := m.GetType().(*Event_WalEntry); ok {
		return x.WalEntry
	}
	return nil
}

func (m *Event) GetRequest() *requestpb.Request {
	if x, ok := m.GetType().(*Event_Request); ok {
		return x.Request
	}
	return nil
}

func (m *Event) GetHashRequest() *HashRequest {
	if x, ok := m.GetType().(*Event_HashRequest); ok {
		return x.HashRequest
	}
	return nil
}

func (m *Event) GetHashResult() *HashResult {
	if x, ok := m.GetType().(*Event_HashResult); ok {
		return x.HashResult
	}
	return nil
}

func (m *Event) GetRequestReady() *RequestReady {
	if x, ok := m.GetType().(*Event_RequestReady); ok {
		return x.RequestReady
	}
	return nil
}

func (m *Event) GetSendMessage() *SendMessage {
	if x, ok := m.GetType().(*Event_SendMessage); ok {
		return x.SendMessage
	}
	return nil
}

func (m *Event) GetMessageReceived() *MessageReceived {
	if x, ok := m.GetType().(*Event_MessageReceived); ok {
		return x.MessageReceived
	}
	return nil
}

func (m *Event) GetDeliver() *Deliver {
	if x, ok := m.GetType().(*Event_Deliver); ok {
		return x.Deliver
	}
	return nil
}

func (m *Event) GetIss() *isspb.ISSEvent {
	if x, ok := m.GetType().(*Event_Iss); ok {
		return x.Iss
	}
	return nil
}

func (m *Event) GetPersistDummyBatch() *PersistDummyBatch {
	if x, ok := m.GetType().(*Event_PersistDummyBatch); ok {
		return x.PersistDummyBatch
	}
	return nil
}

func (m *Event) GetAnnounceDummyBatch() *AnnounceDummyBatch {
	if x, ok := m.GetType().(*Event_AnnounceDummyBatch); ok {
		return x.AnnounceDummyBatch
	}
	return nil
}

func (m *Event) GetStoreDummyRequest() *StoreDummyRequest {
	if x, ok := m.GetType().(*Event_StoreDummyRequest); ok {
		return x.StoreDummyRequest
	}
	return nil
}

func (m *Event) GetNext() []*Event {
	if m != nil {
		return m.Next
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Event) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Event_Tick)(nil),
		(*Event_WalAppend)(nil),
		(*Event_WalEntry)(nil),
		(*Event_Request)(nil),
		(*Event_HashRequest)(nil),
		(*Event_HashResult)(nil),
		(*Event_RequestReady)(nil),
		(*Event_SendMessage)(nil),
		(*Event_MessageReceived)(nil),
		(*Event_Deliver)(nil),
		(*Event_Iss)(nil),
		(*Event_PersistDummyBatch)(nil),
		(*Event_AnnounceDummyBatch)(nil),
		(*Event_StoreDummyRequest)(nil),
	}
}

type Tick struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Tick) Reset()         { *m = Tick{} }
func (m *Tick) String() string { return proto.CompactTextString(m) }
func (*Tick) ProtoMessage()    {}
func (*Tick) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{1}
}

func (m *Tick) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Tick.Unmarshal(m, b)
}
func (m *Tick) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Tick.Marshal(b, m, deterministic)
}
func (m *Tick) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Tick.Merge(m, src)
}
func (m *Tick) XXX_Size() int {
	return xxx_messageInfo_Tick.Size(m)
}
func (m *Tick) XXX_DiscardUnknown() {
	xxx_messageInfo_Tick.DiscardUnknown(m)
}

var xxx_messageInfo_Tick proto.InternalMessageInfo

type HashRequest struct {
	Data                 [][]byte    `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	Origin               *HashOrigin `protobuf:"bytes,2,opt,name=origin,proto3" json:"origin,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *HashRequest) Reset()         { *m = HashRequest{} }
func (m *HashRequest) String() string { return proto.CompactTextString(m) }
func (*HashRequest) ProtoMessage()    {}
func (*HashRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{2}
}

func (m *HashRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HashRequest.Unmarshal(m, b)
}
func (m *HashRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HashRequest.Marshal(b, m, deterministic)
}
func (m *HashRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HashRequest.Merge(m, src)
}
func (m *HashRequest) XXX_Size() int {
	return xxx_messageInfo_HashRequest.Size(m)
}
func (m *HashRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_HashRequest.DiscardUnknown(m)
}

var xxx_messageInfo_HashRequest proto.InternalMessageInfo

func (m *HashRequest) GetData() [][]byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *HashRequest) GetOrigin() *HashOrigin {
	if m != nil {
		return m.Origin
	}
	return nil
}

type HashResult struct {
	Digest               []byte      `protobuf:"bytes,1,opt,name=digest,proto3" json:"digest,omitempty"`
	Origin               *HashOrigin `protobuf:"bytes,2,opt,name=origin,proto3" json:"origin,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *HashResult) Reset()         { *m = HashResult{} }
func (m *HashResult) String() string { return proto.CompactTextString(m) }
func (*HashResult) ProtoMessage()    {}
func (*HashResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{3}
}

func (m *HashResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HashResult.Unmarshal(m, b)
}
func (m *HashResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HashResult.Marshal(b, m, deterministic)
}
func (m *HashResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HashResult.Merge(m, src)
}
func (m *HashResult) XXX_Size() int {
	return xxx_messageInfo_HashResult.Size(m)
}
func (m *HashResult) XXX_DiscardUnknown() {
	xxx_messageInfo_HashResult.DiscardUnknown(m)
}

var xxx_messageInfo_HashResult proto.InternalMessageInfo

func (m *HashResult) GetDigest() []byte {
	if m != nil {
		return m.Digest
	}
	return nil
}

func (m *HashResult) GetOrigin() *HashOrigin {
	if m != nil {
		return m.Origin
	}
	return nil
}

type HashOrigin struct {
	// Types that are valid to be assigned to Type:
	//	*HashOrigin_Request
	Type                 isHashOrigin_Type `protobuf_oneof:"type"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *HashOrigin) Reset()         { *m = HashOrigin{} }
func (m *HashOrigin) String() string { return proto.CompactTextString(m) }
func (*HashOrigin) ProtoMessage()    {}
func (*HashOrigin) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{4}
}

func (m *HashOrigin) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HashOrigin.Unmarshal(m, b)
}
func (m *HashOrigin) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HashOrigin.Marshal(b, m, deterministic)
}
func (m *HashOrigin) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HashOrigin.Merge(m, src)
}
func (m *HashOrigin) XXX_Size() int {
	return xxx_messageInfo_HashOrigin.Size(m)
}
func (m *HashOrigin) XXX_DiscardUnknown() {
	xxx_messageInfo_HashOrigin.DiscardUnknown(m)
}

var xxx_messageInfo_HashOrigin proto.InternalMessageInfo

type isHashOrigin_Type interface {
	isHashOrigin_Type()
}

type HashOrigin_Request struct {
	Request *requestpb.Request `protobuf:"bytes,1,opt,name=request,proto3,oneof"`
}

func (*HashOrigin_Request) isHashOrigin_Type() {}

func (m *HashOrigin) GetType() isHashOrigin_Type {
	if m != nil {
		return m.Type
	}
	return nil
}

func (m *HashOrigin) GetRequest() *requestpb.Request {
	if x, ok := m.GetType().(*HashOrigin_Request); ok {
		return x.Request
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*HashOrigin) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*HashOrigin_Request)(nil),
	}
}

type RequestReady struct {
	RequestRef           *requestpb.RequestRef `protobuf:"bytes,1,opt,name=request_ref,json=requestRef,proto3" json:"request_ref,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *RequestReady) Reset()         { *m = RequestReady{} }
func (m *RequestReady) String() string { return proto.CompactTextString(m) }
func (*RequestReady) ProtoMessage()    {}
func (*RequestReady) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{5}
}

func (m *RequestReady) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RequestReady.Unmarshal(m, b)
}
func (m *RequestReady) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RequestReady.Marshal(b, m, deterministic)
}
func (m *RequestReady) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RequestReady.Merge(m, src)
}
func (m *RequestReady) XXX_Size() int {
	return xxx_messageInfo_RequestReady.Size(m)
}
func (m *RequestReady) XXX_DiscardUnknown() {
	xxx_messageInfo_RequestReady.DiscardUnknown(m)
}

var xxx_messageInfo_RequestReady proto.InternalMessageInfo

func (m *RequestReady) GetRequestRef() *requestpb.RequestRef {
	if m != nil {
		return m.RequestRef
	}
	return nil
}

type SendMessage struct {
	Destinations         []uint64           `protobuf:"varint,1,rep,packed,name=destinations,proto3" json:"destinations,omitempty"`
	Msg                  *messagepb.Message `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *SendMessage) Reset()         { *m = SendMessage{} }
func (m *SendMessage) String() string { return proto.CompactTextString(m) }
func (*SendMessage) ProtoMessage()    {}
func (*SendMessage) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{6}
}

func (m *SendMessage) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SendMessage.Unmarshal(m, b)
}
func (m *SendMessage) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SendMessage.Marshal(b, m, deterministic)
}
func (m *SendMessage) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SendMessage.Merge(m, src)
}
func (m *SendMessage) XXX_Size() int {
	return xxx_messageInfo_SendMessage.Size(m)
}
func (m *SendMessage) XXX_DiscardUnknown() {
	xxx_messageInfo_SendMessage.DiscardUnknown(m)
}

var xxx_messageInfo_SendMessage proto.InternalMessageInfo

func (m *SendMessage) GetDestinations() []uint64 {
	if m != nil {
		return m.Destinations
	}
	return nil
}

func (m *SendMessage) GetMsg() *messagepb.Message {
	if m != nil {
		return m.Msg
	}
	return nil
}

type MessageReceived struct {
	From                 uint64             `protobuf:"varint,1,opt,name=from,proto3" json:"from,omitempty"`
	Msg                  *messagepb.Message `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *MessageReceived) Reset()         { *m = MessageReceived{} }
func (m *MessageReceived) String() string { return proto.CompactTextString(m) }
func (*MessageReceived) ProtoMessage()    {}
func (*MessageReceived) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{7}
}

func (m *MessageReceived) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MessageReceived.Unmarshal(m, b)
}
func (m *MessageReceived) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MessageReceived.Marshal(b, m, deterministic)
}
func (m *MessageReceived) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MessageReceived.Merge(m, src)
}
func (m *MessageReceived) XXX_Size() int {
	return xxx_messageInfo_MessageReceived.Size(m)
}
func (m *MessageReceived) XXX_DiscardUnknown() {
	xxx_messageInfo_MessageReceived.DiscardUnknown(m)
}

var xxx_messageInfo_MessageReceived proto.InternalMessageInfo

func (m *MessageReceived) GetFrom() uint64 {
	if m != nil {
		return m.From
	}
	return 0
}

func (m *MessageReceived) GetMsg() *messagepb.Message {
	if m != nil {
		return m.Msg
	}
	return nil
}

type WALAppend struct {
	Event                *Event   `protobuf:"bytes,1,opt,name=event,proto3" json:"event,omitempty"`
	RetentionIndex       uint64   `protobuf:"varint,2,opt,name=retention_index,json=retentionIndex,proto3" json:"retention_index,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *WALAppend) Reset()         { *m = WALAppend{} }
func (m *WALAppend) String() string { return proto.CompactTextString(m) }
func (*WALAppend) ProtoMessage()    {}
func (*WALAppend) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{8}
}

func (m *WALAppend) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WALAppend.Unmarshal(m, b)
}
func (m *WALAppend) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WALAppend.Marshal(b, m, deterministic)
}
func (m *WALAppend) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WALAppend.Merge(m, src)
}
func (m *WALAppend) XXX_Size() int {
	return xxx_messageInfo_WALAppend.Size(m)
}
func (m *WALAppend) XXX_DiscardUnknown() {
	xxx_messageInfo_WALAppend.DiscardUnknown(m)
}

var xxx_messageInfo_WALAppend proto.InternalMessageInfo

func (m *WALAppend) GetEvent() *Event {
	if m != nil {
		return m.Event
	}
	return nil
}

func (m *WALAppend) GetRetentionIndex() uint64 {
	if m != nil {
		return m.RetentionIndex
	}
	return 0
}

type WALEntry struct {
	Event                *Event   `protobuf:"bytes,1,opt,name=event,proto3" json:"event,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *WALEntry) Reset()         { *m = WALEntry{} }
func (m *WALEntry) String() string { return proto.CompactTextString(m) }
func (*WALEntry) ProtoMessage()    {}
func (*WALEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{9}
}

func (m *WALEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WALEntry.Unmarshal(m, b)
}
func (m *WALEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WALEntry.Marshal(b, m, deterministic)
}
func (m *WALEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WALEntry.Merge(m, src)
}
func (m *WALEntry) XXX_Size() int {
	return xxx_messageInfo_WALEntry.Size(m)
}
func (m *WALEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_WALEntry.DiscardUnknown(m)
}

var xxx_messageInfo_WALEntry proto.InternalMessageInfo

func (m *WALEntry) GetEvent() *Event {
	if m != nil {
		return m.Event
	}
	return nil
}

type Deliver struct {
	Sn                   uint64           `protobuf:"varint,1,opt,name=sn,proto3" json:"sn,omitempty"`
	Batch                *requestpb.Batch `protobuf:"bytes,2,opt,name=batch,proto3" json:"batch,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *Deliver) Reset()         { *m = Deliver{} }
func (m *Deliver) String() string { return proto.CompactTextString(m) }
func (*Deliver) ProtoMessage()    {}
func (*Deliver) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{10}
}

func (m *Deliver) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Deliver.Unmarshal(m, b)
}
func (m *Deliver) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Deliver.Marshal(b, m, deterministic)
}
func (m *Deliver) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Deliver.Merge(m, src)
}
func (m *Deliver) XXX_Size() int {
	return xxx_messageInfo_Deliver.Size(m)
}
func (m *Deliver) XXX_DiscardUnknown() {
	xxx_messageInfo_Deliver.DiscardUnknown(m)
}

var xxx_messageInfo_Deliver proto.InternalMessageInfo

func (m *Deliver) GetSn() uint64 {
	if m != nil {
		return m.Sn
	}
	return 0
}

func (m *Deliver) GetBatch() *requestpb.Batch {
	if m != nil {
		return m.Batch
	}
	return nil
}

type StoreDummyRequest struct {
	RequestRef           *requestpb.RequestRef `protobuf:"bytes,1,opt,name=request_ref,json=requestRef,proto3" json:"request_ref,omitempty"`
	Data                 []byte                `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *StoreDummyRequest) Reset()         { *m = StoreDummyRequest{} }
func (m *StoreDummyRequest) String() string { return proto.CompactTextString(m) }
func (*StoreDummyRequest) ProtoMessage()    {}
func (*StoreDummyRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{11}
}

func (m *StoreDummyRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StoreDummyRequest.Unmarshal(m, b)
}
func (m *StoreDummyRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StoreDummyRequest.Marshal(b, m, deterministic)
}
func (m *StoreDummyRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StoreDummyRequest.Merge(m, src)
}
func (m *StoreDummyRequest) XXX_Size() int {
	return xxx_messageInfo_StoreDummyRequest.Size(m)
}
func (m *StoreDummyRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_StoreDummyRequest.DiscardUnknown(m)
}

var xxx_messageInfo_StoreDummyRequest proto.InternalMessageInfo

func (m *StoreDummyRequest) GetRequestRef() *requestpb.RequestRef {
	if m != nil {
		return m.RequestRef
	}
	return nil
}

func (m *StoreDummyRequest) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type PersistDummyBatch struct {
	Sn                   uint64           `protobuf:"varint,1,opt,name=sn,proto3" json:"sn,omitempty"`
	Batch                *requestpb.Batch `protobuf:"bytes,2,opt,name=batch,proto3" json:"batch,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *PersistDummyBatch) Reset()         { *m = PersistDummyBatch{} }
func (m *PersistDummyBatch) String() string { return proto.CompactTextString(m) }
func (*PersistDummyBatch) ProtoMessage()    {}
func (*PersistDummyBatch) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{12}
}

func (m *PersistDummyBatch) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PersistDummyBatch.Unmarshal(m, b)
}
func (m *PersistDummyBatch) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PersistDummyBatch.Marshal(b, m, deterministic)
}
func (m *PersistDummyBatch) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PersistDummyBatch.Merge(m, src)
}
func (m *PersistDummyBatch) XXX_Size() int {
	return xxx_messageInfo_PersistDummyBatch.Size(m)
}
func (m *PersistDummyBatch) XXX_DiscardUnknown() {
	xxx_messageInfo_PersistDummyBatch.DiscardUnknown(m)
}

var xxx_messageInfo_PersistDummyBatch proto.InternalMessageInfo

func (m *PersistDummyBatch) GetSn() uint64 {
	if m != nil {
		return m.Sn
	}
	return 0
}

func (m *PersistDummyBatch) GetBatch() *requestpb.Batch {
	if m != nil {
		return m.Batch
	}
	return nil
}

type AnnounceDummyBatch struct {
	Sn                   uint64           `protobuf:"varint,1,opt,name=sn,proto3" json:"sn,omitempty"`
	Batch                *requestpb.Batch `protobuf:"bytes,2,opt,name=batch,proto3" json:"batch,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *AnnounceDummyBatch) Reset()         { *m = AnnounceDummyBatch{} }
func (m *AnnounceDummyBatch) String() string { return proto.CompactTextString(m) }
func (*AnnounceDummyBatch) ProtoMessage()    {}
func (*AnnounceDummyBatch) Descriptor() ([]byte, []int) {
	return fileDescriptor_e1d62373b81ab9ca, []int{13}
}

func (m *AnnounceDummyBatch) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AnnounceDummyBatch.Unmarshal(m, b)
}
func (m *AnnounceDummyBatch) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AnnounceDummyBatch.Marshal(b, m, deterministic)
}
func (m *AnnounceDummyBatch) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AnnounceDummyBatch.Merge(m, src)
}
func (m *AnnounceDummyBatch) XXX_Size() int {
	return xxx_messageInfo_AnnounceDummyBatch.Size(m)
}
func (m *AnnounceDummyBatch) XXX_DiscardUnknown() {
	xxx_messageInfo_AnnounceDummyBatch.DiscardUnknown(m)
}

var xxx_messageInfo_AnnounceDummyBatch proto.InternalMessageInfo

func (m *AnnounceDummyBatch) GetSn() uint64 {
	if m != nil {
		return m.Sn
	}
	return 0
}

func (m *AnnounceDummyBatch) GetBatch() *requestpb.Batch {
	if m != nil {
		return m.Batch
	}
	return nil
}

func init() {
	proto.RegisterType((*Event)(nil), "eventpb.Event")
	proto.RegisterType((*Tick)(nil), "eventpb.Tick")
	proto.RegisterType((*HashRequest)(nil), "eventpb.HashRequest")
	proto.RegisterType((*HashResult)(nil), "eventpb.HashResult")
	proto.RegisterType((*HashOrigin)(nil), "eventpb.HashOrigin")
	proto.RegisterType((*RequestReady)(nil), "eventpb.RequestReady")
	proto.RegisterType((*SendMessage)(nil), "eventpb.SendMessage")
	proto.RegisterType((*MessageReceived)(nil), "eventpb.MessageReceived")
	proto.RegisterType((*WALAppend)(nil), "eventpb.WALAppend")
	proto.RegisterType((*WALEntry)(nil), "eventpb.WALEntry")
	proto.RegisterType((*Deliver)(nil), "eventpb.Deliver")
	proto.RegisterType((*StoreDummyRequest)(nil), "eventpb.StoreDummyRequest")
	proto.RegisterType((*PersistDummyBatch)(nil), "eventpb.PersistDummyBatch")
	proto.RegisterType((*AnnounceDummyBatch)(nil), "eventpb.AnnounceDummyBatch")
}

func init() { proto.RegisterFile("eventpb/eventpb.proto", fileDescriptor_e1d62373b81ab9ca) }

var fileDescriptor_e1d62373b81ab9ca = []byte{
	// 795 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x55, 0x5d, 0x4f, 0xe3, 0x38,
	0x14, 0x4d, 0xdb, 0xb4, 0x85, 0xdb, 0x42, 0xa9, 0x81, 0x55, 0x60, 0x5f, 0x50, 0x40, 0xbb, 0x48,
	0xbb, 0xdb, 0xb2, 0x20, 0xad, 0xb4, 0xd2, 0xbc, 0x14, 0xc1, 0x28, 0x08, 0x66, 0x98, 0x71, 0x47,
	0x42, 0xe2, 0x25, 0x4a, 0x1a, 0x37, 0x89, 0x68, 0x9c, 0x4c, 0xec, 0x02, 0xfd, 0xbb, 0xf3, 0x4b,
	0x46, 0x76, 0x5c, 0xa7, 0x1f, 0x2f, 0x0c, 0x2f, 0xed, 0xfd, 0x3a, 0xc7, 0xd7, 0xf6, 0x3d, 0x0e,
	0xec, 0x93, 0x67, 0x42, 0x79, 0xe6, 0xf7, 0xd5, 0x7f, 0x2f, 0xcb, 0x53, 0x9e, 0xa2, 0xa6, 0x72,
	0x0f, 0x0f, 0x72, 0xf2, 0x7d, 0x4a, 0x98, 0xa8, 0xd0, 0x56, 0x51, 0x73, 0x78, 0x90, 0x10, 0xc6,
	0xbc, 0x90, 0x64, 0x7e, 0x5f, 0x5b, 0x2a, 0xd5, 0x8d, 0x19, 0xcb, 0xfc, 0xbe, 0xfc, 0x2d, 0x42,
	0xf6, 0x8f, 0x06, 0xd4, 0xaf, 0x05, 0x29, 0x3a, 0x06, 0x93, 0xc7, 0xa3, 0x27, 0xab, 0x72, 0x54,
	0x39, 0x6d, 0x9d, 0x6f, 0xf5, 0xe6, 0x2b, 0x7f, 0x8b, 0x47, 0x4f, 0x8e, 0x81, 0x65, 0x12, 0x5d,
	0x00, 0xbc, 0x78, 0x13, 0xd7, 0xcb, 0x32, 0x42, 0x03, 0xab, 0x2a, 0x4b, 0x91, 0x2e, 0x7d, 0x18,
	0xdc, 0x0d, 0x64, 0xc6, 0x31, 0xf0, 0xe6, 0x8b, 0x37, 0x29, 0x1c, 0x74, 0x06, 0xc2, 0x71, 0x09,
	0xe5, 0xf9, 0xcc, 0xaa, 0x49, 0x4c, 0x77, 0x11, 0x73, 0x2d, 0x12, 0x8e, 0x81, 0x37, 0x5e, 0xbc,
	0x89, 0xb4, 0x51, 0x0f, 0x9a, 0x6a, 0x5b, 0x96, 0xa9, 0xd6, 0x28, 0xb7, 0x89, 0x0b, 0xcb, 0x31,
	0xf0, 0xbc, 0x08, 0xfd, 0x0f, 0xed, 0xc8, 0x63, 0x91, 0x3b, 0x07, 0xd5, 0x25, 0x68, 0x4f, 0x2f,
	0xe2, 0x78, 0x2c, 0x2a, 0x61, 0xad, 0xa8, 0x74, 0xd1, 0x7f, 0xd0, 0x52, 0x50, 0x36, 0x9d, 0x70,
	0xab, 0x21, 0x91, 0xbb, 0x2b, 0x48, 0x91, 0x72, 0x0c, 0x0c, 0x91, 0xf6, 0xd0, 0x07, 0xd8, 0x52,
	0xab, 0xb9, 0x39, 0xf1, 0x82, 0x99, 0xd5, 0x94, 0xc8, 0x7d, 0x8d, 0x54, 0x0b, 0x60, 0x91, 0x74,
	0x0c, 0xdc, 0xce, 0x17, 0x7c, 0xd1, 0x30, 0x23, 0x34, 0x70, 0xd5, 0x0d, 0x59, 0x1b, 0x2b, 0x0d,
	0x0f, 0x09, 0x0d, 0x3e, 0x15, 0x39, 0xd1, 0x30, 0x2b, 0x5d, 0x74, 0x0d, 0x3b, 0x0a, 0xe5, 0xe6,
	0x64, 0x44, 0xe2, 0x67, 0x12, 0x58, 0x9b, 0x12, 0x6e, 0x69, 0xb8, 0xaa, 0xc5, 0x2a, 0xef, 0x18,
	0xb8, 0x93, 0x2c, 0x87, 0xd0, 0xdf, 0xd0, 0x0c, 0xc8, 0x24, 0x7e, 0x26, 0xb9, 0x05, 0x12, 0xbd,
	0xa3, 0xd1, 0x57, 0x45, 0x5c, 0x1c, 0xb0, 0x2a, 0x41, 0xc7, 0x50, 0x8b, 0x19, 0xb3, 0x5a, 0xb2,
	0xb2, 0xd3, 0x2b, 0x26, 0xe8, 0x66, 0x38, 0x94, 0xa3, 0xe3, 0x18, 0x58, 0x64, 0xd1, 0x1d, 0xec,
	0x66, 0x24, 0x67, 0x31, 0xe3, 0x6e, 0x30, 0x4d, 0x92, 0x99, 0xeb, 0x7b, 0x7c, 0x14, 0x59, 0x44,
	0x82, 0x0e, 0x35, 0xfd, 0x97, 0xa2, 0xe6, 0x4a, 0x94, 0x5c, 0x8a, 0x0a, 0xc7, 0xc0, 0xdd, 0x6c,
	0x35, 0x88, 0xee, 0x61, 0xcf, 0xa3, 0x34, 0x9d, 0xd2, 0x11, 0x59, 0xa2, 0x1b, 0x4b, 0xba, 0xdf,
	0x35, 0xdd, 0x40, 0x15, 0x2d, 0xf1, 0x21, 0x6f, 0x2d, 0x2a, 0xda, 0x63, 0x3c, 0xcd, 0xe7, 0x6c,
	0xf3, 0x59, 0x09, 0x57, 0xda, 0x1b, 0x8a, 0x1a, 0x09, 0x2b, 0x27, 0xa6, 0xcb, 0x56, 0x83, 0xc8,
	0x06, 0x93, 0x92, 0x57, 0x6e, 0x05, 0x47, 0xb5, 0xd3, 0xd6, 0xf9, 0xb6, 0x86, 0xcb, 0x13, 0xc1,
	0x32, 0x77, 0xd9, 0x00, 0x93, 0xcf, 0x32, 0x62, 0x37, 0xc0, 0x14, 0x2a, 0xb2, 0x3f, 0x43, 0x6b,
	0x61, 0x12, 0x11, 0x02, 0x33, 0xf0, 0xb8, 0x67, 0x55, 0x8e, 0x6a, 0xa7, 0x6d, 0x2c, 0x6d, 0xf4,
	0x17, 0x34, 0xd2, 0x3c, 0x0e, 0x63, 0xaa, 0xc4, 0xb5, 0x3c, 0x89, 0xf7, 0x32, 0x85, 0x55, 0x89,
	0xfd, 0x15, 0xa0, 0x9c, 0x4f, 0xf4, 0x1b, 0x34, 0x82, 0x38, 0x14, 0x5b, 0x12, 0x12, 0x6e, 0x63,
	0xe5, 0xfd, 0x1a, 0xe5, 0x55, 0x41, 0x59, 0x44, 0x17, 0x75, 0x58, 0x79, 0x83, 0x0e, 0xf5, 0x86,
	0x3f, 0x42, 0x7b, 0x71, 0xfc, 0x85, 0xc8, 0x4a, 0xb1, 0x8c, 0x15, 0xd7, 0xfe, 0x3a, 0x17, 0x26,
	0x63, 0x0c, 0x5a, 0x28, 0x63, 0xfb, 0x01, 0x5a, 0x0b, 0x4a, 0x40, 0x36, 0xb4, 0x03, 0xc2, 0x78,
	0x4c, 0x3d, 0x1e, 0xa7, 0x94, 0xc9, 0x83, 0x33, 0xf1, 0x52, 0x0c, 0x9d, 0x40, 0x2d, 0x61, 0xa1,
	0x7e, 0x9a, 0xca, 0x27, 0x70, 0xae, 0x09, 0x91, 0xb6, 0x6f, 0xa1, 0xb3, 0xa2, 0x11, 0x71, 0x1b,
	0xe3, 0x3c, 0x4d, 0x64, 0x73, 0x26, 0x96, 0xf6, 0x1b, 0xc9, 0x1e, 0x61, 0x53, 0xbf, 0x7c, 0xe8,
	0x04, 0xea, 0xf2, 0x78, 0xd5, 0x26, 0x57, 0x07, 0xa3, 0x48, 0xa2, 0x3f, 0xa1, 0x93, 0x13, 0x4e,
	0xa8, 0xe8, 0xd9, 0x8d, 0x69, 0x40, 0x5e, 0xe5, 0x22, 0x26, 0xde, 0xd6, 0xe1, 0x1b, 0x11, 0xb5,
	0xcf, 0x60, 0x63, 0xfe, 0x42, 0xbe, 0x8d, 0xda, 0x1e, 0x40, 0x53, 0x09, 0x18, 0x6d, 0x43, 0x95,
	0x51, 0xb5, 0xa1, 0x2a, 0xa3, 0xe8, 0x0f, 0xa8, 0x17, 0x1a, 0xaa, 0x2a, 0xc5, 0x97, 0x17, 0x20,
	0x25, 0x82, 0x8b, 0xb4, 0xed, 0x42, 0x77, 0x4d, 0x05, 0xef, 0xbd, 0x43, 0x3d, 0xe5, 0x55, 0x39,
	0x94, 0xd2, 0xb6, 0x6f, 0xa1, 0xbb, 0xf6, 0x0a, 0xbc, 0xbb, 0xdb, 0x3b, 0x40, 0xeb, 0x6f, 0xc0,
	0x7b, 0xd9, 0x2e, 0x2f, 0x1e, 0xff, 0x0d, 0x63, 0x1e, 0x4d, 0xfd, 0xde, 0x28, 0x4d, 0xfa, 0xd1,
	0x2c, 0x23, 0xf9, 0x84, 0x04, 0x21, 0xc9, 0xff, 0x99, 0x78, 0x3e, 0xeb, 0x27, 0x71, 0xee, 0x8f,
	0x79, 0x3f, 0x7b, 0x0a, 0xfb, 0xe5, 0xd7, 0xd9, 0x6f, 0xc8, 0x8f, 0xe9, 0xc5, 0xcf, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x32, 0xcc, 0x79, 0x34, 0xb7, 0x07, 0x00, 0x00,
}
