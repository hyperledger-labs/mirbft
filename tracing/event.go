package tracing

type EventType int

const (
	PROPOSE EventType = iota
	PREPREPARE
	COMMIT
	CLIENT_SLACK
	REQ_SEND
	REQ_RECEIVE
	RESP_SEND
	RESP_RECEIVE
	ENOUGH_RESP
	REQ_FINISHED
	REQ_DELIVERED
	ETH_VOTE_SUBMIT
	ETH_VOTE_DONE
	CPU_USAGE
	MSG_BATCH
	BANDWIDTH
	BUCKET_STATE
	NEW_EPOCH
	VIEW_CHANGE
)

func (et EventType) String() string {
	return [...]string{
		"PROPOSE",
		"PREPREPARE",
		"COMMIT",
		"CLIENT_SLACK",
		"REQ_SEND",
		"REQ_RECEIVE",
		"RESP_SEND",
		"RESP_RECEIVE",
		"ENOUGH_RESP",
		"REQ_FINISHED",
		"REQ_DELIVERED",
		"ETH_VOTE_SUBMIT",
		"ETH_VOTE_DONE",
		"CPU_USAGE",
		"MSG_BATCH",
		"BANDWIDTH",
		"BUCKET_STATE",
		"NEW_EPOCH",
		"VIEW_CHANGE",
	}[et]
}

//type ProtocolEvent struct {
//	EventType EventType
//	Timestamp int64
//	PeerId    int32
//	SeqNr     int32
//}
//
//type RequestEvent struct {
//	EventType EventType
//	Timestamp int64
//	ClId      int32
//	ClSn      int32
//	PeerId    int32
//}
//
//type EthereumEvent struct {
//	EventType EventType
//	Timestamp int64
//	PeerId    int32
//	ConfigNr  int64
//	GasCost   int64
//}
//
type GenericEvent struct {
	EventType  EventType
	Timestamp  int64
	NodeId     int32
	SampledVal int64
	Val0       int64
}
