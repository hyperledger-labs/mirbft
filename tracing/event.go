// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
