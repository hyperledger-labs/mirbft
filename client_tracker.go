/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"

	pb "github.com/IBM/mirbft/mirbftpb"
	// "github.com/IBM/mirbft/status"
)

type clientTracker struct {
	logger    Logger
	myConfig  *pb.StateEvent_InitialParameters
	persisted *persisted

	networkConfig *pb.NetworkState_Config
	readyList     *readyList
	availableList *availableList // A list of requests which have f+1 ACKs and the requestData
	clientStates  []*pb.NetworkState_Client
}

func newClientTracker(persisted *persisted, myConfig *pb.StateEvent_InitialParameters, logger Logger) *clientTracker {
	return &clientTracker{
		logger:    logger,
		myConfig:  myConfig,
		persisted: persisted,
	}
}

func (ct *clientTracker) reinitialize() {
	var lowCEntry, highCEntry *pb.CEntry

	ct.persisted.iterate(logIterator{
		onCEntry: func(cEntry *pb.CEntry) {
			if lowCEntry == nil {
				lowCEntry = cEntry
			}
			highCEntry = cEntry
		},
	})

	assertNotEqual(lowCEntry, nil, "log must have checkpoint")

	ct.networkConfig = lowCEntry.NetworkState.Config
	ct.availableList = newAvailableList()
	ct.readyList = newReadyList()
	ct.clientStates = highCEntry.NetworkState.Clients

}

func (ct *clientTracker) addReady(crn *clientReqNo) {
	ct.readyList.pushBack(crn)
}

func (ct *clientTracker) addAvailable(req *pb.RequestAck) {
	ct.availableList.pushBack(req)
}

func (ct *clientTracker) allocate(seqNo uint64, state *pb.NetworkState) {
	stateMap := map[uint64]*pb.NetworkState_Client{}
	for _, client := range state.Clients {
		stateMap[client.Id] = client
	}
	ct.availableList.garbageCollect(stateMap)
	ct.readyList.garbageCollect(seqNo)
}

// appendList is a data structure uniquely suited to the operations of the state machine
// it allows for a single iterator consumer, which may be reset on events like epoch change.
// Entries are first added into the 'pending' list, and as they are iterated over, they
// are moved to the consumed list.  At any point, entries may be removed from either list.
// The behavior of the iterator is always to simply begin at the head of the pending list,
// moving elements to the consumed list until it is exhausted.  New elements are always
// pushed onto the back of the pending list.
type appendList struct {
	consumed *list.List
	pending  *list.List
}

func newAppendList() *appendList {
	return &appendList{
		consumed: list.New(),
		pending:  list.New(),
	}
}

func (al *appendList) resetIterator() {
	al.pending.PushFrontList(al.consumed)
	al.consumed = list.New()
}

func (al *appendList) hasNext() bool {
	return al.pending.Len() > 0
}

func (al *appendList) next() interface{} {
	value := al.pending.Remove(al.pending.Front())
	al.consumed.PushBack(value)
	return value
}

func (al *appendList) pushBack(value interface{}) {
	al.pending.PushBack(value)
}

func (al *appendList) garbageCollect(gcFunc func(value interface{}) bool) {
	el := al.consumed.Front()
	for el != nil {
		if gcFunc(el.Value) {
			xel := el
			el = el.Next()
			al.consumed.Remove(xel)
			continue
		}

		el = el.Next()
	}

	el = al.pending.Front()
	for el != nil {
		if gcFunc(el.Value) {
			xel := el
			el = el.Next()
			al.pending.Remove(xel)
			continue
		}

		el = el.Next()
	}
}

type readyList struct {
	appendList *appendList
}

func newReadyList() *readyList {
	return &readyList{
		appendList: newAppendList(),
	}
}

func (rl *readyList) resetIterator() {
	rl.appendList.resetIterator()
}

func (rl *readyList) hasNext() bool {
	return rl.appendList.hasNext()
}

func (rl *readyList) next() *clientReqNo {
	return rl.appendList.next().(*clientReqNo)
}

func (rl *readyList) pushBack(crn *clientReqNo) {
	rl.appendList.pushBack(crn)
}

func (rl *readyList) garbageCollect(seqNo uint64) {
	rl.appendList.garbageCollect(func(value interface{}) bool {
		crn := value.(*clientReqNo)
		c := crn.committed
		return c != nil && *c <= seqNo
	})
}

type availableList struct {
	appendList *appendList
}

func newAvailableList() *availableList {
	return &availableList{
		appendList: newAppendList(),
	}
}

func (al *availableList) pushBack(ack *pb.RequestAck) {
	al.appendList.pushBack(ack)
}

func (al *availableList) resetIterator() {
	al.appendList.resetIterator()
}

func (al *availableList) hasNext() bool {
	return al.appendList.hasNext()
}

func (al *availableList) next() *pb.RequestAck {
	return al.appendList.next().(*pb.RequestAck)
}

func (al *availableList) garbageCollect(states map[uint64]*pb.NetworkState_Client) {
	al.appendList.garbageCollect(func(value interface{}) bool {
		ack := value.(*pb.RequestAck)
		state, ok := states[ack.ClientId]
		assertTrue(ok, "any available client req must have client in config")
		return isCommitted(ack.ReqNo, state)
	})
}
