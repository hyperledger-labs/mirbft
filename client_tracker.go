/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"container/list"
	"fmt"
	"sort"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/status"
)

// The client_tracker code is responsible for tracking the requests made by clients,
// for ack-ing those requests to the network, and replicating requests from other replicas.
// There are two general paths through which a request may arrive.
//
//   1. The replica has the request given to it directly by a client, or from some other
//      verifiable source (like a signed gossip message).  The replica verifies this request
//      to be correct using whatever application logic is required, and, then injects it
//      into the state machine via the 'Propose' API.
//
//   2. The replica receives a weak quorum of request-ack messages, indicating that at least
//      one correct replica has a request from the client and has validated it according to the
//      application logic as required.  This request may be replicated in a number of ways.
//
//        a) The replica may receive an unsolicited 'forward' of the request from a primary
//           who is including this request in a batch but has not received this replica's ack.
//
//        b) The replica may, after sufficient ticks elapse, send a fetch request for this
//           request from a subset of the weak quorum.  This is to handle the case where a
//           non-byzantine client crashes after disseminating the request to f+1 correct nodes
//           but before disseminating it to 2f+1 correct nodes.
//
//        c) During an epoch change, a request may be selected, which requires f+1 nodes to
//           agree on the digest, which implies at least one correct node has validate the request.
//           In this case, we apply the epoch-change messages as request-acks because we may
//           not have a weak quorum of request-ack messages yet.
//
// Because we do not have reliable message transmission, it is possible that some replicas may 'miss'
// a request-ack and a client's progress could stall.  To counter this, we rebroadcast the request-ack
// for a client request, with a backoff, such that eventually, all replicas receive all acks (or the
// request number for that client first commits).
//
// It is possible that the client is byzantine (or, behaves in a byzantine manner due to a crash),
// and ends up submitting two valid requests for the same request number.  First, to some set of f+1
// replicas, then to another disjoint set of f+1.  With cooperation of byzantine replicas, it becomes
// clear that even more duplicated requests could be injected into the system.  To counter this
// resource drain, once a replica observes two distinct valid requests for a request number, it
// begins to advocate via its request-ack for a null request (consuming the request number, but
// committing no data).  A byzantine client may of course stall its own progress by carefully selecting
// quorums which prevent the null request from being generated, but, this has no impact on other
// clients, so the null request recourse is only assistance for clients which are accidentally
// byzantine.
//
// When a client connects to a replica, it should solicit the last request number that this replica
// has stored and acknowledged for this client as well as the currently committed low watermark
// for this client.  Based on this information, the client should be able to compute a request range
// which is uncommitted, and it may either:
//
//   1. If the client persisted its request log, it may simply resubmit the same persisted
//      requests, and committing should resume across the network with no loss of data, or holes.
//
//   2. If the client did not persist its requests to a log, it may solicit the uncommitted requests
//      from the replicas, and verify them itself.  This could be checking a signature, a hash-mac
//      or other signal to indicate that the request is valid.  Then, it can resubmit the uncommitted
//      requests just as in (1).
//
//   3. If the client is willing to tolerate request loss, it may simply submit a null request (a
//      a request with no data) for each uncommitted request number for which some replica claims it
//      has received a request.  The replicas will preferentially acknowledge the null request and the
//      client may then begin submitting new requests from a new common low watermark.  Note, that
//      because the client cannot validate whether a replica is byzantinely claiming it received
//      a request that it did not commit, in the worst case, the client will be forced to fill its
//      entire set of watermarks with null requests.  Fortunately, null requests are very lightweight
//      and are of little consequence, so this is an uninteresting attack vector.
//
// Both the normal epoch operation and epoch change code have a dependency on waiting for requests to
// become available (which means both correct, and persisted).  In the normal operation case, a
// replica receives a preprepare, and validates that all of the requests contained in the preprepare
// are present.  If they are not, it waits to preprepare the request until they are available.  In
// the case of a non-byzantine primary, at least f+1 correct replicas have already acked this request,
// so the request will eventually be known as correct, and, the primary will forward the request
// because we have not acked it.  Any preprepare/prepare or commit message also indicates that a
// replica acks this request and we can update the set of agreements accordingly.
//
// In the epoch change case, a batch which contains requests we do not have may be selected for a
// sequence in accordance with the normal view-change rules.  In this case, we must fetch the request
// before continuing with the epoch change, but since f+1 replicas must have included this request
// in their q-set, we know it to be correct, and therefore, we may update our agreements and fetch
// accordingly.
//
// In order to prevent spamming the network and to allow for message dropping, we flag a request
// as being fetched, and, after some expiration attempt to fetch once more.  Any concurrent instructions
// to fetch the request are ignored so long as the request is currently being fetched.
//
// To additionally enforce that a byzantine replica does not spam many different digests for the same
// request, we only store the first non-null ack a replica sends for a given client and request number.
// However, if a request is known to be correct, because of a weak quorum during the standard three
// phase commit, or during epoch change, then we do record the replicas agreement with this correct
// digest.  This bounds the maximum possible number of digests a replica stores for a given sequence
// at n (though requires a byzantine client colluding with f byzantine replicas, and the client
// will be detected as byzantine).
//
// The client tracker stores
// availableList -- all requests we have stored (and are therefore correct) -- a list of clientReqNos
// since multiple requests for the same reqno could become available.
// strongList -- all requests which have a continuous strong quorum cert for each reqNo after the last commited reqNo, and can therefore safely be proposed -- a list of clientReqNos (allowing for the null request to become strong and supersede another strong request)
// weakList -- all requests which are correct, but we have not replicated locally -- a list of
// replicatingMap -- all requests which are currently being fetched

var zeroPtr *uint64

func init() {
	var zero uint64
	zeroPtr = &zero
}

// appendList is a useful data structure for us because it allows appending
// of new elements, with stable iteration across garabage collection.  By
// stable iteration, we mean that even if an element is removed from the list
// any active iterators will still encounter that element, and eventually
// reach the tail of the list.  Contrast this with the golang stdlib doubly
// linked list which, when elements are removed, can no longer be used to
// iterate over the list.
type appendList struct {
	head *appendListEntry
	tail *appendListEntry
}

type appendListEntry struct {
	next  *appendListEntry
	value interface{}
}

type appendListIterator struct {
	lastEntry  *appendListEntry
	appendList *appendList
}

func (ale *appendListEntry) isSkipElement() bool {
	return ale.value == nil
}

func newAppendList() *appendList {
	startElement := &appendListEntry{}
	return &appendList{
		head: startElement,
		tail: startElement,
	}
}

func (al *appendList) pushBack(value interface{}) {
	newEntry := &appendListEntry{
		value: value,
	}

	al.tail.next = newEntry
	al.tail = newEntry
}

func (al *appendList) iterator() *appendListIterator {
	return &appendListIterator{
		appendList: al,
		lastEntry: &appendListEntry{
			next: al.head,
		},
	}
}

func (ali *appendListIterator) hasNext() bool {
	for {
		currentEntry := ali.lastEntry.next
		if currentEntry.next == nil {
			return false
		}
		if !currentEntry.next.isSkipElement() {
			return true
		}
		ali.lastEntry = currentEntry
	}
}

func (ali *appendListIterator) next() interface{} {
	currentEntry := ali.lastEntry.next
	ali.lastEntry = currentEntry
	return currentEntry.next.value
}

func (ali *appendListIterator) remove() {
	currentEntry := ali.lastEntry.next
	if currentEntry.next == nil {
		ali.appendList.pushBack(nil)
	}

	ali.lastEntry.next = currentEntry.next
}

type readyList struct {
	appendList *appendList
}

func newReadyList() *readyList {
	return &readyList{
		appendList: newAppendList(),
	}
}

type readyIterator struct {
	appendListIterator *appendListIterator
}

func (ri *readyIterator) hasNext() bool {
	return ri.appendListIterator.hasNext()
}

func (ri *readyIterator) next() *clientReqNo {
	return ri.appendListIterator.next().(*clientReqNo)
}

func (rl *readyList) pushBack(crn *clientReqNo) {
	rl.appendList.pushBack(crn)
}

func (rl *readyList) iterator() *readyIterator {
	return &readyIterator{
		appendListIterator: rl.appendList.iterator(),
	}
}

func (rl *readyList) garbageCollect(seqNo uint64) {
	i := rl.iterator()
	for i.hasNext() {
		crn := i.next()
		c := crn.committed
		if c == nil || *c > seqNo {
			continue
		}

		i.appendListIterator.remove()
	}
}

type availableList struct {
	appendList *appendList
}

type availableIterator struct {
	appendListIterator *appendListIterator
}

func newAvailableList() *availableList {
	return &availableList{
		appendList: newAppendList(),
	}
}

func (al *availableList) pushBack(ack *clientRequest) {
	al.appendList.pushBack(ack)
}

func (al *availableList) iterator() *availableIterator {
	return &availableIterator{
		appendListIterator: al.appendList.iterator(),
	}
}

func (al *availableIterator) hasNext() bool {
	return al.appendListIterator.hasNext()
}

func (al *availableIterator) next() *clientRequest {
	return al.appendListIterator.next().(*clientRequest)
}

type clientTracker struct {
	clients       map[uint64]*client
	clientIDs     []uint64
	networkConfig *pb.NetworkState_Config
	msgBuffers    map[nodeID]*msgBuffer
	logger        Logger
	readyList     *readyList
	availableList *availableList // A list of requests which have f+1 ACKs and the requestData
	myConfig      *pb.StateEvent_InitialParameters
}

func newClientWindows(persisted *persisted, myConfig *pb.StateEvent_InitialParameters, logger Logger) *clientTracker {
	ct := &clientTracker{
		logger:        logger,
		clients:       map[uint64]*client{},
		availableList: newAvailableList(),
		readyList:     newReadyList(),
		myConfig:      myConfig,
		msgBuffers:    map[nodeID]*msgBuffer{},
	}

	batches := map[string][]*pb.RequestAck{}

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persistent_CEntry:
			// Note, we're guaranteed to see this first
			if ct.networkConfig == nil {
				ct.networkConfig = d.CEntry.NetworkState.Config

				for _, clientState := range d.CEntry.NetworkState.Clients {
					client := newClient(clientState, d.CEntry.NetworkState.Config, logger)
					ct.insert(clientState.Id, client)
				}
			}

			// TODO, handle new clients added at checkpoints
		case *pb.Persistent_QEntry:
			batches[string(d.QEntry.Digest)] = d.QEntry.Requests
		case *pb.Persistent_PEntry:
			batch, ok := batches[string(d.PEntry.Digest)]
			if !ok {
				panic("dev sanity test")
			}

			for _, request := range batch {
				ct.clients[request.ClientId].ack(nodeID(myConfig.Id), request)
			}
		}
	}

	for _, clientID := range ct.clientIDs {
		ct.advanceReady(ct.clients[clientID])
	}

	for _, id := range ct.networkConfig.Nodes {
		ct.msgBuffers[nodeID(id)] = newMsgBuffer(myConfig, logger)
	}

	return ct
}

func (ct *clientTracker) filter(msg *pb.Msg) applyable {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, prevent ack spam of multiple msg digests from the same node
		ack := innerMsg.RequestAck
		client, ok := ct.client(ack.ClientId)
		if !ok {
			return future
		}
		switch {
		case client.lowWatermark > ack.ReqNo:
			return past
		case client.lowWatermark+client.width < ack.ReqNo:
			return future
		default:
			return current
		}
	case *pb.Msg_FetchRequest:
		return current // TODO decide if this is actually current
	case *pb.Msg_ForwardRequest:
		requestAck := innerMsg.ForwardRequest.RequestAck
		client, ok := ct.client(requestAck.ClientId)
		if !ok {
			return future
		}
		switch {
		case client.lowWatermark > requestAck.ReqNo:
			return past
		case client.lowWatermark+client.width < requestAck.ReqNo:
			return future
		default:
			return current
		}
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

func (ct *clientTracker) step(source nodeID, msg *pb.Msg) *Actions {
	switch ct.filter(msg) {
	case past:
		// discard
		return &Actions{}
	case future:
		ct.msgBuffers[source].store(msg)
		return &Actions{}
	}

	// current
	return ct.applyMsg(source, msg)
}

func (ct *clientTracker) applyMsg(source nodeID, msg *pb.Msg) *Actions {
	switch innerMsg := msg.Type.(type) {
	case *pb.Msg_RequestAck:
		// TODO, make sure nodeMsgs ignores this if client is not defined
		ct.ack(source, innerMsg.RequestAck)
		return &Actions{}
	case *pb.Msg_FetchRequest:
		msg := innerMsg.FetchRequest
		return ct.replyFetchRequest(source, msg.ClientId, msg.ReqNo, msg.Digest)
	case *pb.Msg_ForwardRequest:
		if source == nodeID(ct.myConfig.Id) {
			// We've already pre-processed this
			// TODO, once we implement unicasting to only those
			// who don't know this should go away.
			return &Actions{}
		}
		return ct.applyForwardRequest(source, innerMsg.ForwardRequest)
	default:
		panic(fmt.Sprintf("unexpected bad client window message type %T, this indicates a bug", msg.Type))
	}
}

func (ct *clientTracker) applyRequestDigest(ack *pb.RequestAck, data []byte) *Actions {
	client, ok := ct.clients[ack.ClientId]
	if !ok {
		// Unusual, client must have been removed since we processed the request
		return &Actions{}
	}

	if !client.inWatermarks(ack.ReqNo) {
		// We've already committed this reqno
		return &Actions{}
	}

	return client.reqNo(ack.ReqNo).applyRequestDigest(ack, data)
}

// Note, this seems like it should take a seqNo as a parameter, but since the garbage
// collection is controlled by the state machine, and it will always invoke this method
// before any seqNos are committed beyond the checkpoint, we are safe.
func (ct *clientTracker) clientConfigs() []*pb.NetworkState_Client {
	clients := make([]*pb.NetworkState_Client, len(ct.clientIDs))
	for i, clientID := range ct.clientIDs {
		cw, ok := ct.clients[clientID]
		if !ok {
			panic("dev sanity test")
		}

		var firstUncommitted, lastCommitted *uint64

		for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
			crn := el.Value.(*clientReqNo)
			if crn.committed != nil {
				lastCommitted = &crn.reqNo
				continue
			}
			if firstUncommitted == nil {
				firstUncommitted = &crn.reqNo
			}
		}

		if firstUncommitted == nil {
			if *lastCommitted != cw.lowWatermark+cw.width {
				panic("dev sanity test, if no client reqs are uncommitted, then all though the high watermark should be committed")
			}

			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.width),
				LowWatermark: *lastCommitted + 1,
			}
			continue
		}

		if lastCommitted == nil {
			clients[i] = &pb.NetworkState_Client{
				Id:           clientID,
				Width:        uint32(cw.width),
				LowWatermark: *firstUncommitted,
			}
			continue
		}

		mask := bitmask(make([]byte, int(*lastCommitted-*firstUncommitted)/8+1))
		for i := 0; i < int(*lastCommitted-*firstUncommitted); i++ {
			reqNo := *firstUncommitted + uint64(i)
			if cw.reqNoMap[reqNo].Value.(*clientReqNo).committed == nil {
				continue
			}

			if i == 0 {
				panic("dev sanity test, if this is the first uncommitted, how is it committed")
			}

			mask.setBit(i)

		}

		clients[i] = &pb.NetworkState_Client{
			Id:            clientID,
			Width:         uint32(cw.width),
			LowWatermark:  *firstUncommitted,
			CommittedMask: mask,
		}

	}

	return clients
}

func (ct *clientTracker) replyFetchRequest(source nodeID, clientID, reqNo uint64, digest []byte) *Actions {
	cw, ok := ct.client(clientID)
	if !ok {
		return &Actions{}
	}

	if !cw.inWatermarks(reqNo) {
		return &Actions{}
	}

	creq := cw.reqNo(reqNo)
	data, ok := creq.requests[string(digest)]
	if !ok {
		return &Actions{}
	}

	if _, ok := data.agreements[nodeID(ct.myConfig.Id)]; !ok {
		return &Actions{}
	}

	return (&Actions{}).forwardRequest(
		[]uint64{uint64(source)},
		&pb.RequestAck{
			ClientId: clientID,
			ReqNo:    reqNo,
			Digest:   digest,
		},
	)
}

func (ct *clientTracker) applyForwardRequest(source nodeID, msg *pb.ForwardRequest) *Actions {
	cw, ok := ct.client(msg.RequestAck.ClientId)
	if !ok {
		// TODO log oddity
		return &Actions{}
	}

	// TODO, make sure that we only allow one vote per replica for a reqno, or bounded
	cr := cw.reqNo(msg.RequestAck.ReqNo)
	req, ok := cr.requests[string(msg.RequestAck.Digest)]
	if !ok {
		return &Actions{}
	}

	if _, ok := req.agreements[nodeID(ct.myConfig.Id)]; !ok {
		return &Actions{}
	}

	req.agreements[source] = struct{}{}

	return &Actions{
		Hash: []*HashRequest{
			{
				Data: [][]byte{
					uint64ToBytes(msg.RequestAck.ClientId),
					uint64ToBytes(msg.RequestAck.ReqNo),
					msg.RequestData,
				},
				Origin: &pb.HashResult{
					Type: &pb.HashResult_VerifyRequest_{
						VerifyRequest: &pb.HashResult_VerifyRequest{
							Source:      uint64(source),
							RequestAck:  msg.RequestAck,
							RequestData: msg.RequestData,
						},
					},
				},
			},
		},
	}
}

func (ct *clientTracker) ack(source nodeID, ack *pb.RequestAck) *clientRequest {
	cw, ok := ct.clients[ack.ClientId]
	if !ok {
		panic("dev sanity test")
	}

	clientRequest, clientReqNo, newlyCorrectReq := cw.ack(source, ack)

	if newlyCorrectReq {
		ct.availableList.pushBack(clientRequest)
	}

	ct.checkReady(cw, clientReqNo)

	return clientRequest
}

func (ct *clientTracker) checkReady(client *client, ocrn *clientReqNo) {
	if ocrn.reqNo != client.nextReadyMark {
		return
	}

	if len(ocrn.strongRequests) == 0 {
		return
	}

	for digest := range ocrn.strongRequests {
		if _, ok := ocrn.myRequests[digest]; ok {
			ct.advanceReady(client)
			return
		}
	}

}

func (ct *clientTracker) advanceReady(client *client) {
	for i := client.nextReadyMark; i <= client.lowWatermark+client.width; i++ {
		if i != client.nextReadyMark {
			// last time through the loop, we must not have updated the ready mark
			return
		}

		crne, ok := client.reqNoMap[i]
		if !ok {
			panic(fmt.Sprintf("dev sanity test: no mapping from reqNo %d", i))
		}

		crn := crne.Value.(*clientReqNo)

		for digest := range crn.strongRequests {
			if _, ok := crn.myRequests[digest]; !ok {
				continue
			}

			ct.readyList.pushBack(crn)
			client.nextReadyMark = i + 1

			break
		}
	}
}

func (ct *clientTracker) garbageCollect(seqNo uint64) {
	for _, id := range ct.clientIDs {
		ct.clients[id].garbageCollect(seqNo)
	}

	// TODO, gc the correctList

	ct.readyList.garbageCollect(seqNo)

	for _, id := range ct.networkConfig.Nodes {
		msgBuffer := ct.msgBuffers[nodeID(id)]
		for {
			// TODO, really inefficient
			msg := msgBuffer.next(ct.filter)
			if msg == nil {
				break
			}
			ct.applyMsg(nodeID(id), msg)
		}
	}
}

func (ct *clientTracker) client(clientID uint64) (*client, bool) {
	// TODO, we could do lazy initialization here
	cw, ok := ct.clients[clientID]
	return cw, ok
}

func (ct *clientTracker) insert(clientID uint64, cw *client) {
	ct.clients[clientID] = cw
	ct.clientIDs = append(ct.clientIDs, clientID)
	sort.Slice(ct.clientIDs, func(i, j int) bool {
		return ct.clientIDs[i] < ct.clientIDs[j]
	})
}

// clientReqNo accumulates acks for this request number
// and attempts to determine which ack is correct, and if the request
// we have is correct.  A replica only ever acks requests it knows to be correct,
// and has the data for.  This means it was injected locally via the Propose API,
// or, a weak quorum exists and something triggered a fetch of the request such
// as an epoch change, or tick based reconcilliation logic.  Because in both the epoch
// change, or reconcilliation paths at least some correct replica validated the
// request, we know the request is correct.  Once we observe two valid requests
// we know that the client is behaving in a byzantine way, and will allow a null
// request to be substituted.  Further, the replica will not ack any request other
// than the null request once two correct requests are observed.
// Additionally, a client may inject a null request via the propose API when attempting
// to recover from a crash without persistence, which will also cause other acks to cease.
// A correct replica will never ack two different non-null requests.  We therefore
// track which replicas have already acked a non-null request and ignore any further
// non-null acks.
type clientReqNo struct {
	networkConfig  *pb.NetworkState_Config
	clientID       uint64
	reqNo          uint64
	nonNullVoters  map[nodeID]struct{}
	requests       map[string]*clientRequest // all requests, correct or not we've observed
	weakRequests   map[string]*clientRequest // all correct requests we have observed
	strongRequests map[string]*clientRequest // strongly correct requests (at most 1 null, 1 non-null)
	myRequests     map[string]*clientRequest // requests we have persisted
	committed      *uint64
}

func (crn *clientReqNo) clientReq(ack *pb.RequestAck) *clientRequest {
	var digestKey string
	if len(ack.Digest) == 0 {
		digestKey = ""
	} else {
		digestKey = string(ack.Digest)
	}

	clientReq, ok := crn.requests[digestKey]
	if !ok {
		clientReq = &clientRequest{
			ack:        ack,
			agreements: map[nodeID]struct{}{},
		}
		crn.requests[digestKey] = clientReq
	}

	return clientReq
}

func (crn *clientReqNo) applyRequestDigest(ack *pb.RequestAck, data []byte) *Actions {
	_, ok := crn.myRequests[string(ack.Digest)]
	if ok {
		// We have already persisted this request, likely
		// a race between a forward and a local proposal, do nothing
		return &Actions{}
	}

	clientReq := crn.clientReq(ack)

	crn.myRequests[string(ack.Digest)] = clientReq

	actions := (&Actions{}).storeRequest(
		&pb.ForwardRequest{
			RequestAck:  ack,
			RequestData: data,
		},
	)

	if len(crn.myRequests) == 1 {
		return actions.send(
			crn.networkConfig.Nodes,
			&pb.Msg{
				Type: &pb.Msg_RequestAck{
					RequestAck: ack,
				},
			},
		)
	}

	// More than one request persisted
	if _, ok := crn.myRequests[""]; !ok {
		// already persisted and acked null request
		return actions
	}

	nullAck := &pb.RequestAck{
		ClientId: crn.clientID,
		ReqNo:    crn.reqNo,
	}

	nullReq := crn.clientReq(nullAck)
	crn.myRequests[""] = nullReq

	return actions.send(
		crn.networkConfig.Nodes,
		&pb.Msg{
			Type: &pb.Msg_RequestAck{
				RequestAck: nullAck,
			},
		},
	).storeRequest(
		&pb.ForwardRequest{
			RequestAck: nullAck,
		},
	)
}

func (crn *clientReqNo) applyRequestAck(source nodeID, ack *pb.RequestAck) *Actions {
	if len(ack.Digest) != 0 {
		_, ok := crn.nonNullVoters[source]
		if !ok {
			return &Actions{}
		}

		crn.nonNullVoters[source] = struct{}{}
	}

	clientReq := crn.clientReq(ack)
	clientReq.agreements[source] = struct{}{}

	if len(clientReq.agreements) < someCorrectQuorum(crn.networkConfig) {
		return &Actions{}
	}

	crn.weakRequests[string(ack.Digest)] = clientReq

	if len(clientReq.agreements) < intersectionQuorum(crn.networkConfig) {
		return &Actions{}
	}

	crn.strongRequests[string(ack.Digest)] = clientReq

	return &Actions{}
}

type clientRequest struct {
	ack        *pb.RequestAck
	agreements map[nodeID]struct{}
	garbage    bool // set when this, or another clientRequest for the same clientID/reqNo commits
}

type client struct {
	clientID      uint64
	nextReadyMark uint64
	lowWatermark  uint64
	width         uint64
	reqNoList     *list.List
	reqNoMap      map[uint64]*list.Element
	clientWaiter  *clientWaiter // Used to throttle clients
	logger        Logger
	networkConfig *pb.NetworkState_Config
}

type clientWaiter struct {
	lowWatermark  uint64
	highWatermark uint64
	expired       chan struct{}
}

func newClient(clientState *pb.NetworkState_Client, networkConfig *pb.NetworkState_Config, logger Logger) *client {
	width := uint64(clientState.Width)
	lowWatermark := clientState.LowWatermark
	highWatermark := clientState.LowWatermark + width

	cw := &client{
		clientID:      clientState.Id,
		logger:        logger,
		networkConfig: networkConfig,
		lowWatermark:  lowWatermark,
		nextReadyMark: lowWatermark,
		width:         width,
		reqNoList:     list.New(),
		reqNoMap:      map[uint64]*list.Element{},
		clientWaiter: &clientWaiter{
			lowWatermark:  lowWatermark,
			highWatermark: highWatermark,
			expired:       make(chan struct{}),
		},
	}

	bm := bitmask(clientState.CommittedMask)
	for i := 0; i <= int(clientState.Width); i++ {
		var committed *uint64
		if bm.isBitSet(i) {
			committed = zeroPtr
		}

		reqNo := uint64(i) + clientState.LowWatermark

		el := cw.reqNoList.PushBack(&clientReqNo{
			networkConfig:  networkConfig,
			clientID:       cw.clientID,
			reqNo:          reqNo,
			requests:       map[string]*clientRequest{},
			weakRequests:   map[string]*clientRequest{},
			strongRequests: map[string]*clientRequest{},
			myRequests:     map[string]*clientRequest{},
			nonNullVoters:  map[nodeID]struct{}{},
			committed:      committed,
		})
		cw.reqNoMap[reqNo] = el
	}

	return cw
}

func (cw *client) garbageCollect(maxSeqNo uint64) {
	removed := uint64(0)

	for el := cw.reqNoList.Front(); el != nil; {
		crn := el.Value.(*clientReqNo)
		if crn.committed == nil || *crn.committed > maxSeqNo {
			break
		}

		oel := el
		el = el.Next()

		if crn.reqNo >= cw.nextReadyMark {
			// It's possible that a request we never saw as ready commits
			// because it was correct, so advance the ready mark
			cw.nextReadyMark = crn.reqNo
		}

		for _, cr := range crn.requests {
			cr.garbage = true
		}

		cw.reqNoList.Remove(oel)
		delete(cw.reqNoMap, crn.reqNo)
		removed++
	}

	for i := uint64(1); i <= removed; i++ {
		reqNo := i + cw.lowWatermark + cw.width
		el := cw.reqNoList.PushBack(&clientReqNo{
			clientID:       cw.clientID,
			networkConfig:  cw.networkConfig,
			reqNo:          reqNo,
			requests:       map[string]*clientRequest{},
			weakRequests:   map[string]*clientRequest{},
			strongRequests: map[string]*clientRequest{},
			myRequests:     map[string]*clientRequest{},
			nonNullVoters:  map[nodeID]struct{}{},
		})
		cw.reqNoMap[reqNo] = el
	}

	cw.lowWatermark += removed

	close(cw.clientWaiter.expired)
	cw.clientWaiter = &clientWaiter{
		lowWatermark:  cw.lowWatermark,
		highWatermark: cw.lowWatermark + cw.width,
		expired:       make(chan struct{}),
	}
}

func (cw *client) ack(source nodeID, ack *pb.RequestAck) (*clientRequest, *clientReqNo, bool) {
	crne, ok := cw.reqNoMap[ack.ReqNo]
	if !ok {
		panic("dev sanity check")
	}

	crn := crne.Value.(*clientReqNo)

	cr := crn.clientReq(ack)
	cr.agreements[source] = struct{}{}

	newlyCorrectReq := false
	if len(cr.agreements) == someCorrectQuorum(cw.networkConfig) {
		newlyCorrectReq = true
		crn.weakRequests[string(ack.Digest)] = cr
	}

	if len(cr.agreements) == intersectionQuorum(cw.networkConfig) {
		crn.strongRequests[string(ack.Digest)] = cr
	}

	return cr, crn, newlyCorrectReq
}

func (cw *client) inWatermarks(reqNo uint64) bool {
	return reqNo <= cw.lowWatermark+cw.width && reqNo >= cw.lowWatermark
}

func (cw *client) reqNo(reqNo uint64) *clientReqNo {
	return cw.reqNoMap[reqNo].Value.(*clientReqNo)
}

func (cw *client) status() *status.ClientTracker {
	allocated := make([]uint64, cw.reqNoList.Len())
	i := 0
	lastNonZero := 0
	for el := cw.reqNoList.Front(); el != nil; el = el.Next() {
		crn := el.Value.(*clientReqNo)
		if crn.committed != nil {
			allocated[i] = 2 // TODO, actually report the seqno it committed to
			lastNonZero = i
		} else if len(crn.requests) > 0 {
			allocated[i] = 1
			lastNonZero = i
		}
		i++
	}

	return &status.ClientTracker{
		LowWatermark:  cw.lowWatermark,
		HighWatermark: cw.lowWatermark + cw.width,
		Allocated:     allocated[:lastNonZero+1],
	}
}
