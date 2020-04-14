/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"
)

var _ = Describe("clientWindows", func() {

})

var _ = Describe("clientWindow", func() {
	var (
		cw       *clientWindow
		lwm, hwm uint64
	)

	BeforeEach(func() {
		lwm = 10
		hwm = 20
	})

	JustBeforeEach(func() {
		cw = newClientWindow(lwm, hwm, nil)
	})

	It("stores requests", func() {
		cw.allocate(&pb.RequestData{ReqNo: 10}, []byte("digest"))
		Expect(cw.requests[0]).NotTo(BeNil())
		Expect(cw.request(10)).NotTo(BeNil())
		status := cw.status()
		Expect(status.LowWatermark).To(Equal(lwm))
		Expect(status.HighWatermark).To(Equal(hwm))
		Expect(status.Allocated[0]).To(Equal(uint64(1)))
	})

	Context("garbage collection", func() {
		When("older requests are not committed", func() {
			It("purges comitted requests while preserving uncommitted ones", func() {
				cw.allocate(&pb.RequestData{ReqNo: 10}, []byte("digest"))
				cw.allocate(&pb.RequestData{ReqNo: 11}, []byte("digest"))
				cw.allocate(&pb.RequestData{ReqNo: 12}, []byte("digest"))
				cw.allocate(&pb.RequestData{ReqNo: 13}, []byte("digest"))
				cw.allocate(&pb.RequestData{ReqNo: 14}, []byte("digest"))
				cw.garbageCollect(13)
				Expect(cw.lowWatermark).To(Equal(lwm))
				Expect(cw.highWatermark).To(Equal(hwm))
				Expect(cw.request(10)).NotTo(BeNil())

				// initialize all req and mark first committed
				cw.requests[0].state = Committed
				cw.requests[0].seqNo = 10

				cw.requests[1].seqNo = 11
				cw.requests[2].seqNo = 12
				cw.requests[3].seqNo = 13
				cw.requests[4].seqNo = 14

				cw.garbageCollect(13)
				Expect(cw.lowWatermark).To(Equal(lwm + 1))
				Expect(cw.highWatermark).To(Equal(hwm + 1))
				Expect(func() { cw.request(10) }).To(Panic())
				Expect(cw.request(11)).NotTo(BeNil())
			})
		})

		When("all requests are committed", func() {
			It("purges all non-nil requests", func() {
				cw.allocate(&pb.RequestData{ReqNo: 10}, []byte("digest"))
				cw.requests[0].state = Committed
				cw.requests[0].seqNo = 10
				cw.allocate(&pb.RequestData{ReqNo: 11}, []byte("digest"))
				cw.requests[1].state = Committed
				cw.requests[1].seqNo = 11
				cw.allocate(&pb.RequestData{ReqNo: 12}, []byte("digest"))
				cw.requests[2].state = Committed
				cw.requests[2].seqNo = 12
				cw.allocate(&pb.RequestData{ReqNo: 13}, []byte("digest"))
				cw.requests[3].state = Committed
				cw.requests[3].seqNo = 13
				cw.allocate(&pb.RequestData{ReqNo: 14}, []byte("digest"))
				cw.requests[4].state = Committed
				cw.requests[4].seqNo = 14

				cw.garbageCollect(13)
				Expect(cw.lowWatermark).To(Equal(lwm + 4))
				Expect(cw.highWatermark).To(Equal(hwm + 4))

				cw.garbageCollect(20)
				Expect(cw.lowWatermark).To(Equal(lwm + 5))
				Expect(cw.highWatermark).To(Equal(hwm + 5))
			})
		})
	})

	Context("allocate", func() {
		It("stores request", func() {
			cw.allocate(&pb.RequestData{ReqNo: 10}, []byte("digest"))
			Expect(cw.requests[0]).NotTo(BeNil())
		})

		When("reqno is out of watermarks", func() {
			It("panics", func() {
				req := &pb.RequestData{ReqNo: 1}
				Expect(func() { cw.allocate(req, []byte("digest")) }).To(Panic())
				req = &pb.RequestData{ReqNo: 21}
				Expect(func() { cw.allocate(req, []byte("digest")) }).To(Panic())
			})
		})

	})
})
