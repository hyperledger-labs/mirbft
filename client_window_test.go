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

func Uint64ToPtr(value uint64) *uint64 {
	return &value
}

var _ = Describe("clientWindows", func() {

})

var _ = Describe("clientWindow", func() {
	var (
		cw            *clientWindow
		networkConfig *pb.NetworkState_Config
		myConfig      *Config
		lwm, hwm      uint64
	)

	BeforeEach(func() {
		lwm = 10
		hwm = 20
		networkConfig = &pb.NetworkState_Config{
			Nodes: []uint64{0},
		}
		myConfig = &Config{} // TODO, populate
	})

	JustBeforeEach(func() {
		cw = newClientWindow(0, lwm, hwm, networkConfig, myConfig)
	})

	It("stores requests", func() {
		cw.allocate(&pb.Request{ReqNo: 10}, []byte("digest"))
		Expect(cw.request(10).digests).NotTo(BeEmpty())
		Expect(cw.request(20).digests).To(BeEmpty())
		status := cw.status()
		Expect(status.LowWatermark).To(Equal(lwm))
		Expect(status.HighWatermark).To(Equal(hwm))
		Expect(status.Allocated[0]).To(Equal(uint64(1)))
	})

	Context("garbage collection", func() {
		When("older requests are not committed", func() {
			It("purges comitted requests while preserving uncommitted ones", func() {
				cw.allocate(&pb.Request{ReqNo: 10}, []byte("digest"))
				cw.reqNoList.Front().Value.(*clientReqNo).committed = Uint64ToPtr(12)
				cw.nextReadyMark = 11
				cw.allocate(&pb.Request{ReqNo: 11}, []byte("digest"))
				cw.allocate(&pb.Request{ReqNo: 12}, []byte("digest"))
				cw.allocate(&pb.Request{ReqNo: 13}, []byte("digest"))
				cw.allocate(&pb.Request{ReqNo: 14}, []byte("digest"))
				Expect(cw.lowWatermark).To(Equal(lwm))
				Expect(cw.highWatermark).To(Equal(hwm))
				Expect(cw.request(10)).NotTo(BeNil())

				cw.garbageCollect(13)
				Expect(cw.lowWatermark).To(Equal(lwm + 1))
				Expect(cw.highWatermark).To(Equal(hwm + 1))
				Expect(func() { cw.request(10) }).To(Panic())
				Expect(cw.request(11)).NotTo(BeNil())
			})
		})

		When("all requests are committed", func() {
			It("purges all non-nil requests", func() {
				cw.allocate(&pb.Request{ReqNo: 10}, []byte("digest"))
				cw.reqNoMap[10].Value.(*clientReqNo).committed = Uint64ToPtr(10)
				cw.allocate(&pb.Request{ReqNo: 11}, []byte("digest"))
				cw.reqNoMap[11].Value.(*clientReqNo).committed = Uint64ToPtr(11)
				cw.allocate(&pb.Request{ReqNo: 12}, []byte("digest"))
				cw.reqNoMap[12].Value.(*clientReqNo).committed = Uint64ToPtr(12)
				cw.allocate(&pb.Request{ReqNo: 13}, []byte("digest"))
				cw.reqNoMap[13].Value.(*clientReqNo).committed = Uint64ToPtr(13)
				cw.allocate(&pb.Request{ReqNo: 14}, []byte("digest"))
				cw.reqNoMap[14].Value.(*clientReqNo).committed = Uint64ToPtr(14)
				cw.nextReadyMark = 15

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
			cw.allocate(&pb.Request{ReqNo: 10}, []byte("digest"))
			Expect(cw.reqNoMap[10]).NotTo(BeNil())
		})

		When("reqno is out of watermarks", func() {
			It("panics", func() {
				req := &pb.Request{ReqNo: 1}
				Expect(func() { cw.allocate(req, []byte("digest")) }).To(Panic())
				req = &pb.Request{ReqNo: 21}
				Expect(func() { cw.allocate(req, []byte("digest")) }).To(Panic())
			})
		})

	})
})
