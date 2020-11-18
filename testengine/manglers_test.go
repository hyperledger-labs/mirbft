package testengine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	rpb "github.com/IBM/mirbft/eventlog/recorderpb"
	pb "github.com/IBM/mirbft/mirbftpb"
)

var _ = Describe("Manglers", func() {
	Describe("initializeMangling", func() {
		It("binds the fields to the underlying implementations", func() {
			mm := &MsgMangling{}
			initializeMangling(mm)
			Expect(mm.FromNode).NotTo(BeNil())
			Expect(mm.FromNodes).NotTo(BeNil())
			Expect(mm.ToNode).NotTo(BeNil())
			Expect(mm.ToNodes).NotTo(BeNil())
			Expect(mm.AtPercent).NotTo(BeNil())

			nmm := mm.AtPercent(30)
			Expect(nmm).To(Equal(mm))
			Expect(nmm.Filters).To(HaveLen(1))
			Expect(nmm.Filters[0].apply(5, nil)).To(BeTrue())

			nmm = nmm.ToNodes(3, 5)
			Expect(nmm).To(Equal(mm))
			Expect(nmm.Filters).To(HaveLen(2))
			Expect(nmm.Filters[1].apply(0, &rpb.RecordedEvent{
				NodeId: 2,
			})).To(BeFalse())
			Expect(nmm.Filters[1].apply(0, &rpb.RecordedEvent{
				NodeId: 3,
			})).To(BeTrue())
		})
	})

	Describe("MsgTypeMangling", func() {
		It("matches the message type", func() {
			mtm := MangleMsgs().OfTypePreprepare()
			Expect(mtm.Filters).To(HaveLen(2))
			Expect(mtm.Filters[1].apply(0, &rpb.RecordedEvent{
				StateEvent: &pb.StateEvent{
					Type: &pb.StateEvent_Step{
						Step: &pb.StateEvent_InboundMsg{
							Msg: &pb.Msg{
								Type: &pb.Msg_Preprepare{
									Preprepare: &pb.Preprepare{},
								},
							},
						},
					},
				},
			},
			)).To(BeTrue())
		})

		It("does not match the wrong message type", func() {
			mtm := MangleMsgs().OfTypePreprepare()
			Expect(mtm.Filters).To(HaveLen(2))
			Expect(mtm.Filters[1].apply(0, &rpb.RecordedEvent{
				StateEvent: &pb.StateEvent{
					Type: &pb.StateEvent_Step{
						Step: &pb.StateEvent_InboundMsg{
							Msg: &pb.Msg{
								Type: &pb.Msg_Commit{
									Commit: &pb.Commit{},
								},
							},
						},
					},
				},
			},
			)).To(BeFalse())
		})
	})
})
