/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"
	rpb "github.com/IBM/mirbft/recorder/recorderpb"
)

var _ = Describe("textmarshal", func() {
	var (
		sampleStepEvent = &rpb.RecordedEvent{
			NodeId: 7,
			Time:   9,
			StateEvent: &pb.StateEvent{
				Type: &pb.StateEvent_Step{
					Step: &pb.StateEvent_InboundMsg{
						Source: 4,
						Msg: &pb.Msg{
							Type: &pb.Msg_Prepare{
								Prepare: &pb.Prepare{
									SeqNo:  11,
									Digest: []byte{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef},
								},
							},
						},
					},
				},
			},
		}
	)

	It("concisely marshals mirbft messages", func() {
		txt, err := textFormat(sampleStepEvent, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(txt).To(Equal("[node_id=7 time=9 state_event=[step=[source=4 msg=[prepare=[seq_no=11 epoch=0 digest=deadbeef]]]]]"))
	})

	It("verbosely marshals mirbft messages", func() {
		txt, err := textFormat(sampleStepEvent, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(txt).To(Equal("[node_id=7 time=9 state_event=[step=[source=4 msg=[prepare=[seq_no=11 epoch=0 digest=deadbeefdeadbeef]]]]]"))
	})
})
