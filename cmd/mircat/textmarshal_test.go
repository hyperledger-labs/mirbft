/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/hyperledger-labs/mirbft/pkg/pb/msgs"
	"github.com/hyperledger-labs/mirbft/pkg/pb/recording"
	"github.com/hyperledger-labs/mirbft/pkg/pb/state"
)

var _ = Describe("textmarshal", func() {
	var (
		sampleStepEvent = &recording.Event{
			NodeId: 7,
			Time:   9,
			StateEvent: &state.Event{
				Type: &state.Event_Step{
					Step: &state.EventStep{
						Source: 4,
						Msg: &msgs.Msg{
							Type: &msgs.Msg_Prepare{
								Prepare: &msgs.Prepare{
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
