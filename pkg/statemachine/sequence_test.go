/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemachine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/pb/state"
)

var _ = XDescribe("sequence", func() {
	var (
		s *sequence
	)

	BeforeEach(func() {
		s = &sequence{
			myConfig: &state.EventInitialParameters{
				Id: 1,
			},
			networkConfig: &msgs.NetworkState_Config{
				Nodes: []uint64{0, 1, 2, 3},
				F:     1,
			},
			epoch:       4,
			seqNo:       5,
			owner:       0,
			nodeChoices: map[nodeID]*nodeSeqChoice{},
			prepares:    map[string]int{},
			commits:     map[string]int{},
		}
	})

	Describe("allocate", func() {
		It("transitions from Unknown to Allocated", func() {
			actions := s.allocate(
				[]*msgs.RequestAck{
					{
						ClientId: 9,
						ReqNo:    7,
						Digest:   []byte("msg1-digest"),
					},
					{
						ClientId: 9,
						ReqNo:    8,
						Digest:   []byte("msg2-digest"),
					},
				},
				nil,
			)

			Expect(actions).To(Equal((&ActionList{}).Hash(
				[][]byte{
					[]byte("msg1-digest"),
					[]byte("msg2-digest"),
				},

				&state.HashOrigin{
					Type: &state.HashOrigin_Batch_{
						Batch: &state.HashOrigin_Batch{
							Source: 0,
							SeqNo:  5,
							Epoch:  4,
							RequestAcks: []*msgs.RequestAck{
								{
									ClientId: 9,
									ReqNo:    7,
									Digest:   []byte("msg1-digest"),
								},
								{
									ClientId: 9,
									ReqNo:    8,
									Digest:   []byte("msg2-digest"),
								},
							},
						},
					},
				},
			),
			))

			Expect(s.state).To(Equal(sequenceAllocated))
			Expect(s.batch).To(Equal(
				[]*clientRequest{
					{
						ack: &msgs.RequestAck{
							ClientId: 9,
							ReqNo:    7,
							Digest:   []byte("msg1-digest"),
						},
					},
					{
						ack: &msgs.RequestAck{
							ClientId: 9,
							ReqNo:    8,
							Digest:   []byte("msg2-digest"),
						},
					},
				},
			))
		})

		When("the current state is not Unknown", func() {
			BeforeEach(func() {
				s.state = sequencePrepared
			})

			It("does not transition and instead panics", func() {
				badTransition := func() {
					s.allocate(
						[]*msgs.RequestAck{
							{
								ClientId: 9,
								ReqNo:    7,
								Digest:   []byte("msg1-digest"),
							},
							{
								ClientId: 9,
								ReqNo:    8,
								Digest:   []byte("msg2-digest"),
							},
						},
						nil,
					)
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(sequencePrepared))
			})
		})
	})

	Describe("applyBatchHashResult", func() {
		BeforeEach(func() {
			s.state = sequenceAllocated
			s.batch = []*msgs.RequestAck{
				{
					ClientId: 9,
					ReqNo:    7,
					Digest:   []byte("msg1-digest"),
				},
				{
					ClientId: 9,
					ReqNo:    8,
					Digest:   []byte("msg2-digest"),
				},
			}
		})

		It("transitions from Allocated to Preprepared", func() {
			actions := s.applyBatchHashResult([]byte("digest"))
			Expect(actions).To(Equal((&ActionList{}).Persist(
				0,
				&msgs.Persistent{
					Type: &msgs.Persistent_QEntry{
						QEntry: &msgs.QEntry{
							SeqNo:  5,
							Digest: []byte("digest"),
							Requests: []*msgs.RequestAck{
								{
									ClientId: 9,
									ReqNo:    7,
									Digest:   []byte("msg1-digest"),
								},
								{
									ClientId: 9,
									ReqNo:    8,
									Digest:   []byte("msg2-digest"),
								},
							},
						},
					},
				},
			).Send(
				[]uint64{0, 1, 2, 3},
				&msgs.Msg{
					Type: &msgs.Msg_Prepare{
						Prepare: &msgs.Prepare{
							SeqNo:  5,
							Epoch:  4,
							Digest: []byte("digest"),
						},
					},
				},
			)))
			Expect(s.digest).To(Equal([]byte("digest")))
			Expect(s.state).To(Equal(sequencePreprepared))
			Expect(s.qEntry).To(Equal(&msgs.QEntry{
				SeqNo:  5,
				Digest: []byte("digest"),
				Requests: []*msgs.RequestAck{
					{
						ClientId: 9,
						ReqNo:    7,
						Digest:   []byte("msg1-digest"),
					},
					{
						ClientId: 9,
						ReqNo:    8,
						Digest:   []byte("msg2-digest"),
					},
				},
			}))

		})

		When("the state is not Allocated", func() {
			BeforeEach(func() {
				s.state = sequencePrepared
			})

			It("does not transition the state and panics", func() {
				badTransition := func() {
					s.applyBatchHashResult([]byte("digest"))
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(sequencePrepared))
			})
		})

	})

	Describe("applyPrepareMsg", func() {
		BeforeEach(func() {
			s.state = sequencePreprepared
			s.digest = []byte("digest")
			// s.prepares["digest"] = map[nodeID]struct{}{
			// 1: {},
			// 2: {},
			// }
		})

		It("transitions from Preprepared to Prepared", func() {
			s.applyPrepareMsg(0, []byte("digest"))
			actions := s.advanceState()
			Expect(actions).To(Equal((&ActionList{}).Persist(
				0,
				&msgs.Persistent{
					Type: &msgs.Persistent_PEntry{
						PEntry: &msgs.PEntry{
							SeqNo:  5,
							Digest: []byte("digest"),
						},
					},
				},
			).Send(
				[]uint64{0, 1, 2, 3},
				&msgs.Msg{
					Type: &msgs.Msg_Commit{
						Commit: &msgs.Commit{
							SeqNo:  5,
							Epoch:  4,
							Digest: []byte("digest"),
						},
					},
				},
			)))
		})
	})
})
