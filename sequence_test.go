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

var _ = XDescribe("sequence", func() {
	var (
		s *sequence
	)

	BeforeEach(func() {
		s = &sequence{
			myConfig: &Config{
				ID: 1,
			},
			networkConfig: &pb.NetworkState_Config{
				Nodes: []uint64{0, 1, 2, 3},
				F:     1,
			},
			epoch:    4,
			seqNo:    5,
			owner:    0,
			prepares: map[string]map[NodeID]struct{}{},
			commits:  map[string]map[NodeID]struct{}{},
		}
	})

	Describe("allocate", func() {
		It("transitions from Unknown to Allocated", func() {
			actions := s.allocate(
				[]*pb.RequestAck{
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
			)

			Expect(actions).To(Equal(&Actions{
				Hash: []*HashRequest{
					{
						Batch: &Batch{
							Source: 0,
							SeqNo:  5,
							Epoch:  4,
							RequestAcks: []*pb.RequestAck{
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
						Data: [][]byte{
							[]byte("msg1-digest"),
							[]byte("msg2-digest"),
						},
					},
				},
			}))

			Expect(s.state).To(Equal(Allocated))
			Expect(s.batch).To(Equal(
				[]*clientRequest{
					{
						digest: []byte("msg1-digest"),
						data: &pb.Request{
							ClientId: 9,
							ReqNo:    7,
							Data:     []byte("msg1"),
						},
					},
					{
						digest: []byte("msg2-digest"),
						data: &pb.Request{
							ClientId: 9,
							ReqNo:    8,
							Data:     []byte("msg2"),
						},
					},
				},
			))
		})

		When("the current state is not Unknown", func() {
			BeforeEach(func() {
				s.state = Prepared
			})

			It("does not transition and instead panics", func() {
				badTransition := func() {
					s.allocate(
						[]*pb.RequestAck{
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
					)
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(Prepared))
			})
		})
	})

	Describe("applyProcessResult", func() {
		BeforeEach(func() {
			s.state = Allocated
			s.batch = []*pb.RequestAck{
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
			actions := s.applyProcessResult([]byte("digest"))
			Expect(actions).To(Equal(&Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Prepare{
							Prepare: &pb.Prepare{
								SeqNo:  5,
								Epoch:  4,
								Digest: []byte("digest"),
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_QEntry{
							QEntry: &pb.QEntry{
								SeqNo:  5,
								Digest: []byte("digest"),
								Requests: []*pb.ForwardRequest{
									{
										Request: &pb.Request{
											ClientId: 9,
											ReqNo:    7,
											Data:     []byte("msg1"),
										},
										Digest: []byte("msg1-digest"),
									},
									{
										Request: &pb.Request{
											ClientId: 9,
											ReqNo:    8,
											Data:     []byte("msg2"),
										},
										Digest: []byte("msg2-digest"),
									},
								},
							},
						},
					},
				},
			}))
			Expect(s.digest).To(Equal([]byte("digest")))
			Expect(s.state).To(Equal(Preprepared))
			Expect(s.qEntry).To(Equal(&pb.QEntry{
				SeqNo:  5,
				Digest: []byte("digest"),
				Requests: []*pb.ForwardRequest{
					{
						Request: &pb.Request{
							ClientId: 9,
							ReqNo:    7,
							Data:     []byte("msg1"),
						},
						Digest: []byte("msg1-digest"),
					},
					{
						Request: &pb.Request{
							ClientId: 9,
							ReqNo:    8,
							Data:     []byte("msg2"),
						},
						Digest: []byte("msg2-digest"),
					},
				},
			}))

		})

		When("the state is not Allocated", func() {
			BeforeEach(func() {
				s.state = Prepared
			})

			It("does not transition the state and panics", func() {
				badTransition := func() {
					s.applyProcessResult([]byte("digest"))
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(Prepared))
			})
		})

	})

	Describe("applyPrepareMsg", func() {
		BeforeEach(func() {
			s.state = Preprepared
			s.digest = []byte("digest")
			s.prepares["digest"] = map[NodeID]struct{}{
				1: {},
				2: {},
			}
		})

		It("transitions from Preprepared to Prepared", func() {
			s.applyPrepareMsg(0, []byte("digest"))
			actions := s.advanceState()
			Expect(actions).To(Equal(&Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								SeqNo:  5,
								Epoch:  4,
								Digest: []byte("digest"),
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_PEntry{
							PEntry: &pb.PEntry{
								SeqNo:  5,
								Digest: []byte("digest"),
							},
						},
					},
				},
			}))
		})
	})
})
