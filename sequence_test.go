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
			myConfig: &pb.StateEvent_InitialParameters{
				Id: 1,
			},
			networkConfig: &pb.NetworkState_Config{
				Nodes: []uint64{0, 1, 2, 3},
				F:     1,
			},
			epoch:    4,
			seqNo:    5,
			owner:    0,
			prepares: map[string]map[nodeID]struct{}{},
			commits:  map[string]map[nodeID]struct{}{},
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
				nil, // TODO, should be non-nil
				nil,
			)

			Expect(actions).To(Equal(&Actions{
				Hash: []*HashRequest{
					{
						Origin: &pb.HashResult{
							Type: &pb.HashResult_Batch_{
								Batch: &pb.HashResult_Batch{
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
							},
						},
						Data: [][]byte{
							[]byte("msg1-digest"),
							[]byte("msg2-digest"),
						},
					},
				},
			}))

			Expect(s.state).To(Equal(sequenceAllocated))
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
				s.state = sequencePrepared
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
						nil, // TODO, should be non-nil
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
			actions := s.applyBatchHashResult([]byte("digest"))
			Expect(actions).To(Equal(&Actions{
				Send: []Send{
					{
						Targets: []uint64{0, 1, 2, 3},
						Msg: &pb.Msg{
							Type: &pb.Msg_Prepare{
								Prepare: &pb.Prepare{
									SeqNo:  5,
									Epoch:  4,
									Digest: []byte("digest"),
								},
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
			Expect(s.state).To(Equal(sequencePreprepared))
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
			s.prepares["digest"] = map[nodeID]struct{}{
				1: {},
				2: {},
			}
		})

		It("transitions from Preprepared to Prepared", func() {
			s.applyPrepareMsg(0, []byte("digest"))
			actions := s.advanceState()
			Expect(actions).To(Equal(&Actions{
				Send: []Send{
					{
						Targets: []uint64{0, 1, 2, 3},
						Msg: &pb.Msg{
							Type: &pb.Msg_Commit{
								Commit: &pb.Commit{
									SeqNo:  5,
									Epoch:  4,
									Digest: []byte("digest"),
								},
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
