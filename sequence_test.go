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

var _ = Describe("sequence", func() {
	var (
		s *sequence
	)

	BeforeEach(func() {
		s = &sequence{
			myConfig: &Config{
				ID: 1,
			},
			networkConfig: &pb.NetworkConfig{
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
				[]*request{
					{
						digest: []byte("msg1-digest"),
						requestData: &pb.RequestData{
							ClientId: []byte("client-id"),
							ReqNo:    7,
							Data:     []byte("msg1"),
						},
					},
					{
						digest: []byte("msg2-digest"),
						requestData: &pb.RequestData{
							ClientId: []byte("client-id"),
							ReqNo:    8,
							Data:     []byte("msg2"),
						},
					},
				},
			)

			Expect(actions).To(Equal(&Actions{
				Process: []*Batch{
					{
						Source: 0,
						SeqNo:  5,
						Epoch:  4,
						Requests: []*PreprocessResult{
							{
								Digest: []byte("msg1-digest"),
								RequestData: &pb.RequestData{
									ClientId: []byte("client-id"),
									ReqNo:    7,
									Data:     []byte("msg1"),
								},
							},
							{
								Digest: []byte("msg2-digest"),
								RequestData: &pb.RequestData{
									ClientId: []byte("client-id"),
									ReqNo:    8,
									Data:     []byte("msg2"),
								},
							},
						},
					},
				},
			}))

			Expect(s.state).To(Equal(Allocated))
			Expect(s.batch).To(Equal(
				[]*request{
					{
						state:  Allocated,
						seqNo:  5,
						digest: []byte("msg1-digest"),
						requestData: &pb.RequestData{
							ClientId: []byte("client-id"),
							ReqNo:    7,
							Data:     []byte("msg1"),
						},
					},
					{
						state:  Allocated,
						seqNo:  5,
						digest: []byte("msg2-digest"),
						requestData: &pb.RequestData{
							ClientId: []byte("client-id"),
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
						[]*request{
							{
								digest: []byte("msg1-digest"),
								requestData: &pb.RequestData{
									ClientId: []byte("client-id"),
									ReqNo:    7,
									Data:     []byte("msg1"),
								},
							},
							{
								digest: []byte("msg2-digest"),
								requestData: &pb.RequestData{
									ClientId: []byte("client-id"),
									ReqNo:    8,
									Data:     []byte("msg2"),
								},
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
			s.batch = []*request{
				{
					state:  Allocated,
					seqNo:  5,
					digest: []byte("msg1-digest"),
					requestData: &pb.RequestData{
						ClientId: []byte("client-id"),
						ReqNo:    7,
						Data:     []byte("msg1"),
					},
				},
				{
					state:  Allocated,
					seqNo:  5,
					digest: []byte("msg2-digest"),
					requestData: &pb.RequestData{
						ClientId: []byte("client-id"),
						ReqNo:    8,
						Data:     []byte("msg2"),
					},
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
				QEntries: []*pb.QEntry{
					{
						SeqNo:  5,
						Epoch:  4,
						Digest: []byte("digest"),
						Requests: []*pb.Request{
							{
								ClientId: []byte("client-id"),
								ReqNo:    7,
								Digest:   []byte("msg1-digest"),
							},
							{
								ClientId: []byte("client-id"),
								ReqNo:    8,
								Digest:   []byte("msg2-digest"),
							},
						},
					},
				},
			}))
			Expect(s.digest).To(Equal([]byte("digest")))
			Expect(s.state).To(Equal(Preprepared))
			Expect(s.qEntry).To(Equal(&pb.QEntry{
				SeqNo:  5,
				Epoch:  4,
				Digest: []byte("digest"),
				Requests: []*pb.Request{
					{
						ClientId: []byte("client-id"),
						ReqNo:    7,
						Digest:   []byte("msg1-digest"),
					},
					{
						ClientId: []byte("client-id"),
						ReqNo:    8,
						Digest:   []byte("msg2-digest"),
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
			actions := s.applyPrepareMsg(0, []byte("digest"))
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
				PEntries: []*pb.PEntry{
					{
						SeqNo:  5,
						Epoch:  4,
						Digest: []byte("digest"),
					},
				},
			}))
		})
	})
})
