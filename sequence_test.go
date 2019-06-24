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
			epochConfig: &epochConfig{
				myConfig: &Config{
					ID: 1,
				},
				number: 4,
				f:      1,
			},
			entry: &Entry{
				Epoch:    4,
				SeqNo:    5,
				BucketID: 3,
			},
			prepares: map[string]map[NodeID]struct{}{},
			commits:  map[string]map[NodeID]struct{}{},
		}
	})

	Describe("applyPreprepare", func() {
		It("transitions from Unknown to Preprepared", func() {
			actions := s.applyPreprepare(
				[][]byte{
					[]byte("msg1"),
					[]byte("msg2"),
				},
			)

			Expect(actions).To(Equal(&Actions{
				Digest: []*Entry{
					{
						SeqNo:    5,
						Epoch:    4,
						BucketID: 3,
						Batch: [][]byte{
							[]byte("msg1"),
							[]byte("msg2"),
						},
					},
				},
			}))

			Expect(s.state).To(Equal(Preprepared))
			Expect(s.entry.Batch).To(Equal(
				[][]byte{
					[]byte("msg1"),
					[]byte("msg2"),
				},
			))
		})

		Context("when the current state is not Unknown", func() {
			BeforeEach(func() {
				s.state = Validated
			})

			It("does not transition and instead panics", func() {
				badTransition := func() {
					s.applyPreprepare(
						[][]byte{
							[]byte("msg1"),
							[]byte("msg2"),
						},
					)
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(Validated))
			})
		})
	})

	Describe("applyValidateResult", func() {
		BeforeEach(func() {
			s.state = Digested
			s.digest = []byte("digest")
		})

		It("transitions from Preprepared to Validated", func() {
			actions := s.applyValidateResult(true)
			Expect(actions).To(Equal(&Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Prepare{
							Prepare: &pb.Prepare{
								SeqNo:  5,
								Epoch:  4,
								Bucket: 3,
								Digest: []byte("digest"),
							},
						},
					},
				},
			}))
			Expect(s.digest).To(Equal([]byte("digest")))
			Expect(s.state).To(Equal(Validated))
		})

		Context("when the state is not Preprepared", func() {
			BeforeEach(func() {
				s.state = Prepared
			})

			It("does not transition the state and panics", func() {
				badTransition := func() {
					s.applyValidateResult(true)
				}
				Expect(badTransition).To(Panic())
				Expect(s.state).To(Equal(Prepared))
			})
		})

		Context("when the validation is not successful", func() {
			It("transitions the state to InvalidBatch", func() {
				actions := s.applyValidateResult(false)
				Expect(actions).To(Equal(&Actions{}))
				Expect(s.state).To(Equal(InvalidBatch))
			})
		})
	})

	Describe("applyPrepare", func() {
		BeforeEach(func() {
			s.state = Validated
			s.digest = []byte("digest")
			s.prepares["digest"] = map[NodeID]struct{}{
				1: {},
				2: {},
			}
		})

		It("transitions from Validated to Prepared", func() {
			actions := s.applyPrepare(0, []byte("digest"))
			Expect(actions).To(Equal(&Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								SeqNo:  5,
								Epoch:  4,
								Bucket: 3,
								Digest: []byte("digest"),
							},
						},
					},
				},
			}))
		})
	})
})
