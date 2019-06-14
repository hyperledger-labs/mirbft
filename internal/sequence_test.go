/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package internal_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft/consumer"
	"github.com/IBM/mirbft/internal"
	pb "github.com/IBM/mirbft/mirbftpb"
)

var _ = Describe("Sequence", func() {
	var (
		s *internal.Sequence
	)

	BeforeEach(func() {
		s = &internal.Sequence{
			EpochConfig: &internal.EpochConfig{
				MyConfig: &consumer.Config{
					ID: 1,
				},
				Number: 4,
				F:      1,
			},
			Entry: &consumer.Entry{
				Epoch:    4,
				SeqNo:    5,
				BucketID: 3,
			},
			Prepares: map[string]map[internal.NodeID]struct{}{},
			Commits:  map[string]map[internal.NodeID]struct{}{},
		}
	})

	Describe("ApplyPreprepare", func() {
		It("transitions from Unknown to Preprepared", func() {
			actions := s.ApplyPreprepare(
				[][]byte{
					[]byte("msg1"),
					[]byte("msg2"),
				},
			)

			Expect(actions).To(Equal(&consumer.Actions{
				Digest: []*consumer.Entry{
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

			Expect(s.State).To(Equal(internal.Preprepared))
			Expect(s.Entry.Batch).To(Equal(
				[][]byte{
					[]byte("msg1"),
					[]byte("msg2"),
				},
			))
		})

		Context("when the current state is not Unknown", func() {
			BeforeEach(func() {
				s.State = internal.Validated
			})

			It("does not transition and instead panics", func() {
				badTransition := func() {
					s.ApplyPreprepare(
						[][]byte{
							[]byte("msg1"),
							[]byte("msg2"),
						},
					)
				}
				Expect(badTransition).To(Panic())
				Expect(s.State).To(Equal(internal.Validated))
			})
		})
	})

	Describe("ApplyValidateResult", func() {
		BeforeEach(func() {
			s.State = internal.Digested
			s.Digest = []byte("digest")
		})

		It("transitions from Preprepared to Validated", func() {
			actions := s.ApplyValidateResult(true)
			Expect(actions).To(Equal(&consumer.Actions{
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
			Expect(s.Digest).To(Equal([]byte("digest")))
			Expect(s.State).To(Equal(internal.Validated))
		})

		Context("when the state is not Preprepared", func() {
			BeforeEach(func() {
				s.State = internal.Prepared
			})

			It("does not transition the state and panics", func() {
				badTransition := func() {
					s.ApplyValidateResult(true)
				}
				Expect(badTransition).To(Panic())
				Expect(s.State).To(Equal(internal.Prepared))
			})
		})

		Context("when the validation is not successful", func() {
			It("transitions the state to InvalidBatch", func() {
				actions := s.ApplyValidateResult(false)
				Expect(actions).To(Equal(&consumer.Actions{}))
				Expect(s.State).To(Equal(internal.InvalidBatch))
			})
		})
	})

	Describe("ApplyPrepare", func() {
		BeforeEach(func() {
			s.State = internal.Validated
			s.Digest = []byte("digest")
			s.Prepares["digest"] = map[internal.NodeID]struct{}{
				1: {},
				2: {},
			}
		})

		It("transitions from Validated to Prepared", func() {
			actions := s.ApplyPrepare(0, []byte("digest"))
			Expect(actions).To(Equal(&consumer.Actions{
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
