/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
)

var _ = Describe("Sequence", func() {
	var (
		s *mirbft.Sequence
	)

	BeforeEach(func() {
		s = &mirbft.Sequence{
			EpochConfig: &mirbft.EpochConfig{
				MyConfig: &mirbft.Config{
					ID: 1,
				},
				Number: 4,
				F:      1,
			},
			Entry: &mirbft.Entry{
				Epoch:    4,
				SeqNo:    5,
				BucketID: 3,
			},
			Prepares: map[string]map[mirbft.NodeID]struct{}{},
			Commits:  map[string]map[mirbft.NodeID]struct{}{},
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

			Expect(actions).To(Equal(&mirbft.Actions{
				Digest: []*mirbft.Entry{
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

			Expect(s.State).To(Equal(mirbft.Preprepared))
			Expect(s.Entry.Batch).To(Equal(
				[][]byte{
					[]byte("msg1"),
					[]byte("msg2"),
				},
			))
		})

		Context("when the current state is not Unknown", func() {
			BeforeEach(func() {
				s.State = mirbft.Validated
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
				Expect(s.State).To(Equal(mirbft.Validated))
			})
		})
	})

	Describe("ApplyValidateResult", func() {
		BeforeEach(func() {
			s.State = mirbft.Digested
			s.Digest = []byte("digest")
		})

		It("transitions from Preprepared to Validated", func() {
			actions := s.ApplyValidateResult(true)
			Expect(actions).To(Equal(&mirbft.Actions{
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
			Expect(s.State).To(Equal(mirbft.Validated))
		})

		Context("when the state is not Preprepared", func() {
			BeforeEach(func() {
				s.State = mirbft.Prepared
			})

			It("does not transition the state and panics", func() {
				badTransition := func() {
					s.ApplyValidateResult(true)
				}
				Expect(badTransition).To(Panic())
				Expect(s.State).To(Equal(mirbft.Prepared))
			})
		})

		Context("when the validation is not successful", func() {
			It("transitions the state to InvalidBatch", func() {
				actions := s.ApplyValidateResult(false)
				Expect(actions).To(Equal(&mirbft.Actions{}))
				Expect(s.State).To(Equal(mirbft.InvalidBatch))
			})
		})
	})

	Describe("ApplyPrepare", func() {
		BeforeEach(func() {
			s.State = mirbft.Validated
			s.Digest = []byte("digest")
			s.Prepares["digest"] = map[mirbft.NodeID]struct{}{
				1: {},
				2: {},
			}
		})

		It("transitions from Validated to Prepared", func() {
			actions := s.ApplyPrepare(0, []byte("digest"))
			Expect(actions).To(Equal(&mirbft.Actions{
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
