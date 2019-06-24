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

	"go.uber.org/zap"
)

var _ = Describe("Integration", func() {
	var (
		serializer     *mirbft.Serializer
		stateMachine   *mirbft.StateMachine
		epoch          *mirbft.Epoch
		epochConfig    *mirbft.EpochConfig
		consumerConfig *mirbft.Config
		logger         *zap.Logger

		doneC chan struct{}
	)

	BeforeEach(func() {
		var err error
		logger, err = zap.NewDevelopment()
		Expect(err).NotTo(HaveOccurred())

		consumerConfig = &mirbft.Config{
			ID:     0,
			Logger: logger,
			BatchParameters: mirbft.BatchParameters{
				CutSizeBytes: 1,
			},
		}

		doneC = make(chan struct{})
	})

	AfterEach(func() {
		logger.Sync()
		close(doneC)
	})

	Describe("F=0,N=1", func() {
		BeforeEach(func() {
			epochConfig = &mirbft.EpochConfig{
				MyConfig: consumerConfig,
				Oddities: &mirbft.Oddities{
					Nodes: map[mirbft.NodeID]*mirbft.Oddity{},
				},
				Number:             3,
				CheckpointInterval: 2,
				HighWatermark:      20,
				LowWatermark:       0,
				F:                  0,
				Nodes:              []mirbft.NodeID{0},
				Buckets:            map[mirbft.BucketID]mirbft.NodeID{0: 0},
			}

			epoch = mirbft.NewEpoch(epochConfig)

			stateMachine = &mirbft.StateMachine{
				Config:       consumerConfig,
				CurrentEpoch: epoch,
			}

			serializer = mirbft.NewSerializer(stateMachine, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.PropC <- []byte("data")
			actions := &mirbft.Actions{}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Preprocess: []mirbft.Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.ResultsC <- mirbft.ActionResults{
				Preprocesses: []mirbft.PreprocessResult{
					{
						Cup: 7,
						Proposal: mirbft.Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Preprepare{
							Preprepare: &pb.Preprepare{
								Epoch:  3,
								Bucket: 0,
								SeqNo:  1,
								Batch:  [][]byte{[]byte("data")},
							},
						},
					},
				},
			}))

			By("broadcasting the preprepare to myself")
			serializer.StepC <- mirbft.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Digest: []*mirbft.Entry{
					{
						Epoch:    3,
						BucketID: 0,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.ResultsC <- mirbft.ActionResults{
				Digests: []mirbft.DigestResult{
					{
						Entry: &mirbft.Entry{
							Epoch:    3,
							BucketID: 0,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Digest: []byte("fake-digest"),
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								Epoch:  3,
								Bucket: 0,
								SeqNo:  1,
								Digest: []byte(("fake-digest")),
							},
						},
					},
				},
			}))

			By("broadcasting the commit to myself")
			serializer.StepC <- mirbft.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Commit: []*mirbft.Entry{
					{
						Epoch:    3,
						BucketID: 0,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))
		})
	})

	Describe("F=1,N=4", func() {
		BeforeEach(func() {
			epochConfig = &mirbft.EpochConfig{
				MyConfig: consumerConfig,
				Oddities: &mirbft.Oddities{
					Nodes: map[mirbft.NodeID]*mirbft.Oddity{},
				},
				Number:             3,
				CheckpointInterval: 2,
				HighWatermark:      20,
				LowWatermark:       0,
				F:                  1,
				Nodes:              []mirbft.NodeID{0, 1, 2, 3},
				Buckets:            map[mirbft.BucketID]mirbft.NodeID{0: 0, 1: 1, 2: 2, 3: 3},
			}

			epoch = mirbft.NewEpoch(epochConfig)

			stateMachine = &mirbft.StateMachine{
				Config:       consumerConfig,
				CurrentEpoch: epoch,
			}

			serializer = mirbft.NewSerializer(stateMachine, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.PropC <- []byte("data")
			actions := &mirbft.Actions{}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Preprocess: []mirbft.Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.ResultsC <- mirbft.ActionResults{
				Preprocesses: []mirbft.PreprocessResult{
					{
						Cup: 7,
						Proposal: mirbft.Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Unicast: []mirbft.Unicast{
					{
						Target: 3,
						Msg: &pb.Msg{
							Type: &pb.Msg_Forward{
								Forward: &pb.Forward{
									Epoch:  3,
									Bucket: 3,
									Data:   []byte("data"),
								},
							},
						},
					},
				},
			}))

			By("faking a preprepare from the leader")
			serializer.StepC <- mirbft.Step{
				Source: 3,
				Msg: &pb.Msg{
					Type: &pb.Msg_Preprepare{
						Preprepare: &pb.Preprepare{
							Epoch:  3,
							Bucket: 3,
							SeqNo:  1,
							Batch:  [][]byte{[]byte("data")},
						},
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Digest: []*mirbft.Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.ResultsC <- mirbft.ActionResults{
				Digests: []mirbft.DigestResult{
					{
						Entry: &mirbft.Entry{
							Epoch:    3,
							BucketID: 3,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Digest: []byte("fake-digest"),
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Validate: []*mirbft.Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a successful validatation for the batch")
			serializer.ResultsC <- mirbft.ActionResults{
				Validations: []mirbft.ValidateResult{
					{
						Entry: &mirbft.Entry{
							Epoch:    3,
							BucketID: 3,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Valid: true,
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Prepare{
							Prepare: &pb.Prepare{
								Epoch:  3,
								Bucket: 3,
								SeqNo:  1,
								Digest: []byte(("fake-digest")),
							},
						},
					},
				},
			}))

			By("broadcasting the prepare to myself, and from one other node")
			serializer.StepC <- mirbft.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- mirbft.Step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								Epoch:  3,
								Bucket: 3,
								SeqNo:  1,
								Digest: []byte(("fake-digest")),
							},
						},
					},
				},
			}))

			By("broadcasting the commit to myself, and from two other nodes")
			serializer.StepC <- mirbft.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- mirbft.Step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- mirbft.Step{
				Source: 3,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&mirbft.Actions{
				Commit: []*mirbft.Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))
		})
	})
})
