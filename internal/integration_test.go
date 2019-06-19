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

	"go.uber.org/zap"
)

var _ = Describe("Integration", func() {
	var (
		serializer     *internal.Serializer
		stateMachine   *internal.StateMachine
		epoch          *internal.Epoch
		epochConfig    *internal.EpochConfig
		consumerConfig *consumer.Config
		logger         *zap.Logger

		doneC chan struct{}
	)

	BeforeEach(func() {
		var err error
		logger, err = zap.NewDevelopment()
		Expect(err).NotTo(HaveOccurred())

		consumerConfig = &consumer.Config{
			ID:     0,
			Logger: logger,
			BatchParameters: consumer.BatchParameters{
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
			epochConfig = &internal.EpochConfig{
				MyConfig: consumerConfig,
				Oddities: &internal.Oddities{
					Nodes: map[internal.NodeID]*internal.Oddity{},
				},
				Number:             3,
				CheckpointInterval: 2,
				HighWatermark:      20,
				LowWatermark:       0,
				F:                  0,
				Nodes:              []internal.NodeID{0},
				Buckets:            map[internal.BucketID]internal.NodeID{0: 0},
			}

			epoch = internal.NewEpoch(epochConfig)

			stateMachine = &internal.StateMachine{
				Config:       consumerConfig,
				CurrentEpoch: epoch,
			}

			serializer = internal.NewSerializer(stateMachine, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.PropC <- []byte("data")
			actions := &consumer.Actions{}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Preprocess: []consumer.Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.ResultsC <- consumer.ActionResults{
				Preprocesses: []consumer.PreprocessResult{
					{
						Cup: 7,
						Proposal: consumer.Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
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
			serializer.StepC <- internal.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Digest: []*consumer.Entry{
					{
						Epoch:    3,
						BucketID: 0,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.ResultsC <- consumer.ActionResults{
				Digests: []consumer.DigestResult{
					{
						Entry: &consumer.Entry{
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
			Expect(actions).To(Equal(&consumer.Actions{
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
			serializer.StepC <- internal.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Commit: []*consumer.Entry{
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
			epochConfig = &internal.EpochConfig{
				MyConfig: consumerConfig,
				Oddities: &internal.Oddities{
					Nodes: map[internal.NodeID]*internal.Oddity{},
				},
				Number:             3,
				CheckpointInterval: 2,
				HighWatermark:      20,
				LowWatermark:       0,
				F:                  1,
				Nodes:              []internal.NodeID{0, 1, 2, 3},
				Buckets:            map[internal.BucketID]internal.NodeID{0: 0, 1: 1, 2: 2, 3: 3},
			}

			epoch = internal.NewEpoch(epochConfig)

			stateMachine = &internal.StateMachine{
				Config:       consumerConfig,
				CurrentEpoch: epoch,
			}

			serializer = internal.NewSerializer(stateMachine, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.PropC <- []byte("data")
			actions := &consumer.Actions{}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Preprocess: []consumer.Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.ResultsC <- consumer.ActionResults{
				Preprocesses: []consumer.PreprocessResult{
					{
						Cup: 7,
						Proposal: consumer.Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Unicast: []consumer.Unicast{
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
			serializer.StepC <- internal.Step{
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
			Expect(actions).To(Equal(&consumer.Actions{
				Digest: []*consumer.Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.ResultsC <- consumer.ActionResults{
				Digests: []consumer.DigestResult{
					{
						Entry: &consumer.Entry{
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
			Expect(actions).To(Equal(&consumer.Actions{
				Validate: []*consumer.Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a successful validatation for the batch")
			serializer.ResultsC <- consumer.ActionResults{
				Validations: []consumer.ValidateResult{
					{
						Entry: &consumer.Entry{
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
			Expect(actions).To(Equal(&consumer.Actions{
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
			serializer.StepC <- internal.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- internal.Step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
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
			serializer.StepC <- internal.Step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- internal.Step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			serializer.StepC <- internal.Step{
				Source: 3,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.ActionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&consumer.Actions{
				Commit: []*consumer.Entry{
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
