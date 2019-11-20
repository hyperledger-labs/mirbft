/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

var _ = Describe("Integration", func() {
	var (
		serializer      *serializer
		stateMachineVal *stateMachine
		epochConfigVal  *epochConfig
		consumerConfig  *Config
		logger          *zap.Logger

		doneC chan struct{}
	)

	BeforeEach(func() {
		var err error
		logger, err = zap.NewDevelopment()
		Expect(err).NotTo(HaveOccurred())

		consumerConfig = &Config{
			ID:     0,
			Logger: logger,
			BatchParameters: BatchParameters{
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
			epochConfigVal = &epochConfig{
				myConfig:           consumerConfig,
				number:             3,
				checkpointInterval: 2,
				highWatermark:      20,
				lowWatermark:       0,
				f:                  0,
				nodes:              []NodeID{0},
				buckets:            map[BucketID]NodeID{0: 0},
			}

			stateMachineVal = newStateMachine(epochConfigVal)

			serializer = newSerializer(stateMachineVal, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.propC <- []byte("data")
			actions := &Actions{}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Preprocess: []Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.resultsC <- ActionResults{
				Preprocesses: []PreprocessResult{
					{
						Cup: 7,
						Proposal: Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
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
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Digest: []*Entry{
					{
						Epoch:    3,
						BucketID: 0,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.resultsC <- ActionResults{
				Digests: []DigestResult{
					{
						Entry: &Entry{
							Epoch:    3,
							BucketID: 0,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Digest: []byte("fake-digest"),
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
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
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Commit: []*Entry{
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
			epochConfigVal = &epochConfig{
				myConfig:           consumerConfig,
				number:             3,
				checkpointInterval: 2,
				highWatermark:      20,
				lowWatermark:       0,
				f:                  1,
				nodes:              []NodeID{0, 1, 2, 3},
				buckets:            map[BucketID]NodeID{0: 0, 1: 1, 2: 2, 3: 3},
			}

			stateMachineVal = newStateMachine(epochConfigVal)

			serializer = newSerializer(stateMachineVal, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.propC <- []byte("data")
			actions := &Actions{}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Preprocess: []Proposal{
					{
						Source: 0,
						Data:   []byte("data"),
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.resultsC <- ActionResults{
				Preprocesses: []PreprocessResult{
					{
						Cup: 7,
						Proposal: Proposal{
							Source: 0,
							Data:   []byte("data"),
						},
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Unicast: []Unicast{
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
			serializer.stepC <- step{
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
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Digest: []*Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.resultsC <- ActionResults{
				Digests: []DigestResult{
					{
						Entry: &Entry{
							Epoch:    3,
							BucketID: 3,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Digest: []byte("fake-digest"),
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Validate: []*Entry{
					{
						Epoch:    3,
						BucketID: 3,
						SeqNo:    1,
						Batch:    [][]byte{[]byte("data")},
					},
				},
			}))

			By("returning a successful validatation for the batch")
			serializer.resultsC <- ActionResults{
				Validations: []ValidateResult{
					{
						Entry: &Entry{
							Epoch:    3,
							BucketID: 3,
							SeqNo:    1,
							Batch:    [][]byte{[]byte("data")},
						},
						Valid: true,
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
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
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.stepC <- step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
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
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			serializer.stepC <- step{
				Source: 1,
				Msg:    actions.Broadcast[0],
			}

			serializer.stepC <- step{
				Source: 3,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Commit: []*Entry{
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
