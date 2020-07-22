/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	pb "github.com/IBM/mirbft/mirbftpb"

	"go.uber.org/zap"
)

var _ = Describe("Integration", func() {
	var (
		serializer     *serializer
		stateMachine   *stateMachine
		epochConfig    *pb.EpochConfig
		networkConfig  *pb.NetworkConfig
		consumerConfig *Config
		logger         *zap.Logger

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
			BufferSize: 500,
		}

		doneC = make(chan struct{})
	})

	AfterEach(func() {
		logger.Sync()
		close(doneC)

		tDesc := CurrentGinkgoTestDescription()
		if tDesc.Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			fmt.Printf("\nStatus of statemachine:\n%s\n", stateMachine.status().Pretty())
			exitErr := serializer.getExitErr()
			if exitErr != nil {
				fmt.Printf("Serializer had exit error: %s\n", exitErr)
			}
		}

	})

	Describe("F=0,N=1", func() {
		BeforeEach(func() {
			epochConfig = &pb.EpochConfig{
				Number:  3,
				Leaders: []uint64{0},
			}

			networkConfig = &pb.NetworkConfig{
				CheckpointInterval: 2,
				F:                  0,
				Nodes:              []uint64{0},
				NumberOfBuckets:    1,
				MaxEpochLength:     10,
				Clients: []*pb.NetworkConfig_Client{
					{
						Id:                  9,
						BucketLowWatermarks: []uint64{51},
					},
				},
			}

			persisted := newPersisted(consumerConfig)
			persisted.addCEntry(
				&pb.CEntry{
					SeqNo:           0,
					CheckpointValue: []byte("TODO, get from state"),
					NetworkConfig:   networkConfig,
					EpochConfig:     epochConfig,
				},
			)

			stateMachine = newStateMachine(consumerConfig, persisted)
			stateMachine.activeEpoch = newEpoch(persisted, stateMachine.clientWindows, consumerConfig)
			stateMachine.nodeMsgs[0].setActiveEpoch(stateMachine.activeEpoch)

			serializer = newSerializer(stateMachine, doneC)
		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.propC <- &pb.Request{
				ClientId: 9,
				ReqNo:    51,
				Data:     []byte("data"),
			}
			actions := &Actions{}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}},
				Hash: []*HashRequest{
					{
						Data: [][]byte{
							uint64ToBytes(9),
							uint64ToBytes(51),
							[]byte("data"),
						},
						Request: &Request{
							Source: 0,
							Request: &pb.Request{
								ClientId: 9,
								ReqNo:    51,
								Data:     []byte("data"),
							},
						},
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.resultsC <- ActionResults{
				Digests: []*HashResult{
					{
						Digest: []byte("request-digest"),
						Request: &HashRequest{
							Data: [][]byte{
								uint64ToBytes(9),
								uint64ToBytes(1),
								[]byte("data"),
							},
							Request: &Request{
								Source: 0,
								Request: &pb.Request{
									ClientId: 9,
									ReqNo:    51,
									Data:     []byte("data"),
								},
							},
						},
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_RequestAck{
							RequestAck: &pb.RequestAck{
								ClientId: 9,
								ReqNo:    51,
								Digest:   []byte("request-digest"),
							},
						},
					},
				},
			}))

			By("applying our own ack")
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}},
				Hash: []*HashRequest{
					{
						Data: [][]byte{
							[]byte("request-digest"),
						},
						Batch: &Batch{
							Source: 0,
							Epoch:  3,
							SeqNo:  1,
							RequestAcks: []*pb.RequestAck{
								{
									ClientId: 9,
									ReqNo:    51,
									Digest:   []byte("request-digest"),
								},
							},
						},
					},
				},
			}))

			By("returning a the process result for the batch")
			serializer.resultsC <- ActionResults{
				Digests: []*HashResult{
					{
						Request: &HashRequest{
							Data: [][]byte{
								uint64ToBytes(7),
							},
							Batch: &Batch{
								Source: 0,
								Epoch:  3,
								SeqNo:  1,
								RequestAcks: []*pb.RequestAck{
									{
										ClientId: 9,
										ReqNo:    51,
										Digest:   []byte("request-digest"),
									},
								},
							},
						},

						Digest: []byte("batch-digest"),
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_ForwardRequest{
							ForwardRequest: &pb.ForwardRequest{
								Request: &pb.Request{
									ClientId: 9,
									ReqNo:    51,
									Data:     []byte("data"),
								},
								Digest: []byte("request-digest"),
							},
						},
					},
					{
						Type: &pb.Msg_Preprepare{
							Preprepare: &pb.Preprepare{
								Epoch: 3,
								SeqNo: 1,
								Batch: []*pb.RequestAck{
									{
										ClientId: 9,
										ReqNo:    51,
										Digest:   []byte("request-digest"),
									},
								},
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_QEntry{
							QEntry: &pb.QEntry{
								SeqNo:  1,
								Digest: []byte("batch-digest"),
								Requests: []*pb.ForwardRequest{
									{
										Request: &pb.Request{
											ClientId: 9,
											ReqNo:    51,
											Data:     []byte("data"),
										},
										Digest: []byte("request-digest"),
									},
								},
							},
						},
					},
				},
			}))

			By("broadcasting the pre-prepare to myself")
			serializer.stepC <- step{
				Source: 0,
				Msg:    actions.Broadcast[1],
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								Epoch:  3,
								SeqNo:  1,
								Digest: []byte("batch-digest"),
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_PEntry{
							PEntry: &pb.PEntry{
								SeqNo:  1,
								Digest: []byte("batch-digest"),
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
				Replicas: []Replica{{ID: 0}},
				Commits: []*Commit{
					{
						QEntry: &pb.QEntry{
							SeqNo:  1,
							Digest: []byte("batch-digest"),
							Requests: []*pb.ForwardRequest{
								{
									Request: &pb.Request{
										ClientId: 9,
										ReqNo:    51,
										Data:     []byte("data"),
									},
									Digest: []byte("request-digest"),
								},
							},
						},
						NetworkConfig: networkConfig,
						EpochConfig:   epochConfig,
					},
				},
			}))
		})
	})

	Describe("F=1,N=4", func() {
		BeforeEach(func() {
			epochConfig = &pb.EpochConfig{
				Number:  2,
				Leaders: []uint64{0, 1, 2, 3},
			}

			networkConfig = &pb.NetworkConfig{
				CheckpointInterval: 5,
				F:                  1,
				Nodes:              []uint64{0, 1, 2, 3},
				NumberOfBuckets:    4,
				MaxEpochLength:     10,
				Clients: []*pb.NetworkConfig_Client{
					{
						Id:                  9,
						BucketLowWatermarks: []uint64{0, 1, 2, 3},
					},
				},
			}

			persisted := newPersisted(consumerConfig)
			persisted.lastCommitted = 1
			persisted.addCEntry(
				&pb.CEntry{
					SeqNo:           0,
					CheckpointValue: []byte("TODO, get from state"),
					NetworkConfig:   networkConfig,
					EpochConfig:     epochConfig,
				},
			)
			stateMachine = newStateMachine(consumerConfig, persisted)
			stateMachine.activeEpoch = newEpoch(persisted, stateMachine.clientWindows, consumerConfig)
			stateMachine.activeEpoch.sequences[0].state = Committed
			stateMachine.activeEpoch.lowestUncommitted = 1
			stateMachine.nodeMsgs[0].setActiveEpoch(stateMachine.activeEpoch)
			stateMachine.nodeMsgs[1].setActiveEpoch(stateMachine.activeEpoch)
			stateMachine.nodeMsgs[2].setActiveEpoch(stateMachine.activeEpoch)
			stateMachine.nodeMsgs[3].setActiveEpoch(stateMachine.activeEpoch)

			serializer = newSerializer(stateMachine, doneC)

		})

		It("works from proposal through commit", func() {
			By("proposing a message")
			serializer.propC <- &pb.Request{
				ClientId: 9,
				ReqNo:    1,
				Data:     []byte("data"),
			}
			actions := &Actions{}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Hash: []*HashRequest{
					{
						Data: [][]byte{
							uint64ToBytes(9),
							uint64ToBytes(1),
							[]byte("data"),
						},
						Request: &Request{
							Source: 0,
							Request: &pb.Request{
								ClientId: 9,
								ReqNo:    1,
								Data:     []byte("data"),
							},
						},
					},
				},
			}))

			By("returning a processed version of the proposal")
			serializer.resultsC <- ActionResults{
				Digests: []*HashResult{
					{
						Digest: []byte("request-digest"),
						Request: &HashRequest{
							Data: [][]byte{
								[]byte("client-1"),
								uint64ToBytes(1),
								[]byte("data"),
							},
							Request: &Request{
								Source: 0,
								Request: &pb.Request{
									ClientId: 9,
									ReqNo:    1,
									Data:     []byte("data"),
								},
							},
						},
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_RequestAck{
							RequestAck: &pb.RequestAck{
								ClientId: 9,
								ReqNo:    1,
								Digest:   []byte("request-digest"),
							},
						},
					},
				},
			}))

			By("applying our own ack and receiving two acks for the request")
			serializer.stepC <- step{
				Source: 0,
				Msg: &pb.Msg{
					Type: &pb.Msg_RequestAck{
						RequestAck: &pb.RequestAck{
							ClientId: 9,
							ReqNo:    1,
							Digest:   []byte("request-digest"),
						},
					},
				},
			}
			serializer.stepC <- step{
				Source: 1,
				Msg: &pb.Msg{
					Type: &pb.Msg_RequestAck{
						RequestAck: &pb.RequestAck{
							ClientId: 9,
							ReqNo:    1,
							Digest:   []byte("request-digest"),
						},
					},
				},
			}
			serializer.stepC <- step{
				Source: 2,
				Msg: &pb.Msg{
					Type: &pb.Msg_RequestAck{
						RequestAck: &pb.RequestAck{
							ClientId: 9,
							ReqNo:    1,
							Digest:   []byte("request-digest"),
						},
					},
				},
			}

			By("faking a preprepare from the leader")
			serializer.stepC <- step{
				Source: 3,
				Msg: &pb.Msg{
					Type: &pb.Msg_Preprepare{
						Preprepare: &pb.Preprepare{
							Epoch: 2,
							SeqNo: 2,
							Batch: []*pb.RequestAck{
								{
									ClientId: 9,
									ReqNo:    1,
									Digest:   []byte("request-digest"),
								},
							},
						},
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Hash: []*HashRequest{
					{
						Batch: &Batch{
							Source: 3,
							Epoch:  2,
							SeqNo:  2,
							RequestAcks: []*pb.RequestAck{
								{
									ClientId: 9,
									ReqNo:    1,
									Digest:   []byte("request-digest"),
								},
							},
						},
						Data: [][]byte{
							[]byte("request-digest"),
						},
					},
				},
			}))

			By("returning a digest for the batch")
			serializer.resultsC <- ActionResults{
				Digests: []*HashResult{
					{
						Request: &HashRequest{
							Batch: &Batch{
								Source: 3,
								Epoch:  2,
								SeqNo:  2,
								RequestAcks: []*pb.RequestAck{
									{
										ClientId: 9,
										ReqNo:    1,
										Digest:   uint64ToBytes(7),
									},
								},
							},
							Data: [][]byte{
								[]byte("request-digest"),
							},
						},
						Digest: []byte("batch-digest"),
					},
				},
			}
			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Prepare{
							Prepare: &pb.Prepare{
								Epoch:  2,
								SeqNo:  2,
								Digest: []byte("batch-digest"),
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_QEntry{
							QEntry: &pb.QEntry{
								SeqNo:  2,
								Digest: []byte("batch-digest"),
								Requests: []*pb.ForwardRequest{
									{
										Request: &pb.Request{
											ClientId: 9,
											ReqNo:    1,
											Data:     []byte("data"),
										},
										Digest: []byte("request-digest"),
									},
								},
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
				Source: 2,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Broadcast: []*pb.Msg{
					{
						Type: &pb.Msg_Commit{
							Commit: &pb.Commit{
								Epoch:  2,
								SeqNo:  2,
								Digest: []byte("batch-digest"),
							},
						},
					},
				},
				Persist: []*pb.Persistent{
					{
						Type: &pb.Persistent_PEntry{
							PEntry: &pb.PEntry{
								SeqNo:  2,
								Digest: []byte("batch-digest"),
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
				Source: 2,
				Msg:    actions.Broadcast[0],
			}

			serializer.stepC <- step{
				Source: 3,
				Msg:    actions.Broadcast[0],
			}

			Eventually(serializer.actionsC).Should(Receive(actions))
			Expect(actions).To(Equal(&Actions{
				Replicas: []Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}},
				Commits: []*Commit{
					{
						QEntry: &pb.QEntry{
							SeqNo:  2,
							Digest: []byte("batch-digest"),
							Requests: []*pb.ForwardRequest{
								{
									Request: &pb.Request{
										ClientId: 9,
										ReqNo:    1,
										Data:     []byte("data"),
									},
									Digest: []byte("request-digest"),
								},
							},
						},
						NetworkConfig: networkConfig,
						EpochConfig:   epochConfig,
					},
				},
			}))
		})
	})
})
