/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"context"
	"encoding/binary"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	"github.com/IBM/mirbft/consumer"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/sample"

	"go.uber.org/zap"
)

var _ = Describe("MirBFT", func() {
	var (
		doneC  chan struct{}
		logger *zap.Logger
	)

	BeforeEach(func() {
		var err error
		//logger, err = zap.NewDevelopment()
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		doneC = make(chan struct{})
	})

	AfterEach(func() {
		logger.Sync()
		close(doneC)
	})

	Describe("SingleNode", func() {
		It("commits all messages", func() {
			config := &consumer.Config{
				ID:     0,
				Logger: logger.Named("node0"),
				BatchParameters: consumer.BatchParameters{
					CutSizeBytes: 1,
				},
			}

			node, err := mirbft.StartNewNode(config, doneC, []mirbft.Replica{{ID: 0}})
			Expect(err).NotTo(HaveOccurred())
			Expect(node).NotTo(BeNil())

			commitC := make(chan *consumer.Entry, 1000)

			sample.NewSerialConsumer(
				doneC,
				commitC,
				node,
				nil,
				sample.ValidatorFunc(func([]byte) error { return nil }),
				sample.HasherFunc(func(data []byte) []byte { return data }),
			)

			ctx := context.TODO()
			for i := uint64(0); i < 1000; i++ {
				node.Propose(ctx, []byte(fmt.Sprintf("msg %d", i)))
			}

			for i := uint64(0); i < 1000; i++ {
				entry := &consumer.Entry{}
				Eventually(commitC).Should(Receive(&entry))
				Expect(entry).To(Equal(&consumer.Entry{
					SeqNo:    i,
					BucketID: 0,
					Epoch:    0,
					Batch:    [][]byte{[]byte(fmt.Sprintf("msg %d", i))},
				}))
			}
		})
	})

	Describe("MultiNode", func() {
		It("commits all messages", func() {
			replicas := []mirbft.Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}
			nodes := make([]*mirbft.Node, 4)
			for i := range nodes {
				config := &consumer.Config{
					ID:     uint64(i),
					Logger: logger.Named(fmt.Sprintf("node%d", i)),
					BatchParameters: consumer.BatchParameters{
						CutSizeBytes: 1,
					},
				}

				node, err := mirbft.StartNewNode(config, doneC, replicas)
				Expect(err).NotTo(HaveOccurred())
				nodes[i] = node
			}

			commitCs := make([]chan *consumer.Entry, 4)
			for i, node := range nodes {
				commitC := make(chan *consumer.Entry, 1000)
				commitCs[i] = commitC

				sample.NewSerialConsumer(
					doneC,
					commitC,
					node,
					NewFakeLink(node.Config.ID, nodes, doneC),
					sample.ValidatorFunc(func([]byte) error { return nil }),
					sample.HasherFunc(func(data []byte) []byte { return data }),
				)
			}

			ctx := context.TODO()
			for i := uint64(0); i < 1000; i++ {
				value := make([]byte, 8)
				binary.LittleEndian.PutUint64(value, i)

				// Propose to only the first 3 nodes round robin, ensuring some are forwarded and others not
				nodes[i%3].Propose(ctx, value)
			}

			for j, commitC := range commitCs {
				By(fmt.Sprintf("checking for node %d that each bucket only commits a sequence number once", j))
				bucketToSeq := map[uint64]map[uint64]struct{}{}
				for i := uint64(0); i < 1000; i++ {
					entry := &consumer.Entry{}
					Eventually(commitC).Should(Receive(&entry))

					Expect(entry.Epoch).To(Equal(uint64(0)))

					msgNo := binary.LittleEndian.Uint64(entry.Batch[0])
					Expect(msgNo % 4).To(Equal(entry.BucketID))

					seqs, ok := bucketToSeq[entry.BucketID]
					if !ok {
						seqs = map[uint64]struct{}{}
						bucketToSeq[entry.BucketID] = seqs
					}

					_, ok = seqs[entry.SeqNo]
					Expect(ok).To(BeFalse())
					seqs[entry.SeqNo] = struct{}{}
				}

				By(fmt.Sprintf("checking for node %d that each bucket commits every sequence", j))
				for j := uint64(0); j < 4; j++ {
					seqs := bucketToSeq[j]
					for i := uint64(0); i < 250; i++ {
						_, ok := seqs[i]
						Expect(ok).To(BeTrue())
					}
				}
			}
		})
	})
})

type FakeLink struct {
	Buffers map[uint64]chan *pb.Msg
}

func NewFakeLink(source uint64, nodes []*mirbft.Node, doneC <-chan struct{}) *FakeLink {
	buffers := map[uint64]chan *pb.Msg{}
	for _, node := range nodes {
		if node.Config.ID == source {
			continue
		}
		buffer := make(chan *pb.Msg, 1000)
		buffers[node.Config.ID] = buffer
		go func(node *mirbft.Node) {
			for {
				select {
				case msg := <-buffer:
					node.Step(context.TODO(), source, msg)
				case <-doneC:
					return
				}
			}
		}(node)
	}
	return &FakeLink{
		Buffers: buffers,
	}
}

func (fl *FakeLink) Send(dest uint64, msg *pb.Msg) {
	fl.Buffers[dest] <- msg
}
