/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

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
		var (
			node    *mirbft.Node
			fakeLog *FakeLog
		)

		BeforeEach(func() {
			config := &consumer.Config{
				ID:     0,
				Logger: logger.Named("node0"),
				BatchParameters: consumer.BatchParameters{
					CutSizeBytes: 1,
				},
			}

			var err error
			node, err = mirbft.StartNewNode(config, doneC, []mirbft.Replica{{ID: 0}})
			Expect(err).NotTo(HaveOccurred())
			Expect(node).NotTo(BeNil())

			fakeLog = &FakeLog{
				CommitC: make(chan *consumer.Entry, 1000),
			}

			sample.NewSerialConsumer(
				doneC,
				node,
				nil,
				sample.ValidatorFunc(func([]byte) error { return nil }),
				sample.HasherFunc(func(data []byte) []byte { return data }),
				fakeLog,
			)

		})

		It("commits all messages", func() {
			ctx := context.TODO()
			for i := uint64(0); i <= 1000; i++ {
				node.Propose(ctx, []byte(fmt.Sprintf("msg %d", i)))
			}

			for i := uint64(0); i <= 1000; i++ {
				entry := &consumer.Entry{}
				Eventually(fakeLog.CommitC).Should(Receive(&entry))
				Expect(entry).To(Equal(&consumer.Entry{
					SeqNo:    i,
					BucketID: 0,
					Epoch:    0,
					Batch:    [][]byte{[]byte(fmt.Sprintf("msg %d", i))},
				}))
			}
		})

		JustAfterEach(func() {
			if CurrentGinkgoTestDescription().Failed {
				fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
				ctx, cancel := context.WithTimeout(context.TODO(), 50*time.Millisecond)
				defer cancel()
				status, err := node.Status(ctx)
				if err != nil {
					fmt.Printf("Could not get status: %s", err)
				} else {
					fmt.Printf("\n%s\n", status)
				}
			}
		})
	})

	Describe("MultiNode", func() {
		var (
			nodes    []*mirbft.Node
			fakeLogs []*FakeLog
		)

		BeforeEach(func() {
			nodes = make([]*mirbft.Node, 4)

			replicas := []mirbft.Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}
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

			fakeLogs = make([]*FakeLog, 4)
			for i, node := range nodes {
				fakeLog := &FakeLog{
					CommitC: make(chan *consumer.Entry, 1000),
				}
				fakeLogs[i] = fakeLog

				sample.NewSerialConsumer(
					doneC,
					node,
					NewFakeLink(node.Config.ID, nodes, doneC),
					sample.ValidatorFunc(func([]byte) error { return nil }),
					sample.HasherFunc(func(data []byte) []byte { return data }),
					fakeLog,
				)
			}
		})

		It("commits all messages", func() {

			ctx := context.TODO()
			for i := uint64(0); i <= 1003; i++ {
				value := make([]byte, 8)
				binary.LittleEndian.PutUint64(value, i)

				// Propose to only the first 3 nodes round robin, ensuring some are forwarded and others not
				nodes[i%3].Propose(ctx, value)
			}

			for j, fakeLog := range fakeLogs {
				By(fmt.Sprintf("checking for node %d that each bucket only commits a sequence number once", j))
				bucketToSeq := map[uint64]map[uint64]struct{}{}
				for i := uint64(0); i <= 1003; i++ {
					entry := &consumer.Entry{}
					Eventually(fakeLog.CommitC).Should(Receive(&entry))

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
					for i := uint64(0); i <= 250; i++ {
						_, ok := seqs[i]
						Expect(ok).To(BeTrue())
					}
				}
			}
		})

		JustAfterEach(func() {
			if CurrentGinkgoTestDescription().Failed {
				fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
				ctx, cancel := context.WithTimeout(context.TODO(), 50*time.Millisecond)
				defer cancel()
				for nodeIndex, node := range nodes {
					status, err := node.Status(ctx)
					if err != nil {
						fmt.Printf("Could not get status for node %d: %s", nodeIndex, err)
					} else {
						fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status)
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

type FakeLog struct {
	Entries []*consumer.Entry
	CommitC chan *consumer.Entry
}

func (fl *FakeLog) Apply(entry *consumer.Entry) {
	fl.Entries = append(fl.Entries, entry)
	fl.CommitC <- entry
}

func (fl *FakeLog) Snap() ([]byte, []byte) {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(len(fl.Entries)))
	return value, value
}

func (fl *FakeLog) CheckSnap(id, attestation []byte) error {
	if bytes.Equal(id, attestation) {
		return nil
	}
	return fmt.Errorf("fake-error")
}
