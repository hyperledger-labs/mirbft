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
			node      *mirbft.Node
			processor *sample.SerialProcessor
			fakeLog   *FakeLog
		)

		BeforeEach(func() {
			config := &mirbft.Config{
				ID:     0,
				Logger: logger.Named("node0"),
				BatchParameters: mirbft.BatchParameters{
					CutSizeBytes: 1,
				},
			}

			var err error
			node, err = mirbft.StartNewNode(config, doneC, []mirbft.Replica{{ID: 0}})
			Expect(err).NotTo(HaveOccurred())
			Expect(node).NotTo(BeNil())

			fakeLog = &FakeLog{
				CommitC: make(chan *mirbft.Entry, 1000),
			}

			processor = &sample.SerialProcessor{
				Node:      node,
				Validator: sample.ValidatorFunc(func([]byte) error { return nil }),
				Hasher:    sample.HasherFunc(func(data []byte) []byte { return data }),
				Committer: &sample.SerialCommitter{
					Log:                  fakeLog,
					OutstandingSeqBucket: map[uint64]map[uint64]*mirbft.Entry{},
				},
				DoneC: doneC,
			}

			go func(doneC chan struct{}) {
				defer func() {
					Expect(recover()).To(BeNil())
				}()

				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case actions := <-node.Ready():
						node.AddResults(*processor.Process(&actions))
					case <-doneC:
						return
					case <-ticker.C:
						node.Tick()
					}
				}
			}(doneC)
		})

		It("commits all messages", func() {
			ctx := context.TODO()
			for i := uint64(1); i <= 1000; i++ {
				node.Propose(ctx, []byte(fmt.Sprintf("msg %d", i)))
			}

			lastObserved := uint64(0)
			lastSeq := uint64(0)
			for iterations := uint64(1); lastObserved < 1000; iterations++ {
				entry := &mirbft.Entry{}
				Eventually(fakeLog.CommitC).Should(Receive(&entry))
				Expect(lastSeq).To(BeNumerically("<", entry.SeqNo))
				lastSeq = entry.SeqNo
				lastObserved++
				Expect(entry).To(Equal(&mirbft.Entry{
					SeqNo:    lastSeq,
					BucketID: 0,
					Epoch:    0,
					Batch:    [][]byte{[]byte(fmt.Sprintf("msg %d", lastObserved))},
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
					fmt.Printf("\n%s\n", status.Pretty())
				}
			}
		})
	})

	Describe("MultiNode", func() {
		var (
			nodes      []*mirbft.Node
			fakeLogs   []*FakeLog
			processors []*sample.SerialProcessor
		)

		BeforeEach(func() {
			nodes = make([]*mirbft.Node, 4)

			replicas := []mirbft.Replica{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}
			for i := range nodes {
				config := &mirbft.Config{
					ID:     uint64(i),
					Logger: logger.Named(fmt.Sprintf("node%d", i)),
					BatchParameters: mirbft.BatchParameters{
						CutSizeBytes: 1,
					},
				}

				node, err := mirbft.StartNewNode(config, doneC, replicas)
				Expect(err).NotTo(HaveOccurred())
				nodes[i] = node
			}

			fakeLogs = make([]*FakeLog, 4)
			processors = make([]*sample.SerialProcessor, 4)
			for i, node := range nodes {
				node := node
				fakeLog := &FakeLog{
					CommitC: make(chan *mirbft.Entry, 1000),
				}
				fakeLogs[i] = fakeLog

				processors[i] = &sample.SerialProcessor{
					Node:      node,
					Link:      sample.NewFakeLink(node.Config.ID, nodes, doneC),
					Validator: sample.ValidatorFunc(func([]byte) error { return nil }),
					Hasher:    sample.HasherFunc(func(data []byte) []byte { return data }),
					Committer: &sample.SerialCommitter{
						Log:                  fakeLog,
						OutstandingSeqBucket: map[uint64]map[uint64]*mirbft.Entry{},
					},
					DoneC: doneC,
				}

				go func(i int, doneC chan struct{}) {
					defer func() {
						Expect(recover()).To(BeNil())
					}()

					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()

					for {
						select {
						case actions := <-node.Ready():
							node.AddResults(*processors[i].Process(&actions))
						case <-doneC:
							return
						case <-ticker.C:
							nodes[i].Tick()
						}
					}
				}(i, doneC)
			}
		})

		It("commits all messages", func() {

			ctx := context.TODO()
			for i := uint64(0); i < 1000; i++ {
				value := make([]byte, 8)
				binary.LittleEndian.PutUint64(value, i)
				// Propose to only the first 3 nodes round robin, ensuring some are forwarded and others not
				nodes[i%3].Propose(ctx, value)
			}

			observations := map[uint64]struct{}{}
			for j, fakeLog := range fakeLogs {
				By(fmt.Sprintf("checking for node %d that each message only commits once", j))
				for len(observations) < 1000 {
					entry := &mirbft.Entry{}
					Eventually(fakeLog.CommitC).Should(Receive(&entry))
					Expect(entry.Epoch).To(Equal(uint64(0)))

					if entry.Batch == nil {
						continue
					}

					msgNo := binary.LittleEndian.Uint64(entry.Batch[0])
					Expect(msgNo % 4).To(Equal(entry.BucketID))

					_, ok := observations[msgNo]
					Expect(ok).To(BeFalse())
					observations[msgNo] = struct{}{}
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
						fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
					}
				}
			}
		})
	})
})

type FakeLog struct {
	Entries []*mirbft.Entry
	CommitC chan *mirbft.Entry
}

func (fl *FakeLog) Apply(entry *mirbft.Entry) {
	if entry.Batch == nil {
		// this is a no-op batch from a tick, or catchup, ignore it
		return
	}
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
