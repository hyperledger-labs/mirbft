/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/sample"

	"go.uber.org/zap"
)

var _ = Describe("MirBFT", func() {
	var (
		doneC  chan struct{}
		logger *zap.Logger

		network *Network
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

	DescribeTable("commits all messages", func(nodeCount int, networkCustomizers ...func(*Network)) {
		network = CreateNetwork(nodeCount, logger, doneC)

		for _, networkCustomizer := range networkCustomizers {
			networkCustomizer(network)
		}

		network.GoRunNetwork(doneC)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		// msgs must be < 1000 (hardcoded in the createNetwork call.
		// this math is to ensure that the messages end on a multiple
		// of the checkpoint interval, when spread across the buckets.
		msgs := ((1000 / nodeCount) / 5) * 5 * nodeCount

		for i := 0; i < msgs; i++ {
			value := make([]byte, 8)
			binary.LittleEndian.PutUint64(value, uint64(i))
			// Unevenly propose across the nodes
			err := network.nodes[i%2%nodeCount].Propose(ctx, value)
			Expect(err).NotTo(HaveOccurred())
		}

		observations := map[uint64]struct{}{}
		for j, fakeLog := range network.fakeLogs {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < msgs {
				entry := &mirbft.Entry{}
				Eventually(fakeLog.CommitC).Should(Receive(&entry))
				Expect(entry.Epoch).To(Equal(uint64(0)))

				if entry.Batch == nil {
					continue
				}

				msgNo := binary.LittleEndian.Uint64(entry.Batch[0])
				Expect(msgNo).To(Equal(entry.SeqNo - 1))

				_, ok := observations[msgNo]
				Expect(ok).To(BeFalse())
				observations[msgNo] = struct{}{}
			}
		}
	},
		Entry("SingleNode", 1),
		PEntry("ThreeNodeCFT", 3),
		Entry("FourNodeBFT", 4),
		PEntry("FourNodeBFT with fault", 4, BrokenLinks(map[uint64]map[uint64]struct{}{
			2: {0: struct{}{}, 1: struct{}{}, 3: struct{}{}},
			// XXX, this one is weird 1: {0: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		})),
	)

	JustAfterEach(func() {
		if CurrentGinkgoTestDescription().Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			ctx, cancel := context.WithTimeout(context.TODO(), 50*time.Millisecond)
			defer cancel()

			Expect(network).NotTo(BeNil())

			for nodeIndex, node := range network.nodes {
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

func (fl *FakeLog) Snap() []byte {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(len(fl.Entries)))
	return value
}

type BrokenLink struct {
	FakeLink    *sample.FakeLink
	Unreachable map[uint64]struct{}
}

func (bl *BrokenLink) Send(dest uint64, msg *pb.Msg) {
	// Always send Forward messages, as they're needed for the test to work
	if _, ok := msg.Type.(*pb.Msg_Forward); !ok {
		if _, ok = bl.Unreachable[dest]; ok {
			// drop the message
			fmt.Println("Dropping message to", dest)
			return
		}
	}
	bl.FakeLink.Send(dest, msg)
}

func BrokenLinks(sourceDestsBroken map[uint64]map[uint64]struct{}) func(*Network) {
	return func(network *Network) {
		for source, dests := range sourceDestsBroken {
			for i, node := range network.nodes {
				if node.Config.ID != source {
					continue
				}

				network.processors[i].Link = &BrokenLink{
					Unreachable: dests,
				}
			}
		}
	}
}

type Network struct {
	nodes      []*mirbft.Node
	fakeLogs   []*FakeLog
	processors []*sample.SerialProcessor
}

func CreateNetwork(nodeCount int, logger *zap.Logger, doneC <-chan struct{}) *Network {
	nodes := make([]*mirbft.Node, nodeCount)
	replicas := make([]mirbft.Replica, nodeCount)

	for i := range replicas {
		replicas[i] = mirbft.Replica{ID: uint64(i)}
	}

	for i := range nodes {
		config := &mirbft.Config{
			ID:     uint64(i),
			Logger: logger.Named(fmt.Sprintf("node%d", i)),
			BatchParameters: mirbft.BatchParameters{
				CutSizeBytes: 1,
			},
			SuspectTicks:         4,
			NewEpochTimeoutTicks: 8,
		}

		node, err := mirbft.StartNewNode(config, doneC, replicas)
		Expect(err).NotTo(HaveOccurred())
		nodes[i] = node
	}

	fakeLogs := make([]*FakeLog, nodeCount)
	processors := make([]*sample.SerialProcessor, nodeCount)
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
				Log:               fakeLog,
				OutstandingSeqNos: map[uint64]*mirbft.Entry{},
			},
			DoneC: doneC,
		}

	}

	return &Network{
		nodes:      nodes,
		fakeLogs:   fakeLogs,
		processors: processors,
	}
}

func (n *Network) GoRunNetwork(doneC <-chan struct{}) {
	for i := range n.nodes {
		go func(i int, doneC <-chan struct{}) {
			defer GinkgoRecover()
			defer func() {
				Expect(recover()).To(BeNil())
			}()

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case actions := <-n.nodes[i].Ready():
					n.nodes[i].AddResults(*n.processors[i].Process(&actions))
				case <-doneC:
					return
				case <-ticker.C:
					n.nodes[i].Tick()
				}
			}
		}(i, doneC)
	}
}
