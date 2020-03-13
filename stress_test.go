/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/sample"

	"go.uber.org/zap"
)

type NoopHasher struct {
	Value []byte
}

func (nh *NoopHasher) Write(b []byte) (int, error) {
	nh.Value = append(nh.Value, b...)
	return len(b), nil
}

func (nh *NoopHasher) Sum(b []byte) []byte {
	return append(nh.Value, b...)
}

func (nh *NoopHasher) Reset() {
	nh.Value = nil
}

func (nh *NoopHasher) Size() int {
	return len(nh.Value)
}

func (nh *NoopHasher) BlockSize() int {
	return 1
}

type Proposer interface {
	Proposal(nodes, i int) (uint64, []byte)
}

type LinearProposer struct{}

func (LinearProposer) Proposal(nodes, i int) (uint64, []byte) {
	return uint64(i), Uint64ToBytes(uint64(i))
}

type SkippingProposer struct {
	NodesToSkip []int
}

func (sp SkippingProposer) Proposal(nodes, i int) (uint64, []byte) {
	nonSkipped := nodes - len(sp.NodesToSkip)
	alreadySkipped := i / nonSkipped * len(sp.NodesToSkip)
	for j := 0; j <= i%nonSkipped; j++ {
		for _, n := range sp.NodesToSkip {
			if n == j {
				alreadySkipped++
			}
		}
	}

	next := uint64(i + alreadySkipped)
	return next, Uint64ToBytes(next)
}

var _ = Describe("SkippingPropser", func() {
	It("skips what I expect", func() {
		sp := SkippingProposer{
			NodesToSkip: []int{0, 3},
		}

		// Expect 1, 2, 5, 6, 9, 10, 13, 14, ...

		i0, b0 := sp.Proposal(4, 0)
		Expect(i0).To(Equal(uint64(1)))
		Expect(b0).To(Equal(Uint64ToBytes(1)))

		i1, _ := sp.Proposal(4, 1)
		Expect(i1).To(Equal(uint64(2)))

		i2, _ := sp.Proposal(4, 2)
		Expect(i2).To(Equal(uint64(5)))

		i3, _ := sp.Proposal(4, 3)
		Expect(i3).To(Equal(uint64(6)))

		i4, _ := sp.Proposal(4, 4)
		Expect(i4).To(Equal(uint64(9)))

		i5, _ := sp.Proposal(4, 5)
		Expect(i5).To(Equal(uint64(10)))

		i6, _ := sp.Proposal(4, 6)
		Expect(i6).To(Equal(uint64(13)))

		i7, _ := sp.Proposal(4, 7)
		Expect(i7).To(Equal(uint64(14)))
	})
})

type Transport interface {
	Send(source, dest uint64, msg *pb.Msg)
	Link(source uint64) sample.Link
}

type FakeLink struct {
	FakeTransport Transport
	Source        uint64
}

func (fl *FakeLink) Send(dest uint64, msg *pb.Msg) {
	fl.FakeTransport.Send(fl.Source, dest, msg)
}

type FakeTransport struct {
	Destinations []*mirbft.Node
}

func (ft *FakeTransport) Send(source, dest uint64, msg *pb.Msg) {
	ft.Destinations[int(dest)].Step(context.Background(), source, msg)
}

func (ft *FakeTransport) Link(source uint64) sample.Link {
	return &FakeLink{
		Source:        source,
		FakeTransport: ft,
	}
}

type SilencingTransport struct {
	Underlying Transport
	Silenced   uint64
}

func (it *SilencingTransport) Send(source, dest uint64, msg *pb.Msg) {
	if source == it.Silenced {
		return
	}

	it.Underlying.Send(source, dest, msg)
}

func (it *SilencingTransport) Link(source uint64) sample.Link {
	return &FakeLink{
		FakeTransport: it,
		Source:        source,
	}
}

func Silence(source uint64) func(Transport) Transport {
	return func(t Transport) Transport {
		return &SilencingTransport{
			Underlying: t,
			Silenced:   source,
		}
	}
}

type FakeLog struct {
	Entries []*pb.QEntry
	CommitC chan *pb.QEntry
}

func (fl *FakeLog) Apply(entry *pb.QEntry) {
	if entry.Requests == nil {
		// this is a no-op batch from a tick, or catchup, ignore it
		return
	}
	fl.Entries = append(fl.Entries, entry)
	fl.CommitC <- entry
}

func (fl *FakeLog) Snap() []byte {
	return Uint64ToBytes(uint64(len(fl.Entries)))
}

type TestConfig struct {
	NodeCount        int
	BucketCount      int
	MsgCount         int
	Proposer         Proposer
	TransportFilters []func(Transport) Transport
	Expectations     TestExpectations
}

type TestExpectations struct {
	Epoch *uint64
}

func Uint64ToPtr(value uint64) *uint64 {
	return &value
}

func Uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

func BytesToUint64(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}

var _ = Describe("StressyTest", func() {
	var (
		doneC     chan struct{}
		logger    *zap.Logger
		proposals map[string]uint64
		wg        sync.WaitGroup

		network *Network
	)

	BeforeEach(func() {
		var err error
		//logger, err = zap.NewDevelopment()
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		proposals = map[string]uint64{}

		doneC = make(chan struct{})
	})

	AfterEach(func() {
		close(doneC)
		logger.Sync()
		wg.Wait()

		if CurrentGinkgoTestDescription().Failed {
			fmt.Printf("Printing state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			Expect(network).NotTo(BeNil())

			for nodeIndex, node := range network.nodes {
				status, err := node.Status(context.Background())
				if err != nil && status == nil {
					fmt.Printf("Could not get status for node %d: %s", nodeIndex, err)
				} else {
					fmt.Printf("\nStatus for node %d\n%s\n", nodeIndex, status.Pretty())
				}
				fmt.Printf("\nFakeLog has %d messages\n", len(network.fakeLogs[nodeIndex].Entries))

				if len(proposals) > len(network.fakeLogs[nodeIndex].Entries)+10 {
					fmt.Printf("Expected %d entries, but only got %d\n", len(proposals), len(network.fakeLogs[nodeIndex].Entries))
				} else if len(proposals) > len(network.fakeLogs[nodeIndex].Entries) {
					entries := map[string]struct{}{}
					for _, entry := range network.fakeLogs[0].Entries {
						entries[string(entry.Requests[0].Digest)] = struct{}{}
					}

					var missing []uint64
					for proposalKey, proposalUint := range proposals {
						if _, ok := entries[proposalKey]; !ok {
							missing = append(missing, proposalUint)
						}
					}
					sort.Slice(missing, func(i, j int) bool {
						return missing[i] < missing[j]
					})
					fmt.Printf("Missing entries for %v\n", missing)
				}
			}
		}
	})

	DescribeTable("commits all messages", func(testConfig *TestConfig) {
		network = CreateNetwork(testConfig, logger, doneC)

		network.GoRunNetwork(doneC, &wg)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		n0Proposer, err := network.nodes[0].ClientProposer(ctx, []byte("fake-client"))
		Expect(err).NotTo(HaveOccurred())

		Expect(testConfig.MsgCount).NotTo(Equal(0))
		for i := 0; i < testConfig.MsgCount; i++ {
			proposalUint, proposalBytes := testConfig.Proposer.Proposal(testConfig.NodeCount, i)

			// Unevenly propose across the nodes
			nodeID := i % 2 % testConfig.NodeCount
			if nodeID == 0 {
				err := n0Proposer.Propose(ctx, true, &pb.RequestData{
					ClientId: []byte("fake-client"),
					ReqNo:    uint64(i) + 1,
					Data:     proposalBytes,
				})
				Expect(err).NotTo(HaveOccurred())
			} else {
				node := network.nodes[i%2%testConfig.NodeCount]
				err := node.Propose(ctx, true, &pb.RequestData{
					ClientId: []byte("fake-client"),
					ReqNo:    uint64(i) + 1,
					Data:     proposalBytes,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			proposalKey := string(proposalBytes)
			Expect(proposals).NotTo(ContainElement(proposalKey))
			proposals[proposalKey] = proposalUint
		}

		observations := map[uint64]struct{}{}
		for j, fakeLog := range network.fakeLogs {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < testConfig.MsgCount {
				entry := &pb.QEntry{}
				Eventually(fakeLog.CommitC).Should(Receive(&entry))

				if testConfig.Expectations.Epoch != nil {
					Expect(entry.Epoch).To(Equal(*testConfig.Expectations.Epoch))
				}

				proposalUint, ok := proposals[string(entry.Requests[0].Digest)]
				Expect(ok).To(BeTrue())
				if testConfig.BucketCount == 0 {
					Expect(proposalUint % uint64(testConfig.NodeCount)).To(Equal((entry.SeqNo - 1) % uint64(testConfig.NodeCount)))
				}

				_, ok = observations[proposalUint]
				Expect(ok).To(BeFalse())
				observations[proposalUint] = struct{}{}
			}
		}
	},
		Entry("SingleNode greenpath", &TestConfig{
			NodeCount: 1,
			MsgCount:  1000,
			Proposer:  LinearProposer{},
			Expectations: TestExpectations{
				Epoch: Uint64ToPtr(0),
			},
		}),

		Entry("FourNodeBFT greenpath", &TestConfig{
			NodeCount: 4,
			MsgCount:  1000,
			Proposer:  LinearProposer{},
			Expectations: TestExpectations{
				Epoch: Uint64ToPtr(0),
			},
		}),

		Entry("FourNodeBFT single bucket greenpath", &TestConfig{
			NodeCount:   4,
			BucketCount: 1,
			MsgCount:    1000,
			Proposer:    LinearProposer{},
			Expectations: TestExpectations{
				Epoch: Uint64ToPtr(0),
			},
		}),
	)
})

type Network struct {
	nodes      []*mirbft.Node
	fakeLogs   []*FakeLog
	processors []*sample.SerialProcessor
}

func CreateNetwork(testConfig *TestConfig, logger *zap.Logger, doneC <-chan struct{}) *Network {
	nodes := make([]*mirbft.Node, testConfig.NodeCount)

	networkConfig := mirbft.StandardInitialNetworkConfig(testConfig.NodeCount)

	if testConfig.BucketCount != 0 {
		networkConfig.NumberOfBuckets = int32(testConfig.BucketCount)
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

		node, err := mirbft.StartNewNode(config, doneC, networkConfig)
		Expect(err).NotTo(HaveOccurred())
		nodes[i] = node
	}

	var transport Transport = &FakeTransport{
		Destinations: nodes,
	}

	for _, transportFilter := range testConfig.TransportFilters {
		transport = transportFilter(transport)
	}

	fakeLogs := make([]*FakeLog, testConfig.NodeCount)
	processors := make([]*sample.SerialProcessor, testConfig.NodeCount)
	for i, node := range nodes {
		node := node
		fakeLog := &FakeLog{
			CommitC: make(chan *pb.QEntry, testConfig.MsgCount),
		}

		fakeLogs[i] = fakeLog

		processors[i] = &sample.SerialProcessor{
			Node: node,
			Link: transport.Link(node.Config.ID),
			Validator: sample.ValidatorFunc(func(result *mirbft.Request) error {
				if result.Source != BytesToUint64(result.ClientRequest.ClientId) {
					return fmt.Errorf("mis-matched originating replica and client id")
				}
				return nil
			}),
			Hasher: func() hash.Hash { return &NoopHasher{} },
			Committer: &sample.SerialCommitter{
				Log:                    fakeLog,
				OutstandingSeqNos:      map[uint64]*mirbft.Commit{},
				OutstandingCheckpoints: map[uint64]struct{}{},
			},
		}

	}

	return &Network{
		nodes:      nodes,
		fakeLogs:   fakeLogs,
		processors: processors,
	}
}

func (n *Network) GoRunNetwork(doneC <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(len(n.nodes))
	for i := range n.nodes {
		go func(i int, doneC <-chan struct{}) {
			defer GinkgoRecover()
			defer wg.Done()

			// TODO, these tests are flaky, and non-deterministic.
			// they need to be re-written onto a single thread with determinism
			// and the crazy high-stress non-deterministic ones moved to
			// a test requiring fewer constraints on the results.
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case actions := <-n.nodes[i].Ready():
					results := n.processors[i].Process(&actions)
					n.nodes[i].AddResults(*results)
				case <-n.nodes[i].Err():
					_, err := n.nodes[i].Status(context.Background())
					if err != mirbft.ErrStopped {
						fmt.Printf("Unexpected err: %+v\n", err)
						Expect(err).NotTo(HaveOccurred())
					}
					return
				case <-ticker.C:
					n.nodes[i].Tick()
				}
			}
		}(i, doneC)
	}
}
