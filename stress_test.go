/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/IBM/mirbft/mock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/recorder"
	"github.com/IBM/mirbft/sample"

	"go.uber.org/zap"
)

type FakeLink struct {
	FakeTransport *FakeTransport
	Source        uint64
}

func (fl *FakeLink) Send(dest uint64, msg *pb.Msg) {
	fl.FakeTransport.Send(fl.Source, dest, msg)
}

type FakeTransport struct {
	// Buffers is source x dest
	Buffers [][]chan *pb.Msg
}

func NewFakeTransport(nodes int) *FakeTransport {
	buffers := make([][]chan *pb.Msg, nodes)
	for i := 0; i < nodes; i++ {
		buffers[i] = make([]chan *pb.Msg, nodes)
		for j := 0; j < nodes; j++ {
			buffers[i][j] = make(chan *pb.Msg, 10000)
		}
	}

	return &FakeTransport{
		Buffers: buffers,
	}
}

func (ft *FakeTransport) Send(source, dest uint64, msg *pb.Msg) {
	select {
	case ft.Buffers[int(source)][int(dest)] <- msg:
	default:
		fmt.Printf("Warning: Dropping message %T from %d to %d\n", msg.Type, source, dest)
	}
}

func (ft *FakeTransport) Link(source uint64) *FakeLink {
	return &FakeLink{
		Source:        source,
		FakeTransport: ft,
	}
}

type FakeLog struct {
	Entries []*pb.QEntry
	CommitC chan *pb.QEntry
}

func (fl *FakeLog) Apply(entry *pb.QEntry) {
	if len(entry.Requests) == 0 {
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
	NodeCount          int
	BucketCount        int
	MsgCount           int
	CheckpointInterval int
	BatchSize          uint32
	ClientWidth        uint32
}

func Uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
}

func BytesToUint64(value []byte) uint64 {
	return binary.LittleEndian.Uint64(value)
}

// StressyTest attempts to spin up as 'real' a network as possible, using
// fake links, but real concurrent go routines.  This means the test is non-deterministic
// so we can't make assertions about the state of the network that are as specific
// as the more general single threaded testengine type tests.  Still, there
// seems to be value in confirming that at a basic level, a concurrent network executes
// correctly.
var _ = Describe("StressyTest", func() {
	var (
		doneC     chan struct{}
		logger    *zap.Logger
		proposals map[uint64]*pb.Request
		wg        sync.WaitGroup

		network *Network
	)

	BeforeEach(func() {
		var err error
		//logger, err = zap.NewDevelopment()
		logger, err = zap.NewProduction()
		Expect(err).NotTo(HaveOccurred())

		proposals = map[uint64]*pb.Request{}

		doneC = make(chan struct{})

	})

	AfterEach(func() {
		close(doneC)
		logger.Sync()
		wg.Wait()

		if CurrentGinkgoTestDescription().Failed {
			fmt.Printf("\n\nPrinting state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)
			Expect(network).NotTo(BeNil())

			for nodeIndex, replica := range network.TestReplicas {
				fmt.Printf("\nStatus for node %d\n", nodeIndex)
				<-replica.Node.Err() // Make sure the serializer has exited
				status, err := replica.Node.Status(context.Background())
				Expect(err).To(HaveOccurred())

				if err == mirbft.ErrStopped {
					fmt.Printf("\nStopped normally\n")
				} else {
					fmt.Printf("\nStopped with error: %+v\n", err)
				}

				if status == nil {
					fmt.Printf("Could not get status for node %d: %s", nodeIndex, err)
				} else {
					fmt.Printf("%s\n", status.Pretty())
				}

				fmt.Printf("\nFakeLog has %d messages\n", len(replica.Log.Entries))

				if len(proposals) > len(replica.Log.Entries)+10 {
					fmt.Printf("Expected %d entries, but only got %d\n", len(proposals), len(replica.Log.Entries))
				} else if len(proposals) > len(replica.Log.Entries) {
					entries := map[uint64]struct{}{}
					for _, entry := range replica.Log.Entries {
						for _, req := range entry.Requests {
							entries[BytesToUint64(req.Request.Data)] = struct{}{}
						}
					}

					var missing []uint64
					for proposalID := range proposals {
						if _, ok := entries[proposalID]; !ok {
							missing = append(missing, proposalID)
						}
					}
					sort.Slice(missing, func(i, j int) bool {
						return missing[i] < missing[j]
					})

					fmt.Printf("Missing entries\n")
					for _, proposalID := range missing {
						request := proposals[proposalID]
						fmt.Printf("  ClientID=%d ReqNo=%d\n", request.ClientId, request.ReqNo)
					}
				}

				fmt.Printf("\nLog available at %s\n", replica.RecordingFile.Name())
			}

		} else {
			for _, replica := range network.TestReplicas {
				err := os.Remove(replica.RecordingFile.Name())
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})

	DescribeTable("commits all messages", func(testConfig *TestConfig) {
		ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
		defer cancel()

		network = CreateNetwork(ctx, &wg, testConfig, logger, doneC)

		network.GoRunNetwork(ctx, doneC, &wg)

		proposers := make([]*mirbft.ClientProposer, len(network.TestReplicas))
		for i, replica := range network.TestReplicas {
			var err error
			proposers[i], err = replica.Node.ClientProposer(ctx, 0)
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(testConfig.MsgCount).NotTo(Equal(0))
		proposalID := uint64(0)
		for i := 0; i < testConfig.MsgCount; i++ {
			proposalID++
			proposal := &pb.Request{
				ClientId: 0,
				ReqNo:    uint64(i),
				Data:     Uint64ToBytes(proposalID),
			}

			for _, proposer := range proposers {
				err := proposer.Propose(ctx, proposal)
				Expect(err).NotTo(HaveOccurred())
			}

			proposals[proposalID] = proposal
		}

		observations := map[uint64]struct{}{}
		for j, replica := range network.TestReplicas {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < testConfig.MsgCount {
				entry := &pb.QEntry{}
				Eventually(replica.Log.CommitC, 10*time.Second).Should(Receive(&entry))

				for _, req := range entry.Requests {
					proposalID := BytesToUint64(req.Request.Data)
					_, ok := proposals[proposalID]
					Expect(ok).To(BeTrue())

					_, ok = observations[proposalID]
					Expect(ok).To(BeFalse())
					observations[proposalID] = struct{}{}
				}
			}
		}
	},
		Entry("SingleNode greenpath", &TestConfig{
			NodeCount: 1,
			MsgCount:  1000,
		}),

		Entry("FourNodeBFT greenpath", &TestConfig{
			NodeCount:          4,
			CheckpointInterval: 20,
			MsgCount:           1000,
		}),

		Entry("FourNodeBFT single bucket greenpath", &TestConfig{
			NodeCount:          4,
			BucketCount:        1,
			CheckpointInterval: 10,
			MsgCount:           1000,
		}),

		Entry("FourNodeBFT single bucket big batch greenpath", &TestConfig{
			NodeCount:          4,
			BucketCount:        1,
			CheckpointInterval: 10,
			BatchSize:          10,
			ClientWidth:        1000,
			MsgCount:           10000,
		}),
	)
})

type TestReplica struct {
	Node          *mirbft.Node
	RecordingFile *os.File
	Log           *FakeLog
	Processor     *sample.SerialProcessor
	FakeTransport *FakeTransport
	DoneC         <-chan struct{}
}

func (tr *TestReplica) Process() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case actions := <-tr.Node.Ready():
			results := tr.Processor.Process(&actions)
			tr.Node.AddResults(*results)
		case <-tr.Node.Err():
			_, err := tr.Node.Status(context.Background())
			if err != mirbft.ErrStopped {
				return err
			}
			return nil
		case <-ticker.C:
			tr.Node.Tick()
		}
	}
}

func (tr *TestReplica) DrainRecorder() error {
	defer tr.RecordingFile.Close()
	return tr.Node.Config.EventInterceptor.(*recorder.Interceptor).Drain(tr.RecordingFile)
}

func (tr *TestReplica) DrainFrom(j int) func() error {
	return func() error {
		sourceBuffer := tr.FakeTransport.Buffers[j][int(tr.Node.Config.ID)]
		for {
			select {
			case msg := <-sourceBuffer:
				err := tr.Node.Step(context.Background(), uint64(j), msg)
				if err != nil && err != mirbft.ErrStopped {
					return err
				}
			case <-tr.DoneC:
				return nil
			}
		}
	}
}

type Network struct {
	TestReplicas []*TestReplica
}

func CreateNetwork(ctx context.Context, wg *sync.WaitGroup, testConfig *TestConfig, logger *zap.Logger, doneC <-chan struct{}) *Network {
	transport := NewFakeTransport(testConfig.NodeCount)

	networkState := mirbft.StandardInitialNetworkState(testConfig.NodeCount, 0)

	if testConfig.BucketCount != 0 {
		networkState.Config.NumberOfBuckets = int32(testConfig.BucketCount)
	}

	if testConfig.CheckpointInterval != 0 {
		networkState.Config.CheckpointInterval = int32(testConfig.CheckpointInterval)
	}

	if testConfig.ClientWidth != 0 {
		for _, client := range networkState.Clients {
			client.Width = testConfig.ClientWidth
		}
	}

	replicas := make([]*TestReplica, testConfig.NodeCount)

	startTime := time.Now()
	for i := range replicas {
		config := &mirbft.Config{
			ID:                   uint64(i),
			Logger:               logger.Named(fmt.Sprintf("node%d", i)),
			BatchSize:            1,
			SuspectTicks:         4,
			HeartbeatTicks:       2,
			NewEpochTimeoutTicks: 8,
			BufferSize:           500,
			EventInterceptor: recorder.NewInterceptor(
				uint64(i),
				func() int64 {
					return time.Since(startTime).Milliseconds()
				},
				10000,
				doneC,
			),
		}

		if testConfig.BatchSize != 0 {
			config.BatchSize = testConfig.BatchSize
		}

		recordingFile, err := ioutil.TempFile("", fmt.Sprintf("stressy.%d-*.eventlog", i))
		Expect(err).NotTo(HaveOccurred())

		storage := &mock.Storage{}
		storage.LoadReturnsOnCall(0, &pb.Persistent{
			Type: &pb.Persistent_CEntry{
				CEntry: &pb.CEntry{
					SeqNo:           0,
					CheckpointValue: []byte("fake-initial-value"),
					NetworkState:    networkState,
					EpochConfig: &pb.EpochConfig{
						Number:            0,
						Leaders:           networkState.Config.Nodes,
						PlannedExpiration: 0,
					},
				},
			},
		}, nil)
		storage.LoadReturnsOnCall(1, &pb.Persistent{
			Type: &pb.Persistent_EpochChange{
				EpochChange: &pb.EpochChange{
					NewEpoch: 1,
					Checkpoints: []*pb.Checkpoint{
						{
							SeqNo: 0,
							Value: []byte("fake-initial-value"),
						},
					},
				},
			},
		}, nil)

		storage.LoadReturnsOnCall(2, nil, io.EOF)

		node, err := mirbft.StartNode(config, doneC, storage)
		Expect(err).NotTo(HaveOccurred())

		fakeLog := &FakeLog{
			// We make the CommitC excessive, to prevent deadlock
			// in case of bugs this test would otherwise catch.
			CommitC: make(chan *pb.QEntry, 5*testConfig.MsgCount),
		}

		replicas[i] = &TestReplica{
			Node:          node,
			RecordingFile: recordingFile,
			Log:           fakeLog,
			FakeTransport: transport,
			Processor: &sample.SerialProcessor{
				Node:   node,
				Link:   transport.Link(node.Config.ID),
				Hasher: sha256.New,
				Log:    fakeLog,
			},
			DoneC: doneC,
		}
	}

	return &Network{
		TestReplicas: replicas,
	}
}

func (n *Network) GoRunNetwork(context context.Context, doneC <-chan struct{}, wg *sync.WaitGroup) {
	goWork := func(i int, desc string, work func() error) {
		wg.Add(1)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			defer fmt.Printf("Node %d: Shutting down go routine %s\n", i, desc)
			err := work()
			if err != nil {
				fmt.Printf("Node %d: Error performing work %s\n", i, desc)
			}
		}()
	}

	for i, testReplica := range n.TestReplicas {
		goWork(i, "Process", testReplica.Process)
		goWork(i, "DrainRecorder", testReplica.DrainRecorder)
		for j := range n.TestReplicas {
			goWork(i, fmt.Sprintf("DrainTransportFrom_%d", j), testReplica.DrainFrom(j))
		}
	}
}
