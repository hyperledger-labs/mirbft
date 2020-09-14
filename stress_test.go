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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/IBM/mirbft"
	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/IBM/mirbft/recorder"
	"github.com/IBM/mirbft/reqstore"
	"github.com/IBM/mirbft/simplewal"
	"github.com/IBM/mirbft/status"

	"go.uber.org/zap"
)

type FakeClient struct {
	MsgCount uint64
}

type SourceMsg struct {
	Source uint64
	Msg    *pb.Msg
}

type FakeLink struct {
	FakeTransport *FakeTransport
	Source        uint64
}

func (fl *FakeLink) Send(dest uint64, msg *pb.Msg) {
	fl.FakeTransport.Send(fl.Source, dest, msg)
}

type FakeTransport struct {
	// Buffers is source x dest
	Buffers   [][]chan *pb.Msg
	NodeSinks []chan SourceMsg
	WaitGroup sync.WaitGroup
	DoneC     chan struct{}
}

func NewFakeTransport(nodes int) *FakeTransport {
	buffers := make([][]chan *pb.Msg, nodes)
	nodeSinks := make([]chan SourceMsg, nodes)
	for i := 0; i < nodes; i++ {
		buffers[i] = make([]chan *pb.Msg, nodes)
		for j := 0; j < nodes; j++ {
			if i == j {
				continue
			}
			buffers[i][j] = make(chan *pb.Msg, 10000)
		}
		nodeSinks[i] = make(chan SourceMsg)
	}

	return &FakeTransport{
		Buffers:   buffers,
		NodeSinks: nodeSinks,
		DoneC:     make(chan struct{}),
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

func (ft *FakeTransport) RecvC(dest uint64) <-chan SourceMsg {
	return ft.NodeSinks[int(dest)]
}

func (ft *FakeTransport) Start() {
	for i, sourceBuffers := range ft.Buffers {
		for j, buffer := range sourceBuffers {
			if i == j {
				continue
			}

			ft.WaitGroup.Add(1)
			go func(i, j int, buffer chan *pb.Msg) {
				// fmt.Printf("Starting drain thread from %d to %d\n", i, j)
				defer ft.WaitGroup.Done()
				for {
					select {
					case msg := <-buffer:
						// fmt.Printf("Sending message from %d to %d\n", i, j)
						select {
						case ft.NodeSinks[j] <- SourceMsg{
							Source: uint64(i),
							Msg:    msg,
						}:
						case <-ft.DoneC:
							return
						}
					case <-ft.DoneC:
						return
					}
				}
			}(i, j, buffer)
		}
	}
}

func (ft *FakeTransport) Stop() {
	close(ft.DoneC)
	ft.WaitGroup.Wait()
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
		doneC                 chan struct{}
		logger                *zap.Logger
		expectedProposalCount int
		proposals             map[uint64]*pb.Request
		wg                    sync.WaitGroup
		nodeStatusesC         chan []*NodeStatus

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

		if nodeStatusesC == nil {
			fmt.Printf("Unexpected network status is nil, skipping status!\n")
			return
		}

		nodeStatuses := <-nodeStatusesC

		if !CurrentGinkgoTestDescription().Failed {
			for _, replica := range network.TestReplicas {
				os.RemoveAll(replica.TmpDir)
			}
			return
		}

		fmt.Printf("\n\nPrinting state machine status because of failed test in %s\n", CurrentGinkgoTestDescription().TestText)

		for nodeIndex, replica := range network.TestReplicas {
			fmt.Printf("\nStatus for node %d\n", nodeIndex)
			nodeStatus := nodeStatuses[nodeIndex]

			if nodeStatus.ExitErr == mirbft.ErrStopped {
				fmt.Printf("\nStopped normally\n")
			} else {
				fmt.Printf("\nStopped with error: %+v\n", nodeStatus.ExitErr)
			}

			if nodeStatus.Status == nil {
				fmt.Printf("Could not get status for node %d", nodeIndex)
			} else {
				fmt.Printf("%s\n", nodeStatus.Status.Pretty())
			}

			fmt.Printf("\nFakeLog has %d messages\n", len(replica.Log.Entries))

			if expectedProposalCount > len(replica.Log.Entries) {
				fmt.Printf("Expected %d entries, but only got %d\n", len(proposals), len(replica.Log.Entries))
			}

			fmt.Printf("\nLog available at %s\n", filepath.Join(replica.TmpDir, "recording.eventlog"))
		}

	})

	DescribeTable("commits all messages", func(testConfig *TestConfig) {
		nodeStatusesC = make(chan []*NodeStatus, 1)
		network = CreateNetwork(testConfig, logger, doneC)
		go func() {
			nodeStatusesC <- network.Run()
		}()

		observations := map[uint64]struct{}{}
		for j, replica := range network.TestReplicas {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < testConfig.MsgCount {
				entry := &pb.QEntry{}
				Eventually(replica.Log.CommitC, 10*time.Second).Should(Receive(&entry))

				for _, req := range entry.Requests {
					Expect(req.ReqNo).To(BeNumerically("<", testConfig.MsgCount))
					_, ok := observations[req.ReqNo]
					Expect(ok).To(BeFalse())
					observations[req.ReqNo] = struct{}{}
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
	Config              *mirbft.Config
	InitialNetworkState *pb.NetworkState
	TmpDir              string
	Log                 *FakeLog
	FakeTransport       *FakeTransport
	FakeClient          *FakeClient
	DoneC               <-chan struct{}
}

func (tr *TestReplica) Run() (*status.StateMachine, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	reqStorePath := filepath.Join(tr.TmpDir, "reqstore")
	err := os.MkdirAll(reqStorePath, 0700)
	Expect(err).NotTo(HaveOccurred())

	walPath := filepath.Join(tr.TmpDir, "wal")
	err = os.MkdirAll(walPath, 0700)
	Expect(err).NotTo(HaveOccurred())

	wal, err := simplewal.Open(walPath)
	Expect(err).NotTo(HaveOccurred())
	defer wal.Close()

	reqStore, err := reqstore.Open(reqStorePath)
	Expect(err).NotTo(HaveOccurred())
	defer reqStore.Close()

	node, err := mirbft.StartNewNode(tr.Config, tr.InitialNetworkState, []byte("fake-application-state"))
	Expect(err).NotTo(HaveOccurred())
	defer node.Stop()

	linkDoneC := make(chan struct{})
	go func() {
		defer close(linkDoneC)
		recvC := tr.FakeTransport.RecvC(node.Config.ID)
		for {
			select {
			case sourceMsg := <-recvC:
				// fmt.Printf("Stepping message from %d to %d\n", sourceMsg.Source, node.Config.ID)
				err := node.Step(context.Background(), sourceMsg.Source, sourceMsg.Msg)
				if err == mirbft.ErrStopped {
					return
				}
				Expect(err).NotTo(HaveOccurred())
			case <-tr.DoneC:
				return
			}
		}
	}()
	defer func() {
		<-linkDoneC
	}()

	processor := &mirbft.Processor{
		Node:         node,
		Link:         tr.FakeTransport.Link(node.Config.ID),
		Hasher:       sha256.New,
		Log:          tr.Log,
		RequestStore: reqStore,
		WAL:          wal,
	}
	// processor.Start()
	// defer processor.Stop()

	recordingFile := filepath.Join(tr.TmpDir, "recording.eventlog")
	recorderDoneC := make(chan struct{})
	recording, err := os.Create(recordingFile)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		node.Config.EventInterceptor.(*recorder.Interceptor).Drain(recording)
		close(recorderDoneC)
	}()
	defer func() {
		<-recorderDoneC
		recording.Close()
	}()

	proposer, err := node.ClientProposer(context.Background(), 0)
	Expect(err).NotTo(HaveOccurred())

	expectedProposalCount := tr.FakeClient.MsgCount
	Expect(expectedProposalCount).NotTo(Equal(0))

	go func() {
		defer GinkgoRecover()
		for i := uint64(0); i < expectedProposalCount; i++ {
			proposal := &pb.Request{
				ClientId: 0,
				ReqNo:    i,
				Data:     Uint64ToBytes(i),
			}

			err := proposer.Propose(context.Background(), proposal)
			Expect(err).NotTo(HaveOccurred())
		}
	}()

	for {
		select {
		case actions := <-node.Ready():
			results := mirbft.ProcessSerially(&actions, processor) // , tr.DoneC)
			node.AddResults(*results)
		case <-node.Err():
			return node.Status(context.Background())
		case <-ticker.C:
			node.Tick()
		case <-tr.DoneC:
			node.Stop()
			return node.Status(context.Background())
		}
	}
}

type Network struct {
	Transport    *FakeTransport
	TestReplicas []*TestReplica
}

type NodeStatus struct {
	Status  *status.StateMachine
	ExitErr error
}

func CreateNetwork(testConfig *TestConfig, logger *zap.Logger, doneC <-chan struct{}) *Network {
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

	tmpDir, err := ioutil.TempDir("", "stress_test.*")
	Expect(err).NotTo(HaveOccurred())

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

		fakeLog := &FakeLog{
			// We make the CommitC excessive, to prevent deadlock
			// in case of bugs this test would otherwise catch.
			CommitC: make(chan *pb.QEntry, 5*testConfig.MsgCount),
		}

		replicas[i] = &TestReplica{
			Config:              config,
			InitialNetworkState: networkState,
			TmpDir:              filepath.Join(tmpDir, fmt.Sprintf("node%d", i)),
			Log:                 fakeLog,
			FakeTransport:       transport,
			FakeClient: &FakeClient{
				MsgCount: uint64(testConfig.MsgCount),
			},
			DoneC: doneC,
		}
	}

	return &Network{
		Transport:    transport,
		TestReplicas: replicas,
	}
}

func (n *Network) Run() []*NodeStatus {
	result := make([]*NodeStatus, len(n.TestReplicas))
	var wg sync.WaitGroup

	// Start the Mir nodes
	for i, testReplica := range n.TestReplicas {
		nodeStatus := &NodeStatus{}
		result[i] = nodeStatus
		wg.Add(1)
		go func(i int, testReplica *TestReplica) {
			defer GinkgoRecover()
			defer wg.Done()
			defer func() {
				fmt.Printf("Node %d: shutting down", i)
				if r := recover(); r != nil {
					fmt.Printf("  Node %d: received panic %s\n%s\n", i, r, debug.Stack())
					panic(r)
				}
			}()

			fmt.Printf("Node %d: running\n", i)
			status, err := testReplica.Run()
			fmt.Printf("Node %d: exit with exitErr=%v\n", i, err)
			nodeStatus.Status, nodeStatus.ExitErr = status, err
		}(i, testReplica)
	}

	n.Transport.Start()
	defer n.Transport.Stop()

	wg.Wait()

	fmt.Printf("All go routines shut down\n")
	return result
}
