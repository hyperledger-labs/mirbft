/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft_test

import (
	"context"
	"crypto"
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
	"github.com/IBM/mirbft/pkg/eventlog"
	"github.com/IBM/mirbft/pkg/pb/msgs"
	"github.com/IBM/mirbft/pkg/reqstore"
	"github.com/IBM/mirbft/pkg/simplewal"
	"github.com/IBM/mirbft/pkg/statemachine"
	"github.com/IBM/mirbft/pkg/status"
)

var (
	tickInterval = 100 * time.Millisecond
	testTimeout  = 10 * time.Second
)

func init() {
	val := os.Getenv("MIRBFT_TEST_STRESS_TICK_INTERVAL")
	if val != "" {
		dur, err := time.ParseDuration(val)
		if err != nil {
			fmt.Printf("Could not parse duration for stress tick interval: %s\n", err)
			return
		}
		fmt.Printf("Setting tick interval to be %v\n", dur)
		tickInterval = dur
	}

	val = os.Getenv("MIRBFT_TEST_STRESS_TEST_TIMEOUT")
	if val != "" {
		dur, err := time.ParseDuration(val)
		if err != nil {
			fmt.Printf("Could not parse duration for stress tick interval: %s\n", err)
			return
		}
		fmt.Printf("Setting test timeout to be %v\n", dur)
		testTimeout = dur
	}
}

type FakeClient struct {
	MsgCount uint64
}

type SourceMsg struct {
	Source uint64
	Msg    *msgs.Msg
}

type FakeLink struct {
	FakeTransport *FakeTransport
	Source        uint64
}

func (fl *FakeLink) Send(dest uint64, msg *msgs.Msg) {
	fl.FakeTransport.Send(fl.Source, dest, msg)
}

type FakeTransport struct {
	// Buffers is source x dest
	Buffers   [][]chan *msgs.Msg
	NodeSinks []chan SourceMsg
	WaitGroup sync.WaitGroup
	DoneC     chan struct{}
}

func NewFakeTransport(nodes int) *FakeTransport {
	buffers := make([][]chan *msgs.Msg, nodes)
	nodeSinks := make([]chan SourceMsg, nodes)
	for i := 0; i < nodes; i++ {
		buffers[i] = make([]chan *msgs.Msg, nodes)
		for j := 0; j < nodes; j++ {
			if i == j {
				continue
			}
			buffers[i][j] = make(chan *msgs.Msg, 10000)
		}
		nodeSinks[i] = make(chan SourceMsg)
	}

	return &FakeTransport{
		Buffers:   buffers,
		NodeSinks: nodeSinks,
		DoneC:     make(chan struct{}),
	}
}

func (ft *FakeTransport) Send(source, dest uint64, msg *msgs.Msg) {
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
			go func(i, j int, buffer chan *msgs.Msg) {
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

type FakeApp struct {
	Entries []*msgs.QEntry
	CommitC chan *msgs.QEntry
}

func (fl *FakeApp) Apply(entry *msgs.QEntry) error {
	if len(entry.Requests) == 0 {
		// this is a no-op batch from a tick, or catchup, ignore it
		return nil
	}
	fl.Entries = append(fl.Entries, entry)
	fl.CommitC <- entry
	return nil
}

func (fl *FakeApp) Snap(*msgs.NetworkState_Config, []*msgs.NetworkState_Client) ([]byte, []*msgs.Reconfiguration, error) {
	return Uint64ToBytes(uint64(len(fl.Entries))), nil, nil
}

func (fl *FakeApp) TransferTo(seqNo uint64, snap []byte) (*msgs.NetworkState, error) {
	return nil, fmt.Errorf("we don't support state transfer in this test (yet)")

}

type TestConfig struct {
	NodeCount          int
	BucketCount        int
	MsgCount           int
	CheckpointInterval int
	BatchSize          uint32
	ClientWidth        uint32
	ParallelProcess    bool
}

func Uint64ToBytes(value uint64) []byte {
	byteValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteValue, value)
	return byteValue
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
		expectedProposalCount int
		proposals             map[uint64]*msgs.Request
		wg                    sync.WaitGroup
		nodeStatusesC         chan []*NodeStatus

		network *Network
	)

	BeforeEach(func() {
		proposals = map[uint64]*msgs.Request{}

		doneC = make(chan struct{})

	})

	AfterEach(func() {
		close(doneC)
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

			fmt.Printf("\nFakeApp has %d messages\n", len(replica.App.Entries))

			if expectedProposalCount > len(replica.App.Entries) {
				fmt.Printf("Expected %d entries, but only got %d\n", len(proposals), len(replica.App.Entries))
			}

			fmt.Printf("\nLog available at %s\n", replica.EventLogPath())
		}

	})

	DescribeTable("commits all messages", func(testConfig *TestConfig) {
		nodeStatusesC = make(chan []*NodeStatus, 1)
		network = CreateNetwork(testConfig, doneC)
		go func() {
			nodeStatusesC <- network.Run()
		}()

		observations := map[uint64]struct{}{}
		for j, replica := range network.TestReplicas {
			By(fmt.Sprintf("checking for node %d that each message only commits once", j))
			for len(observations) < testConfig.MsgCount {
				entry := &msgs.QEntry{}
				Eventually(replica.App.CommitC, 10*time.Second).Should(Receive(&entry))

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
			// ParallelProcess:    true, // TODO, re-enable once parallel processing exists again
		}),
	)
})

type TestReplica struct {
	Config              *mirbft.Config
	InitialNetworkState *msgs.NetworkState
	TmpDir              string
	App                 *FakeApp
	FakeTransport       *FakeTransport
	FakeClient          *FakeClient
	ParallelProcess     bool
	DoneC               <-chan struct{}
}

func (tr *TestReplica) EventLogPath() string {
	return filepath.Join(tr.TmpDir, "eventlog.gz")
}

func clientReq(clientID, reqNo uint64) []byte {
	res := make([]byte, 16)
	binary.BigEndian.PutUint64(res, clientID)
	binary.BigEndian.PutUint64(res[8:], reqNo)
	return res
}

func (tr *TestReplica) Run() (*status.StateMachine, error) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	reqStorePath := filepath.Join(tr.TmpDir, "reqstore")
	err := os.MkdirAll(reqStorePath, 0700)
	Expect(err).NotTo(HaveOccurred())

	walPath := filepath.Join(tr.TmpDir, "wal")
	err = os.MkdirAll(walPath, 0700)
	Expect(err).NotTo(HaveOccurred())

	file, err := os.Create(tr.EventLogPath())
	Expect(err).NotTo(HaveOccurred())
	defer file.Close()

	interceptor := eventlog.NewRecorder(tr.Config.ID, file)
	defer func() {
		err := interceptor.Stop()
		Expect(err).NotTo(HaveOccurred())
	}()
	tr.Config.EventInterceptor = interceptor // XXX a hack, get rid of it

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
		defer GinkgoRecover()
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
		NodeID: node.Config.ID,
		Link:   tr.FakeTransport.Link(node.Config.ID),
		Hasher: crypto.SHA256,
		App:    tr.App,
		WAL:    wal,
	}

	clientProcessor := &mirbft.ClientProcessor{
		NodeID:       node.Config.ID,
		RequestStore: reqStore,
		Hasher:       crypto.SHA256,
	}

	expectedProposalCount := tr.FakeClient.MsgCount
	Expect(expectedProposalCount).NotTo(Equal(0))

	clientDoneC := make(chan struct{})
	defer func() {
		<-clientDoneC
	}()
	go func() {
		defer GinkgoRecover()
		defer close(clientDoneC)
		client := clientProcessor.Client(0)
		for {
			nextReqNo, err := client.NextReqNo()
			if err == mirbft.ErrClientNotExist {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if nextReqNo == tr.FakeClient.MsgCount {
				return
			}

			// Batch them in, 50 at a time
			for i := nextReqNo; i < tr.FakeClient.MsgCount && i < nextReqNo+50; i++ {
				err := client.Propose(i, clientReq(0, i))
				if err != nil {
					// TODO, failing on err causes flakes in the teardown,
					// so just returning for now, we should address later
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	// TODO, don't pre-allocate all of the requests, do it in the go routine
	go func() {
		for {
			var err error
			select {
			case clientActions := <-node.ClientReady():
				var clientResults *mirbft.ClientActionResults
				clientResults, err = clientProcessor.Process(&clientActions)
				if err != nil {
					break
				}
				err = node.AddClientResults(*clientResults)
			case <-clientProcessor.ClientWork.Ready():
				err = node.AddClientResults(*clientProcessor.ClientWork.Results())
			case <-node.Err():
				return
			case <-tr.DoneC:
				return
			}

			if err != nil {
				select {
				case <-time.After(10 * time.Second):
					// Odds are we're just shutting down, but if not
					// make a scene.
					panic(err)
				case <-node.Err():
					return
				case <-tr.DoneC:
					return
				}
			}
		}
	}()

	var process func(*statemachine.ActionList) (*statemachine.EventList, error)

	Expect(tr.ParallelProcess).To(BeFalse())
	process = processor.Process

	for {
		select {
		case actions := <-node.Actions():
			events, err := process(actions)
			Expect(err).NotTo(HaveOccurred())
			node.InjectEvents(events)
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

func CreateNetwork(testConfig *TestConfig, doneC <-chan struct{}) *Network {
	transport := NewFakeTransport(testConfig.NodeCount)

	networkState := mirbft.StandardInitialNetworkState(testConfig.NodeCount, 1)

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

	for i := range replicas {
		config := &mirbft.Config{
			ID:                   uint64(i),
			BatchSize:            1,
			SuspectTicks:         4,
			HeartbeatTicks:       2,
			NewEpochTimeoutTicks: 8,
			BufferSize:           5 * 1024 * 1024, // 5 MB
			Logger:               mirbft.ConsoleWarnLogger,
		}

		if testConfig.BatchSize != 0 {
			config.BatchSize = testConfig.BatchSize
		}

		fakeApp := &FakeApp{
			// We make the CommitC excessive, to prevent deadlock
			// in case of bugs this test would otherwise catch.
			CommitC: make(chan *msgs.QEntry, 5*testConfig.MsgCount),
		}

		replicas[i] = &TestReplica{
			Config:              config,
			InitialNetworkState: networkState,
			TmpDir:              filepath.Join(tmpDir, fmt.Sprintf("node%d", i)),
			App:                 fakeApp,
			FakeTransport:       transport,
			FakeClient: &FakeClient{
				MsgCount: uint64(testConfig.MsgCount),
			},
			ParallelProcess: testConfig.ParallelProcess,
			DoneC:           doneC,
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
				fmt.Printf("Node %d: shutting down\n", i)
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
