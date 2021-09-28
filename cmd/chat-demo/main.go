package main

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	"github.com/hyperledger-labs/mirbft/pkg/dummyclient"
	"github.com/hyperledger-labs/mirbft/pkg/grpctransport"
	"github.com/hyperledger-labs/mirbft/pkg/logger"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/ordering"
	"github.com/hyperledger-labs/mirbft/pkg/reqstore"
	"github.com/hyperledger-labs/mirbft/pkg/requestreceiver"
	"github.com/hyperledger-labs/mirbft/pkg/simplewal"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (

	// Base port number for the nodes to listen to messages from each other.
	// Each node will listen on a port computed by adding its numeric ID to nodeBasePort.
	// Note that protocol messages and requests are following two completely distinct paths to avoid interference
	// of clients with node-to-node communication.
	nodeBasePort = 10000

	// Base port number for the node request receivers to listen to messages from clients.
	// Each request receiver will listen on a port computed by adding its node's numeric ID to reqReceiverBasePort.
	// Note that protocol messages and requests are following two completely distinct paths to avoid interference
	// of clients with node-to-node communication.
	reqReceiverBasePort = 20000
)

// params represents parsed command-line parameters passed to the program.
type params struct {

	// Numeric ID of this node.
	OwnId uint64
}

func main() {

	// Parse command-line parameters.
	args := parseArgs(os.Args[1:])

	// ================================================================================
	// Generate system membership info: addresses, ports, etc...
	// ================================================================================

	// IDs of nodes that are part of the system.
	// This example uses a static configuration of 4 nodes.
	nodeIds := []uint64{0, 1, 2, 3}

	// Generate addresses and ports of participating nodes.
	// All machines are on the local machine, but listen on different port numbers.
	nodeAddrs := make(map[uint64]string)
	for _, i := range nodeIds {
		nodeAddrs[uint64(i)] = fmt.Sprintf("127.0.0.1:%d", nodeBasePort+i)
	}

	// Generate addresses and ports for receiving requests from clients.
	// Each node uses different ports for receiving protocol messages and requests.
	reqReceiverAddrs := make(map[uint64]string)
	for i := 0; i < 4; i++ {
		reqReceiverAddrs[uint64(i)] = fmt.Sprintf("127.0.0.1:%d", reqReceiverBasePort+i)
	}

	// ================================================================================
	// Create and initialize various modules used by mirbft.
	// ================================================================================

	// Initialize the write-ahead log.
	// This is where the MirBFT library will continuously persist its state
	// for the case of restarts / crash-recovery events.
	// At the time of writing this comment, restarts / crash-recovery is not yet implemented though.
	walPath := filepath.Join(".", "chat-demo-wal")
	writeAheadLog, err := simplewal.Open(walPath)
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(walPath, 0700); err != nil {
		panic(err)
	}
	defer func() {
		if err := writeAheadLog.Close(); err != nil {
			fmt.Println("Could not close write-ahead log.")
		}
	}()

	// Initialize the networking module.
	// MirBFT will use it for transporting nod-to-node messages.
	net := grpctransport.NewGrpcTransport(nodeAddrs, args.OwnId)
	if err := net.Start(); err != nil {
		panic(err)
	}
	net.Connect()

	// Create a new request store. Request payloads will be stored in it.
	// Generally, the request store should be a persistent one,
	// but for this dummy example we use a simple in-memory implementation,
	// as restarts and crash-recovery (where persistence is necessary) are not implemented yet anyway.
	reqStore := reqstore.NewVolatileRequestStore()

	// ================================================================================
	// Create a MirBFT Node, attaching the ChanApp implementation and other modules.
	// ================================================================================

	// Create a MirBFT Node, using a default configuration and passing the modules initialized just above.
	node, err := mirbft.NewNode(args.OwnId, mirbft.DefaultNodeConfig(), &modules.Modules{
		Net:          net,
		WAL:          writeAheadLog,
		RequestStore: reqStore,
		Protocol:     ordering.NewDummyProtocol(logger.ConsoleInfoLogger, nodeIds, args.OwnId),

		// This is the application logic MirBFT is going to deliver requests to.
		// It requires to have access to the request store, as MirBFT only passes request references to it.
		// It is the application's responsibility to get the necessary request data from the request store.
		// For the implementation of the application, see app.go.
		App: NewChatApp(reqStore),
	})

	// ================================================================================
	// Start the Node by establishing network connections and launching necessary processing threads
	// ================================================================================

	// Define variables to synchronize Node startup and shutdown.
	stopC := make(chan struct{}) // Closing this channel will signal the Node to stop.
	var nodeErr error            // The error returned from running the Node will be stored here.
	var wg sync.WaitGroup        // The Node will call Done() on this WaitGroup when it actually stops.
	wg.Add(1)

	// Start the node in a separate goroutine
	go func() {
		// Since the Node does not have any notion of real time,
		// it needs to be supplied with logical time in form of a Ticker.
		nodeErr = node.Run(stopC, time.NewTicker(time.Second).C)
		wg.Done()
	}()

	// Create a request receiver and start receiving requests.
	// Node that the RequestReceiver is _not_ part of the Node as its module.
	// It is external to the Node and only submits requests it receives to the node.
	reqReceiver := requestreceiver.NewRequestReceiver(node)
	if err := reqReceiver.Start(reqReceiverBasePort + int(args.OwnId)); err != nil {
		panic(err)
	}

	// ================================================================================
	// Create a dummy client for submitting requests (chat messages) to the system.
	// ================================================================================

	// Create a DummyClient. In this example, the client's ID corresponds to the ID of the node it is collocated with,
	// but in general this need not be the case.
	// Also note that the client IDs are in a different namespace than Node IDs.
	client := dummyclient.NewDummyClient(args.OwnId)

	// Create network connections to all Nodes' request receivers.
	client.Connect(reqReceiverAddrs)

	// Send 3 chat messages by submitting them as requests.
	// In this simple example we simply wait one second before sending each message,
	// but this can / will be replaced by reading the messages from stdin, so the user can type them in.
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		if err := client.SubmitRequest(
			[]byte(fmt.Sprintf("Chat message %d from client %d", i, args.OwnId)),
		); err != nil {
			panic(err)
		}
	}

	// ================================================================================
	// Shut down.
	// ================================================================================

	// After sending a few messages, we disconnect the client,
	fmt.Println("Done sending messages.")
	client.Disconnect()

	// wait a moment
	time.Sleep(3 * time.Second)

	// and stop the server.
	fmt.Println("Stopping server.")
	close(stopC)
	wg.Wait()
}

// Parses the command-line arguments and returns them in a params struct.
func parseArgs(args []string) *params {
	app := kingpin.New("chat-demo", "Small chat application to demonstrate the usage of the MirBFT library.")
	ownId := app.Arg("id", "Numeric ID of this node").Required().Uint64()

	if _, err := app.Parse(args); err != nil {
		panic(err)
	}

	return &params{
		OwnId: *ownId,
	}
}
