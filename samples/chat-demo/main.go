/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// ********************************************************************************
//       Chat demo application for demonstrating the usage of MirBFT             //
//                             (main executable)                                 //
//                                                                               //
//                     Run with --help flag for usage info.                      //
// ********************************************************************************

package main

import (
	"bufio"
	"context"
	"crypto"
	"fmt"
	"github.com/hyperledger-labs/mirbft"
	mirCrypto "github.com/hyperledger-labs/mirbft/pkg/crypto"
	"github.com/hyperledger-labs/mirbft/pkg/dummyclient"
	"github.com/hyperledger-labs/mirbft/pkg/grpctransport"
	"github.com/hyperledger-labs/mirbft/pkg/iss"
	"github.com/hyperledger-labs/mirbft/pkg/logging"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/reqstore"
	"github.com/hyperledger-labs/mirbft/pkg/requestreceiver"
	"github.com/hyperledger-labs/mirbft/pkg/simplewal"
	t "github.com/hyperledger-labs/mirbft/pkg/types"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"path"
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

// parsedArgs represents parsed command-line parameters passed to the program.
type parsedArgs struct {

	// Numeric ID of this node.
	// The package github.com/hyperledger-labs/mirbft/pkg/types defines this and other types used by the library.
	OwnId t.NodeID

	// If set, print verbose output to stdout.
	Verbose bool
}

func main() {

	// Parse command-line parameters.
	_ = parseArgs(os.Args)
	args := parseArgs(os.Args)

	// Initialize logger that will be used throughout the code to print log messages.
	var logger logging.Logger
	if args.Verbose {
		logger = logging.ConsoleDebugLogger // Print debug-level info in verbose mode.
	} else {
		logger = logging.ConsoleWarnLogger // Only print errors and warnings by default.
	}

	fmt.Println("Initializing...")

	// ================================================================================
	// Generate system membership info: addresses, ports, etc...
	// ================================================================================

	// IDs of nodes that are part of the system.
	// This example uses a static configuration of 4 nodes.
	nodeIds := []t.NodeID{0, 1, 2, 3}

	// Generate addresses and ports of participating nodes.
	// All nodes are on the local machine, but listen on different port numbers.
	// Change this or make this configurable do deploy different nodes on different physical machines.
	nodeAddrs := make(map[t.NodeID]string)
	for _, i := range nodeIds {
		nodeAddrs[i] = fmt.Sprintf("127.0.0.1:%d", nodeBasePort+i)
	}

	// Generate addresses and ports for client request receivers.
	// Each node uses different ports for receiving protocol messages and requests.
	// These addresses will be used by the client code to know where to send its requests
	// (each client sends its requests to all request receivers). Each request receiver,
	// however, will only submit the received requests to its associated Node.
	reqReceiverAddrs := make(map[t.NodeID]string)
	for _, i := range nodeIds {
		reqReceiverAddrs[i] = fmt.Sprintf("127.0.0.1:%d", reqReceiverBasePort+i)
	}

	// ================================================================================
	// Create and initialize various modules used by mirbft.
	// ================================================================================

	// Initialize the write-ahead log.
	// This is where the MirBFT library will continuously persist its state
	// for the case of restarts / crash-recovery events.
	// At the time of writing this comment, restarts / crash-recovery is not yet implemented though.
	// Nevertheless, running this code will create a directory with the WAL file in it.
	// Those need to be manually removed.
	walPath := path.Join("chat-demo-wal", fmt.Sprintf("%d", args.OwnId))
	wal, err := simplewal.Open(walPath)
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(walPath, 0700); err != nil {
		panic(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			fmt.Println("Could not close write-ahead log.")
		}
	}()

	// Initialize the networking module.
	// MirBFT will use it for transporting nod-to-node messages.
	net := grpctransport.NewGrpcTransport(nodeAddrs, args.OwnId, nil)
	if err := net.Start(); err != nil {
		panic(err)
	}
	net.Connect()

	// Create a new request store. Request payloads will be stored in it.
	// Generally, the request store should be a persistent one,
	// but for this dummy example we use a simple in-memory implementation,
	// as restarts and crash-recovery (where persistence is necessary) are not yet implemented anyway.
	reqStore := reqstore.NewVolatileRequestStore()

	// Instantiate the ISS protocol module with default configuration.
	issConfig := iss.DefaultConfig(nodeIds)
	issProtocol, err := iss.New(args.OwnId, issConfig, logger)
	if err != nil {
		panic(fmt.Errorf("could not instantiate ISS protocol module: %w", err))
	}

	// ================================================================================
	// Create a MirBFT Node, attaching the ChatApp implementation and other modules.
	// ================================================================================

	// Create a MirBFT Node, using a default configuration and passing the modules initialized just above.
	node, err := mirbft.NewNode(args.OwnId, &mirbft.NodeConfig{Logger: logger}, &modules.Modules{
		Net:          net,
		WAL:          wal,
		RequestStore: reqStore,
		Protocol:     issProtocol,

		// This is the application logic MirBFT is going to deliver requests to.
		// It requires to have access to the request store, as MirBFT only passes request references to it.
		// It is the application's responsibility to get the necessary request data from the request store.
		// For the implementation of the application, see app.go.
		App: NewChatApp(reqStore),

		// Use dummy crypto module that only produces signatures
		// consisting of a single zero byte and treats those signatures as valid.
		// TODO: Remove this line once a default crypto implementation is provided by MirBFT.
		Crypto: &mirCrypto.DummyCrypto{DummySig: []byte{0}},
	})

	// Exit immediately if Node could not be created.
	if err != nil {
		fmt.Printf("Could not create node: %v\n", err)
		os.Exit(1)
	}

	// ================================================================================
	// Start the Node by establishing network connections and launching necessary processing threads
	// ================================================================================

	// Initialize variables to synchronize Node startup and shutdown.
	stopC := make(chan struct{}) // Closing this channel will signal the Node to stop.
	var nodeErr error            // The error returned from running the Node will be stored here.
	var wg sync.WaitGroup        // The Node will call Done() on this WaitGroup when it actually stops.
	wg.Add(1)

	// Start the node in a separate goroutine
	go func() {
		// Since the Node does not have any notion of real time,
		// it needs to be supplied with logical time in form of a Ticker.
		nodeErr = node.Run(stopC, time.NewTicker(100*time.Millisecond).C)
		wg.Done()
	}()

	// Create a request receiver and start receiving requests.
	// Note that the RequestReceiver is _not_ part of the Node as its module.
	// It is external to the Node and only submits requests it receives to the node.
	reqReceiver := requestreceiver.NewRequestReceiver(node, logger)
	if err := reqReceiver.Start(reqReceiverBasePort + int(args.OwnId)); err != nil {
		panic(err)
	}

	// ================================================================================
	// Create a dummy client for submitting requests (chat messages) to the system.
	// ================================================================================

	// Create a DummyClient. In this example, the client's ID corresponds to the ID of the node it is collocated with,
	// but in general this need not be the case.
	// Also note that the client IDs are in a different namespace than Node IDs.
	// The client also needs to be initialized with a Hasher and Crypto module in order to be able to sign requests.
	// We use a dummy Crypto module set up the same way as the Node's Crypto module,
	// so the client's signatures are acceptedl.
	client := dummyclient.NewDummyClient(
		t.ClientID(args.OwnId),
		crypto.SHA256,
		&mirCrypto.DummyCrypto{DummySig: []byte{0}},
		logger,
	)

	// Create network connections to all Nodes' request receivers.
	// We use just the background context in this demo app, expecting that the connection will succeed
	// and the Connect() function will return. In a real deployment, the passed context
	// can be used for failure handling, for example to cancel connecting.
	client.Connect(context.Background(), reqReceiverAddrs)

	// ================================================================================
	// Read chat messages from stdin and submit them as requests.
	// ================================================================================

	scanner := bufio.NewScanner(os.Stdin)

	// Prompt for chat message input.
	fmt.Println("Type in your messages and press 'Enter' to send.")

	// Read chat message from stdin.
	for scanner.Scan() {

		// Submit the chat message as request payload.
		if err := client.SubmitRequest(
			scanner.Bytes(),
		); err != nil {
			fmt.Println(err)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}

	// ================================================================================
	// Shut down.
	// ================================================================================

	// After sending a few messages, we disconnect the client,
	if args.Verbose {
		fmt.Println("Done sending messages.")
	}
	client.Disconnect()

	// stop the request receiver,
	reqReceiver.Stop()

	// stop the server,
	if args.Verbose {
		fmt.Println("Stopping server.")
	}
	close(stopC)
	wg.Wait()

	// and print the error returned by the stopped node.
	fmt.Printf("Node error: %v\n", nodeErr)
}

// Parses the command-line arguments and returns them in a params struct.
func parseArgs(args []string) *parsedArgs {
	app := kingpin.New("chat-demo", "Small chat application to demonstrate the usage of the MirBFT library.")
	verbose := app.Flag("verbose", "Verbose mode.").Short('v').Bool()
	// Currently the type of the node ID is defined as uint64 by the /pkg/types package.
	// In case that changes, this line will need to be updated.
	ownId := app.Arg("id", "Numeric ID of this node").Required().Uint64()

	if _, err := app.Parse(args[1:]); err != nil { // Skip args[0], which is the name of the program, not an argument.
		app.FatalUsage("could not parse arguments: %v\n", err)
	}

	return &parsedArgs{
		OwnId:   t.NodeID(*ownId),
		Verbose: *verbose,
	}
}
