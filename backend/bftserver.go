/*
Copyright IBM Corp. 2021 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backend

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"os"

	"github.com/IBM/mirbft/config"
	"github.com/IBM/mirbft/connection"
	"github.com/IBM/mirbft/crypto"
	"github.com/IBM/mirbft/mir"
	"github.com/IBM/mirbft/persist"
	"github.com/IBM/mirbft/tracing"
	"github.com/op/go-logging"
	"time"
)

const numRequestCount = 0

var log = logging.MustGetLogger("bftserver")
var format = logging.MustStringFormatter(
	`%{time:2006/01/02 15:04:05.000000} %{shortfunc} %{message}`,
)

func fatal(err error) {
	if err != nil {
		log.Critical(err)
	}
}

func getHost(address string) string {
	spl := strings.Split(address, ":")
	if len(spl) != 2 {
		panic("Wrongly formatted address")
	}
	return spl[0]
}

func getPort(address string) string {
	spl := strings.Split(address, ":")
	if len(spl) != 2 {
		panic("Wrongly formatted address")
	}
	return spl[1]
}

func getAddress(host string, port string) string {
	return host + ":" + port
}

func incrementPort(port string, increment uint64) string {
	u, _ := strconv.ParseUint(port, 0, 64)
	u = u + increment
	return strconv.FormatUint(u, 10)
}

func StartServer(outFilePrefix string, txNo int) {

	// Set basic server parameters
	N := config.Config.N
	D := config.Config.D
	id := config.Config.Id

	// Set up logging
	logBackend := logging.NewLogBackend(os.Stdout, "", 0)
	backendFormatter := logging.NewBackendFormatter(logBackend, format)
	logging.SetBackend(backendFormatter)

	if config.Config.Logging == "error" {
		logging.SetLevel(logging.ERROR, "server-main")
		logging.SetLevel(logging.ERROR, "tracing")
		logging.SetLevel(logging.ERROR, "sbft")
		logging.SetLevel(logging.ERROR, "connection")
		logging.SetLevel(logging.ERROR, "bftserver")
		logging.SetLevel(logging.ERROR, "config")
	}
	if config.Config.Logging == "critical" {
		logging.SetLevel(logging.CRITICAL, "server-main")
		logging.SetLevel(logging.CRITICAL, "tracing")
		logging.SetLevel(logging.CRITICAL, "sbft")
		logging.SetLevel(logging.CRITICAL, "connection")
		logging.SetLevel(logging.CRITICAL, "bftserver")
		logging.SetLevel(logging.CRITICAL, "config")
	}
	if config.Config.Logging == "info" {
		logging.SetLevel(logging.INFO, "server-main")
		logging.SetLevel(logging.INFO, "tracing")
		logging.SetLevel(logging.INFO, "sbft")
		logging.SetLevel(logging.INFO, "connection")
		logging.SetLevel(logging.INFO, "bftserver")
		logging.SetLevel(logging.INFO, "config")

	}
	if config.Config.Logging == "debug" {
		logging.SetLevel(logging.DEBUG, "server-main")
		logging.SetLevel(logging.DEBUG, "tracing")
		logging.SetLevel(logging.DEBUG, "sbft")
		logging.SetLevel(logging.DEBUG, "connection")
		logging.SetLevel(logging.DEBUG, "bftserver")
		logging.SetLevel(logging.DEBUG, "config")

	}

	log.Infof("Starting peer with id: %d", id)

	if D < 3 {
		panic("current configuration set up for at least 3 connection layers")
		return
	}



	// Create communication-related variables
	dispatchers := make([]*mir.Dispatcher, D-2) // -2 for managerDispatcher and requestHandlingDispatcher
	connections := make([]*connection.Manager, D)
	startPorts := make(map[int]string)

	// Load full addresses and certificate files
	fullAddresses := config.Config.Servers.Addresses
	certFiles := config.Config.Servers.CertFiles
	if len(fullAddresses) != N || len(certFiles) != N {
		fatal(errors.New("Insufficient peer addresses or certificates in configuration file"))
	}

	// Initialize host addresses and ports
	hosts := make([]string, len(fullAddresses))
	for k, addr := range fullAddresses {
		host := getHost(addr)
		port := getPort(addr)
		startPorts[k] = port
		hosts[k] = host
	}

	// Load information about myself
	ledger := config.Config.Ledger
	self := config.Config.Self.Listen
	log.Infof("self : %s", self)
	selfHost := getHost(self)
	log.Infof("selfHost: %s", selfHost)
	selfStartPort := getPort(self) // port number for the first dispatcher
	log.Infof("selfStartPort: %s", selfStartPort)

	// Declare message dispatchers
	var managerDispatcher *mir.Dispatcher
	managerDispatcher = nil
	var requestHandlingDispatcher *mir.Dispatcher
	requestHandlingDispatcher = nil

	// Create a persistent object
	// TODO: Do we still need this?
	persistence := persist.New(ledger)

	system, err := NewBackend(N, persistence, uint64(config.Config.MaxRequestCount), config.Config.Self.CertFile, config.Config.Self.KeyFile)
	fatal(err)

	tracing.MainTrace.Start(fmt.Sprintf("%s-%03d.trc", outFilePrefix, id), int32(id))
	tracing.MainTrace.StopOnSignal(os.Interrupt, true)

	system.wg.Add(N*D)

	// Create message dispatchers
	for i := 0; i < D; i++ {
		// Calculate address and port number for dispatcher
		// (incrementally from a pre-defined starting port number)
		selfPort := incrementPort(selfStartPort, uint64(i))

		log.Infof("selfPort: %s", selfPort)
		selfAddr := getAddress(selfHost, selfPort)

		// Last dispatcher gets address 0.0.0.0
		// The client is sending requests to a different network interface (for some reason in only wokrs with 0.0.0.0)
		// TODO: Maybe get back to this some day?
		if i == D-1 {
			selfAddr = getAddress("0.0.0.0", selfPort)
		}
		log.Infof("Self addr is %s", selfAddr)

		// Create an authenticated network connection
		// TODO: Make sure we don't TLS-authenticate the clients

		//conn, err := connection.New(selfAddr, config.GetString("self.CertFile"), config.GetString("self.KeyFile"), config.GetString("self.CACertFile"), config.GetBool("UseTLS"))
		conn, err := connection.New(selfAddr, config.Config.Self.CertFile, config.Config.Self.KeyFile)
		fatal(err)
		connections[i] = conn

		peers := make(map[string][]byte)
		peersToId := make(map[string]uint64)

		// Assign IDs, addresses, ports and certificates to peers (for this dispatcher)
		for k, file := range certFiles {
			log.Infof("for k %d start port is %s", k, startPorts[k])
			address := getAddress(hosts[k], incrementPort(startPorts[k], uint64(i)))
			log.Infof("address is: %s", address)
			var err error
			peers[address], err = crypto.ParseCertPEM(file)
			peersToId[address] = uint64(k)
			fatal(err)
		}

		// Check whether this is the managerDispatcher or the requestHandlingDispatcher
		isManager := false
		isRequestHandler := false
		if i == D-2 {
			isManager = true
		}
		if i == D-1 {
			isRequestHandler = true
		}

		log.Infof("Creating dispatcher with id %d", i)

		// Create a new dispatcher with a new backend system.
		// The backend system associates the connection (conn) with a gRPC server that implements the backend semantics.
		// We later use conn to start the gRPC server.
		err = system.AddConnection(peers, peersToId, conn)
		fatal(err)

		dispatcher, err := mir.NewDispatcher(N, uint64(i), system, isManager, isRequestHandler, uint64(D-2))
		fatal(err)

		// Assign the new dispatcher to the right place
		if i == D-2 {
			managerDispatcher = dispatcher
		} else if i == D-1 {
			requestHandlingDispatcher = dispatcher
		} else {
			dispatchers[i] = dispatcher
		}
	}

	// Create a new Mir instance
	_, err = mir.New(uint64(id), managerDispatcher.Backend(), dispatchers, managerDispatcher, requestHandlingDispatcher)
	fatal(err)

	// Create an dummy channel for each dispatcher (for synchronization)
	complete := make(map[int]chan int)
	for i := 0; i < D; i++ {
		complete[i] = make(chan int)
	}

	// Start all message dispatchers
	managerDispatcher.Backend().AddReceiver("manager", managerDispatcher)
	connMD := connections[D-2]
	go startServer(connMD, complete[D-2])

	requestHandlingDispatcher.Backend().AddReceiver("request", requestHandlingDispatcher)
	connRD := connections[D-1]
	go startServer(connRD, complete[D-1])

	for i := 0; i < D-2; i++ {
		dispatchers[i].Backend().AddReceiver("protocol", dispatchers[i])
		conn := connections[i]
		go startServer(conn, complete[i])
	}

	// Wait for all the connections to be established
	// TODO: See if we can catch the event of completion of peer-to-peer connection establishment
	system.wg.Wait()

	time.Sleep(time.Duration(config.Config.ServerConnectionBuffer) * time.Second)

	go managerDispatcher.RunManager()
	go requestHandlingDispatcher.RunRequestProcessor()

	// Wait for completion
	for i := 0; i < D; i++ {
		_ = <-complete[i]
	}
}

func startServer(conn *connection.Manager, complete chan int) {
	if err := conn.Server.Serve(conn.Listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	close(complete)
}