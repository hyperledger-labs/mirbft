package main

import (
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/checkpoint"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/discovery"
	"github.com/hyperledger-labs/mirbft/manager"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/messenger"
	"github.com/hyperledger-labs/mirbft/orderer"
	"github.com/hyperledger-labs/mirbft/profiling"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/statetransfer"
	"github.com/hyperledger-labs/mirbft/tracing"
)

// Flag indicating whether profiling is enabled.
// Used to decide whether the tracer should shut down the process on the INT signal or not.
// TODO: This is ugly and dirty. Implement graceful shutdown!
var profilingEnabled = false

func main() {

	// Get command line arguments
	configFileName := os.Args[1]
	discoveryServAddr := os.Args[2]
	ownPublicIP := os.Args[3]
	ownPrivateIP := os.Args[4]

	config.LoadFile(configFileName)

	// Configure logger
	zerolog.SetGlobalLevel(config.Config.LoggingLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	logger.Logger = logger.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		NoColor:    true,
		TimeFormat: "15:04:05.000"})
	//zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	//logger.Logger = logger.Output(zerolog.ConsoleWriter{
	//	Out:        os.Stdout,
	//	NoColor:    true,
	//	TimeFormat: "15:04:05.000",
	//})

	// Initialize packages that need the configuration to be loaded for initialization
	membership.Init()
	request.Init()
	tracing.Init()
	statetransfer.Init()

	// Register with the discovery service and obtain:
	// - Own ID
	// - Identities of all other peers
	// - Private key
	// - Public key for BLS threshold cryptosystem
	// - Private key share for BLS threshold cryptosystem
	ownID, nodeIdentities, privateKey, serializedTBLSPubKey, serializedTBLSPrivKeyShare :=
		discovery.RegisterPeer(discoveryServAddr, ownPublicIP, ownPrivateIP)
	membership.OwnID = ownID
	membership.OwnPrivKey = privateKey
	membership.InitNodeIdentities(nodeIdentities)
	logger.Info().
		Int32("ownID", ownID).
		Int("numPeers", len(nodeIdentities)).
		Msg("Registered with discovery server.")

	// Desirialize TBLS keys
	TBLSPubKey, err := crypto.TBLSPubKeyFromBytes(serializedTBLSPubKey)
	if err != nil {
		logger.Fatal().Msgf("Could not deserialize TBLS public key %s", err.Error())
	}
	membership.TBLSPublicKey = TBLSPubKey
	TBLSPrivKeyShare, err := crypto.TBLSPrivKeyShareFromBytes(serializedTBLSPrivKeyShare)
	if err != nil {
		logger.Fatal().Msgf("Could not deserialize TBLS private key share %s", err.Error())
	}
	membership.TBLSPrivKeyShare = TBLSPrivKeyShare

	// Start profiler if necessary
	// ATTENTION! We first look for argument 6, and only then check argument 5
	//            (as the presence of profiling influences setting up of tracing).
	if len(os.Args) > 6 {
		profilingEnabled = true // UGLY DIRTY CODE!
		logger.Info().Msg("Profiling enabled.")
		setUpProfiling(os.Args[6])
	}

	// Set up tracing if necessary
	if len(os.Args) > 5 {
		setUpTracing(os.Args[5], ownID)
	}

	// Declare variables for component modules.
	var mngr manager.Manager
	var ord orderer.Orderer
	var chkp checkpoint.Checkpointer
	var rsp *request.Responder

	// Instantiate component modules (with stubs).
	mngr = setManager(config.Config.Manager)
	ord = setOrderer(config.Config.Orderer)
	chkp = setCheckpointer(config.Config.Checkpointer)
	rsp = request.NewResponder()

	// Initialize modules.
	// No outgoing messages must be produced even after initialization,
	// but the modules must be raady to process incoming messages.
	ord.Init(mngr)
	chkp.Init(mngr)

	//// Make adjustments if this peer simulates a faulty one
	//if membership.OwnID < int32(config.Config.Failures) {
	//	config.Config.ViewChangeTimeout = 100
	//}

	// Register message and entry handlers
	messenger.CheckpointMsgHandler = chkp.HandleMessage
	messenger.OrdererMsgHandler = ord.HandleMessage
	messenger.ClientRequestHandler = request.HandleRequest
	messenger.StateTransferMsgHandler = statetransfer.HandleMessage
	statetransfer.OrdererEntryHandler = ord.HandleEntry

	// Create wait group for all the modules that will run as separate goroutines.
	// (Currently the graceful termination is not implemented, so waiting on wg will take forever and the process
	// needs to be killed.)
	wg := sync.WaitGroup{}
	wg.Add(5) // messenger, checkpointer, orderer, manager, responder

	// Start the messaging subsystem.
	// Connect needs to come after starting the messenger which launches the gRPC server everybody connects to.
	// Otherwise we deadlock, everybody connecting to gRPC servers that are not (and never will be) running.
	go messenger.Start(&wg)
	messenger.Connect()
	logger.Info().Msg("Connected to all peers.")

	// Synchronize with master again to make sure that all peers finished connecting.
	discovery.SyncPeer(discoveryServAddr, ownID)
	logger.Info().Msg("All peers finished connecting. Starting ISS.")

	// If we are simulating a crashed node, exit immediately.
	if config.Config.LeaderPolicy == "SimulatedRandomFailures" {
		crash := true
		for _, l := range manager.NewLeaderPolicy(config.Config.LeaderPolicy).GetLeaders(0) {
			if l == membership.OwnID {
				crash = false
			}
		}
		if crash {
			logger.Info().Msg("Simulating crashed peer. Exiting.")
			return
		}
	}

	// Start all modules.
	// The order of the calls must not matter, as they are all concurrent. If it does, it's a bug.
	// By now all the modules must be initialized and ready to process messages.
	// After starting, the modules will produce messages on their own.
	go rsp.Start(&wg)
	go chkp.Start(&wg)
	go mngr.Start(&wg)
	go ord.Start(&wg)

	// Wait for all modules to finish.
	wg.Wait()
}

// Enables and starts the profiler of used resources.
func setUpProfiling(outFilePrefix string) {
	logger.Info().Msg("Setting up profiling.")

	profiling.StartProfiler("cpu", outFilePrefix+".cpu", 1)
	profiling.StartProfiler("block", outFilePrefix+".block", 1)
	profiling.StartProfiler("mutex", outFilePrefix+".mutex", 1)

	// Stop profiler on INT signal
	// TODO: Once graceful termination is implemented, use defer StopProfiler()
	profiling.StopOnSignal(os.Interrupt, true)
}

// Sets up tracing of events.
func setUpTracing(outFileName string, ownID int32) {

	// Initialize tracing with output file name given at command line
	tracing.MainTrace.Start(outFileName, ownID)

	// TODO: Move the CPU tracing to a more appropriate place
	//       For now, it is here, as it depends on tracing being enabled.
	profiling.StartCPUTracing(tracing.MainTrace, 500*time.Millisecond)

	// Stop tracing on INT signal.
	// TODO: Once graceful termination is implemented, use defer
	// The second parameter determines whether to shut down the process after stopping the tracing.
	// If profiling is on, the profiler will do the job.
	// TODO: ATTENTION! Implement some synchronization here, otherwise the profiler might exit the process before
	//                  the tracer is done flushing its buffers.
	tracing.MainTrace.StopOnSignal(os.Interrupt, !profilingEnabled)

	logger.Info().Str("traceFile", outFileName).Msg("Started tracing.")
}

func setManager(managerType string) (mngr manager.Manager) {
	switch managerType {
	case "Dummy":
		mngr = manager.NewDummyManager()
	case "Mir":
		mngr = manager.NewMirManager()
	default:
		logger.Fatal().Msg("Unsupported manager type")
	}
	return mngr
}

func setOrderer(ordererType string) (ord orderer.Orderer) {
	switch ordererType {
	case "Dummy":
		ord = &orderer.DummyOrderer{}
	case "Pbft":
		ord = &orderer.PbftOrderer{}
	case "HotStuff":
		ord = &orderer.HotStuffOrderer{}
	case "Raft":
		ord = &orderer.RaftOrderer{}
	default:
		logger.Fatal().Msg("Unsupported orderer type")
	}
	return ord
}

func setCheckpointer(managerType string) (chkp checkpoint.Checkpointer) {
	switch managerType {
	case "Simple":
		chkp = checkpoint.NewSimpleCheckpointer()
	case "Signing":
		chkp = checkpoint.NewSigningCheckpointer()
	default:
		logger.Fatal().Msg("Unsupported manager type")
	}
	return chkp
}
