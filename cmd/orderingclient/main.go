package main

import (
	"math/rand"
	"os"
	"sync"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/membership"
	"github.com/hyperledger-labs/mirbft/profiling"
)

var (
	// Prefix of the client-specific output files, to which the client ID will be appended.
	outFilePrefix string

	// Used to initialize membership only once.
	membershipInitializer sync.Once

	// Random byte array used as request payload.
	// Only generated once and used by all the clients in this process for all requests.
	randomRequestPayload []byte
)

// Need more than one third of the peers to respond.
func enoughResponses(n int) bool {
	return n > membership.NumNodes()/3
}

func main() {
	// Check number of parameters
	if len(os.Args) <= 2 {
		logger.Fatal().
			Int("argsGiven", len(os.Args)-1).
			Msg("usage: orderingclient config_file_name discovery_server_addr:port out_file_prefix [profiler_output_prefix]")
	}

	// Get config file name
	configFileName := os.Args[1]
	config.LoadFile(configFileName)

	// Configure logger.
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

	// Get discovery server address from command line.
	// The membership will be obtained from this server
	dServAddr := os.Args[2]

	// Get output file prefix from command line.
	// Log and trace files will all begin with this
	outFilePrefix = os.Args[3]

	// Initialize membership module
	membership.Init()

	// Start profiler if necessary
	if len(os.Args) > 4 {
		setUpProfiling(os.Args[4])
		defer profiling.StopProfiler()
	}

	// Get number of clients and number of requests / client.
	numClients := config.Config.ClientsPerProcess
	numRequests := config.Config.RequestsPerClient

	// Generate random request payload
	randomRequestPayload = make([]byte, config.Config.RequestPayloadSize)
	rand.Read(randomRequestPayload)

	logger.Info().
		Int("numClients", numClients).
		Int("numRequests", numRequests).
		Msg("Starting clients.")

	// Create wait group for initializing and running clients
	wg := sync.WaitGroup{}

	// Create all clients first (and have them pre-compute all requests)
	logger.Info().Msg("Initilizing clients.")
	clients := make([]*client, numClients)
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(j int) { clients[j] = newClient(dServAddr, numRequests); wg.Done() }(i)
	}

	// Wait until all clients are initialized.
	wg.Wait()
	logger.Info().Msg("Clients initialized.")

	// Run all clients
	logger.Info().Msg("Launching clients.")
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go clients[i].Run(&wg)
	}

	// Wait for clients to finish.
	logger.Info().Msg("Waiting for clients.")
	wg.Wait()

	logger.Info().Msg("Done.")
}

// Enables and starts the profiler of used resources.
func setUpProfiling(outFilePrefix string) {
	profiling.StartProfiler("cpu", outFilePrefix+".cpu", 1)
	profiling.StartProfiler("block", outFilePrefix+".block", 1)
	profiling.StartProfiler("mutex", outFilePrefix+".mutex", 1)
}
