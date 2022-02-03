package main

import (
	"bufio"
	"os"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/discovery"
	"google.golang.org/grpc"
)

func main() {

	// Configure logger
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"})

	// Parse command line arguments.
	port := os.Args[1]  // Port to listen on
	cmds := os.Args[2:] // Interpret all remaining arguments as command tokens

	// Create a wait groups to wait for goroutines.
	// Need two different ones, as we first need to wait for commands to be processed and only then
	// shut down (and wait for) the server.
	var srvWg sync.WaitGroup
	srvWg.Add(1)
	var cmdWg sync.WaitGroup
	cmdWg.Add(1)

	// Start the discovery master server.
	grpcServer := grpc.NewServer()
	masterSrv := discovery.NewDiscoveryServer()
	go discovery.RunDiscoveryServer(port, grpcServer, masterSrv, &srvWg)

	// Execute commands given as parameters on the command line
	tokenChan := masterSrv.ProcessCommands(&cmdWg) // Commands tokens to be written in the returned channel.
	for i := 0; i < len(cmds); i++ {
		switch cmds[i] {
		case "-":
			readCommandsFromStdin(tokenChan)
		case "file":
			i++
			processCommandFile(cmds[i], tokenChan)
		default:
			tokenChan <- cmds[i]
		}
	}

	// Stop command processor and wait until it's done.
	close(tokenChan)
	cmdWg.Wait()

	// Stop and wait for server.
	grpcServer.Stop()
	srvWg.Wait()
}

func processCommandFile(fileName string, tokenChannel chan string) {

	// Declare file and error variables
	var file *os.File
	var err error

	// Open input file.
	if file, err = os.Open(fileName); err != nil {
		logger.Error().Err(err).Str("fileName", fileName).Msg("Couldn't open file. Ignoring.")
		return
	}
	defer file.Close()

	// Create scanner reading file line by line
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines) // Should be redundant, as ScanLines is the default

	// Process each line of the file
	for scanner.Scan() {
		discovery.ParseCommandStr(scanner.Text(), tokenChannel)
	}

	// Check for errors
	if err := scanner.Err(); err != nil {
		logger.Error().Err(err).Str("fileName", fileName).Msg("Error while reading input file.")
	}
}

// Reads master commands from stdin and, dumps them as tokens to the provided channel.
// This channel is meant to be read and interpreted by the DiscoveryServer.
func readCommandsFromStdin(tokenChannel chan string) {
	logger.Info().Msg("Reading commands from stdin.")

	// Create scanner of stdin
	scanner := bufio.NewScanner(os.Stdin)

	// For each input line
	for scanner.Scan() {
		// Trim white space from input
		cmdStr := strings.TrimSpace(scanner.Text())

		// Stop iterating on "done"
		if cmdStr == "done" {
			break
			// Process command string
		} else {
			discovery.ParseCommandStr(cmdStr, tokenChannel)
		}
	}

	// Handle potential scanner error.
	if err := scanner.Err(); err != nil {
		logger.Error().Err(err).Msg("Error while scanning input: ")
	}

	logger.Info().Msg("Finished reading commands from stdin.")
}
