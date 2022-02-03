package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/tracing"
)

type LocalClient struct {
	ownClientID int32

	// Number of requests the client tries to submit before stopping.
	numRequests int

	// For each request, stores a flag indicating whether the request is finished.
	finished map[int32]bool

	// The sequence number of the oldest in-flight request.
	oldestClientSN int32

	// Channel containing in-flight requests. Its capacity corresponds to the watermark window size.
	// Used to implement the client watermark window. Before submitting a request, it needs to be written
	// to this channel. Once enough responses have been received for the first request in the channel, it is removed,
	// making space for the next one.
	inFlight chan *pb.ClientRequest

	// Logger used by this client.
	// Each client's log goes in a separate file, even if multiple clients are running from within the same process.
	log zerolog.Logger

	// Same as with logging, each client has a separate trace that is output in a separate file,
	// even if multiple clients are running in the same process.
	trace tracing.Trace

	// Handler function to pass submitted requests to.
	// This replaces sending the request over the network (as this is a LocalClient).
	requestHandler func(msg *pb.ClientRequest)
}

// Allocates and returns a pointer to a new client.
func NewLocalClient(numRequests int) *LocalClient {
	return &LocalClient{
		ownClientID:    -1,
		numRequests:    numRequests,
		finished:       make(map[int32]bool, numRequests),
		oldestClientSN: 0,
		inFlight:       make(chan *pb.ClientRequest, config.Config.ClientWatermarkWindowSize),
		trace: &tracing.BufferedTrace{
			Sampling:              config.Config.TraceSampling,
			BufferCapacity:        config.Config.EventBufferSize,
			ProtocolEventCapacity: config.Config.EventBufferSize,
			RequestEventCapacity:  config.Config.EventBufferSize,
		},
		// The clientStubs field is not initialized, as it is directly assigned a map
		// that the messenger allocates when connecting to the orderers.
	}
}

// Runs the main client logic that
// - connects to the orderers and
// - submits requests according to the configuration read from the config file.
func (c *LocalClient) Run(handlerFunc func(msg *pb.ClientRequest), wg *sync.WaitGroup) {
	defer wg.Done()

	// Register request handler function
	c.requestHandler = handlerFunc

	// Open log file specific to this client and create a new logger.
	logFileName := fmt.Sprintf("%s-%03d.log", outFilePrefix, c.ownClientID)
	logFile, err := os.Create(logFileName)
	if err != nil {
		logger.Fatal().
			Err(err).
			Int32("clId", c.ownClientID).
			Str("fileName", logFileName).
			Msg("Could not create log file.")
	}
	defer logFile.Close()
	c.log = logger.Output(zerolog.ConsoleWriter{Out: logFile, NoColor: true})

	// Initialize tracing
	c.trace.Start(fmt.Sprintf("%s-%03d.trc", outFilePrefix, c.ownClientID), -1) // Client uses -1 as peer ID.
	defer c.trace.Stop()

	c.log.Info().Msg("Starting to submit requests.")

	// Submit requests
	for i := int32(0); i < int32(c.numRequests); i++ {

		// blocks while watermark window is full
		c.submitRequest(i)

		// After submitting each request, wait for some time to respect the maximum request rate.
		// config.Config.RequestRateLimit is in requests / second.
		time.Sleep(time.Microsecond * 1000000 / time.Duration(config.Config.RequestRate))
	}

	c.log.Info().Int("nReq", c.numRequests).Msg("All requests submitted.")
}

// Submits a single client request with sequence number seqNr.
// Blocks until the request fits in the client watermark window.
func (c *LocalClient) submitRequest(seqNr int32) {

	c.finished[seqNr] = false

	// Create request message.
	req := &pb.ClientRequest{
		RequestId: &pb.RequestID{
			ClientId: c.ownClientID,
			ClientSn: seqNr,
		},
		Payload:   randomRequestPayload,
		Signature: nil,
	}

	// Write request to in-flight request channel.
	// The buffer of this channel is as big as the watermark window size and requests stay in this channel until
	// enough responses have been received. Thus, this line blocks until there is "space" in the watermark window.
	c.inFlight <- req

	c.trace.Event(tracing.REQ_SEND, int64(seqNr), int64(c.ownClientID))

	// Submit request to handler function.
	c.requestHandler(req)

	c.log.Debug().Int32("clSeqNr", req.RequestId.ClientSn).Msg("Submitted request.")
}

// Registers response to request with clientSN from replica peerID.
// If this is the last response necessary for the oldest pending request, advances the watermark window accordingly.
func (c *LocalClient) RegisterResponse(clientSN int32) {

	clientWatermarkWindowSize := int32(config.Config.ClientWatermarkWindowSize)

	// Ignore responses outside of the client watermark window.
	if clientSN >= c.oldestClientSN && clientSN < c.oldestClientSN+clientWatermarkWindowSize {

		// Mark request as finished
		if !c.finished[clientSN] {
			c.trace.Event(tracing.REQ_FINISHED, int64(clientSN), int64(c.ownClientID))
			c.finished[clientSN] = true
			c.log.Debug().Int32("clSeqNr", clientSN).Msg("Request finished (out of order).")

			// Sanity check: For a LocalClient, the else branch should never be executed,
			// As we only ever get responses from our local peer.
		} else {
			c.log.Error().
				Int32("clSeqNr", clientSN).
				Msg("Duplicate response at the local client!")
		}

		// Sanity check: Never should receive responses for requests that shouldn't have been issued.
	} else if clientSN >= c.oldestClientSN+clientWatermarkWindowSize {
		c.log.Error().
			Int32("clSeqNr", clientSN).
			Int32("maxExpected", c.oldestClientSN+clientWatermarkWindowSize-1).
			Msg("Received response for unsubmitted request!")
	}

	// If this was the last response required for the oldest in-flight request
	if clientSN == c.oldestClientSN {

		// Process finished requests.
		for c.finished[c.oldestClientSN] {
			c.log.Info().Int32("clSeqNr", c.oldestClientSN).Msg("Request finished (in order).")
			<-c.inFlight
			c.oldestClientSN++
		}
	}
}
