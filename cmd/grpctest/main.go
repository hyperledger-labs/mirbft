package main

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"net"
	"os"
	"strconv"

	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/messenger"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	port       = 9998
	nRequests  = 10000
	nResponses = 10000
)

var (
	payloadSizes = [...]int{1, 4, 16, 64, 256, 1024, 4096, 8192, 16384, 32768, 65536} // In Bytes.

	// Map of random byte arrays used as request payload. Indexed by payload size.
	// Only generated once and used by all the clients in this process for all requests.
	randomByteArrays map[int][]byte

	useTLS = false
)

func main() {

	// Configure logger.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"})

	// Initialize random byte arrays of various sizes
	randomByteArrays = make(map[int][]byte)
	for _, payloadSize := range payloadSizes {
		randomByteArrays[payloadSize] = make([]byte, payloadSize)
		rand.Read(randomByteArrays[payloadSize])
	}

	var wg sync.WaitGroup

	if os.Args[1] == "server" {

		if len(os.Args) > 2 && os.Args[2] == "--tls" {
			useTLS = true
		}

		wg.Add(1)
		runServer(&wg)
		logger.Info().Int("listenPort", port).Bool("TLS", useTLS).Msg("Server started.")

	} else if os.Args[1] == "client" {

		if len(os.Args) > 3 && os.Args[3] == "--tls" {
			useTLS = true
		}

		wg.Add(1)
		runClient(0, os.Args[2], &wg)

	}

	wg.Wait()
}

type testServer struct {
	pb.UnimplementedTestServiceServer
}

func (ts *testServer) Test(srv pb.TestService_TestServer) error {

	logger.Info().Msg("Incoming connection.")

	for _, payloadSize := range payloadSizes {
		logger.Info().Int("payloadSize", payloadSize).Msg("Receiving requests.")
		startItme := time.Now().UnixNano()
		for i := 0; i < nRequests; i++ {
			if _, err := srv.Recv(); err != nil {
				logger.Info().Err(err).Msg("Error receiving request.")
				break
			}
		}
		endTime := time.Now().UnixNano()
		duration := (endTime - startItme) / 1000000                           // In milliseconds
		gbps := float64(payloadSize*nRequests*8) / float64(endTime-startItme) // In Gbps
		logger.Info().
			Int("payloadSize", payloadSize).
			Int("numRequests", nRequests).
			Int64("duration", duration).
			Float64("Gbps", gbps).
			Msg("Done Receiving.")
	}

	for _, payloadSize := range payloadSizes {
		logger.Info().Int("payloadSize", payloadSize).Msg("Sending responses.")
		startItme := time.Now().UnixNano()
		for i := 0; i < nResponses; i++ {
			sendResponse(i, payloadSize, srv)
		}
		endTime := time.Now().UnixNano()
		duration := (endTime - startItme) / 1000000                            // In milliseconds
		gbps := float64(payloadSize*nResponses*8) / float64(endTime-startItme) // In Gbps
		logger.Info().
			Int("payloadSize", payloadSize).
			Int("numResponses", nResponses).
			Int64("duration", duration).
			Float64("Gbps", gbps).
			Msg("Done sending.")
	}

	return nil
}

func runServer(wg *sync.WaitGroup) *grpc.Server {
	// Depending on configuration, create a plain or TLS-enabled gRPC server
	var grpcServer *grpc.Server
	if useTLS {
		tlsConfig := messenger.ConfigureTLS(config.Config.CertFile, config.Config.KeyFile)
		grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		grpcServer = grpc.NewServer()
	}

	// Assign logic to the gRPC server.
	pb.RegisterTestServiceServer(grpcServer, &testServer{})

	// Start listening on the network
	conn, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		logger.Fatal().Int("port", port).Err(err).Msg("Failed to listen for connections.")
		os.Exit(1)
	}

	// Start the gRPC server
	go func() {
		if err := grpcServer.Serve(conn); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start gRPC server.")
		}
		wg.Done()
	}()

	return grpcServer
}

func runClient(clID int, serverAddr string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create logger specific to this client and create a new logger.
	clientLog := logger.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "15:04:05.000"}).With().Int("clId", clID).Logger()

	clientLog.Info().Str("serverAddr", serverAddr).Bool("TLS", useTLS).Msg("Starting client.")

	// Set general gRPC dial options.
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		//grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	}

	// Add TLS-specific gRPC dial options, depending on configuration
	if useTLS {
		tlsConfig := messenger.ConfigureTLS(config.Config.CertFile, config.Config.KeyFile)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	if msgSink := createConnection(serverAddr, dialOpts); msgSink == nil {
		clientLog.Fatal().Msg("Could not connect to test server.")
	} else {

		for _, payloadSize := range payloadSizes {
			clientLog.Info().Int("payloadSize", payloadSize).Msg("Sending requests.")
			startItme := time.Now().UnixNano()
			for i := 0; i < nRequests; i++ {
				sendRequest(i, payloadSize, msgSink)
			}
			endTime := time.Now().UnixNano()
			duration := (endTime - startItme) / 1000000                           // In milliseconds
			gbps := float64(payloadSize*nRequests*8) / float64(endTime-startItme) // In Gbps
			clientLog.Info().
				Int("payloadSize", payloadSize).
				Int("numRequests", nRequests).
				Int64("duration", duration).
				Float64("Gbps", gbps).
				Msg("Done sending requests.")
		}

		for _, payloadSize := range payloadSizes {
			clientLog.Info().Int("payloadSize", payloadSize).Msg("Receiving responses.")
			startItme := time.Now().UnixNano()
			for i := 0; i < nResponses; i++ {
				if _, err := msgSink.Recv(); err != nil {
					logger.Info().Err(err).Msg("Error receiving request.")
					break
				}
			}
			endTime := time.Now().UnixNano()
			duration := (endTime - startItme) / 1000000                            // In milliseconds
			gbps := float64(payloadSize*nResponses*8) / float64(endTime-startItme) // In Gbps
			clientLog.Info().
				Int("payloadSize", payloadSize).
				Int("numResponses", nResponses).
				Int64("duration", duration).
				Float64("Gbps", gbps).
				Msg("Done Receiving.")
		}

		if err := msgSink.CloseSend(); err != nil {
			clientLog.Error().Err(err).Str("serverAddr", serverAddr).Msg("Failed closing connection to server.")
		}
	}
}

func createConnection(addr string, dialOpts []grpc.DialOption) pb.TestService_TestClient {

	// Set up a gRPC connection.
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		logger.Error().Err(err).Str("addrStr", addr).Msg("Could not connect to gRPC server.")
		return nil
	}

	// Register client stub.
	client := pb.NewTestServiceClient(conn)

	// Remotely invoke the Test() function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.Test(context.Background())
	if err != nil {
		logger.Error().Err(err).Str("addrStr", addr).Msg("Could not invoke Test RPC.")
		if err := conn.Close(); err != nil {
			logger.Error().Err(err).Msg("Failed to close network connection.")
		}
		return nil
	}

	// Return the message sink connected to the peer.
	return msgSink
}

func sendRequest(sn int, payloadSize int, msgSink pb.TestService_TestClient) {
	req := pb.TestReq{
		Sn:      int32(sn),
		Payload: randomByteArrays[payloadSize],
	}

	if err := msgSink.Send(&req); err != nil {
		logger.Fatal().Err(err).Msg("Could not send request on network.")
	}
}

func sendResponse(sn int, payloadSize int, msgSink pb.TestService_TestServer) {
	resp := pb.TestResp{
		Sn:      int32(sn),
		Payload: randomByteArrays[payloadSize],
	}

	if err := msgSink.Send(&resp); err != nil {
		logger.Fatal().Err(err).Msg("Could not send response on network.")
	}
}
