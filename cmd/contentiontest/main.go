package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/hyperledger-labs/mirbft/profiling"

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
	port0       = 9996
	port1       = 9997
	controlPort = 9998
	mapKeySpace = (1024 * 1024)
)

var (
	useTLS      = false
	nClients    = 1
	payloadSize = 1
	timeout     = 1 // experiment duration in seconds
	outfile     = ""
	randomBytes []byte
	tlsDir      = ""

	srvStopChan = make(chan struct{})

	statsLock          sync.Mutex
	counting                 = false
	running            int32 = 1
	totalReq0          int64
	totalReq1          int64
	cumTime0           int64
	cumTime1           int64
	connections0       int64
	connections1       int64
	currentConnections         = 0
	bandwidth          float64 = 0

	numMaps       = 0
	plainMaps     = make(map[int]map[int]bool)
	plainMapLocks = make(map[int]*sync.Mutex)
	syncMap       sync.Map
)

func main() {

	// Configure logger.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"})

	if os.Args[1] == "server" {
		os.Args = os.Args[2:]

		for len(os.Args) > 0 {
			if os.Args[0] == "--tls" {
				useTLS = true
				tlsDir = os.Args[1]
				os.Args = os.Args[1:]
				logger.Info().Str("tlsDir", tlsDir).Msg("Using TLS.")
				config.Config.KeyFile = fmt.Sprintf("%s/auth.key", tlsDir)
				config.Config.CertFile = fmt.Sprintf("%s/auth.pem", tlsDir)
				config.Config.CACertFile = fmt.Sprintf("%s/ca.pem", tlsDir)
			} else if os.Args[0] == "-f" || os.Args[0] == "--outFile" {
				outfile = os.Args[1]
				os.Args = os.Args[1:]
			} else if os.Args[0] == "-m" || os.Args[0] == "--maps" {
				numMaps, _ = strconv.Atoi(os.Args[1])
				os.Args = os.Args[1:]
			} else {
				logger.Fatal().Str("arg", os.Args[0]).Msg("Unknown argument.")
			}
			os.Args = os.Args[1:]
		}

		// Initialize dummy maps
		for i := 0; i < numMaps; i++ {
			plainMaps[i] = make(map[int]bool)
			plainMapLocks[i] = &sync.Mutex{}
		}

		wg := &sync.WaitGroup{}
		wg.Add(3)

		server0, server1, controlServer := runServer(wg)
		logger.Info().
			Int("port0", port0).
			Int("port1", port1).
			Int("controlPort", controlPort).
			Bool("TLS", useTLS).
			Msg("Server started.")

		<-srvStopChan
		server0.GracefulStop()
		server1.GracefulStop()
		controlServer.GracefulStop()

		wg.Wait()

	} else if os.Args[1] == "client" {
		srvAddr := os.Args[2]
		os.Args = os.Args[3:]

		ch0 := false
		ch1 := false

		for len(os.Args) > 0 {
			if os.Args[0] == "--tls" {
				useTLS = true
				tlsDir = os.Args[1]
				os.Args = os.Args[1:]
				logger.Info().Str("tlsDir", tlsDir).Msg("Using TLS.")
				config.Config.KeyFile = fmt.Sprintf("%s/auth.key", tlsDir)
				config.Config.CertFile = fmt.Sprintf("%s/auth.pem", tlsDir)
				config.Config.CACertFile = fmt.Sprintf("%s/ca.pem", tlsDir)
			} else if os.Args[0] == "-c" {
				if os.Args[1] == "0" {
					ch0 = true
				} else if os.Args[1] == "1" {
					ch1 = true
				}
				os.Args = os.Args[1:]
			} else if os.Args[0] == "-n" {
				nClients, _ = strconv.Atoi(os.Args[1])
				os.Args = os.Args[1:]
			} else if os.Args[0] == "-t" {
				timeout, _ = strconv.Atoi(os.Args[1])
				os.Args = os.Args[1:]
			} else if os.Args[0] == "-l" {
				payloadSize, _ = strconv.Atoi(os.Args[1])
				os.Args = os.Args[1:]
			} else {
				logger.Fatal().Str("arg", os.Args[0]).Msg("Unknown argument.")
			}
			os.Args = os.Args[1:]
		}

		// Initialize array with payload
		randomBytes = make([]byte, payloadSize)
		rand.Read(randomBytes)

		if ch0 || ch1 {
			ch := 0 // channel the next created client will send requests on.

			controlClient := pb.NewContentionTestControlClient(newClientConnection(fmt.Sprintf("%s:%d", srvAddr, controlPort)))

			if _, err := controlClient.Control(
				context.Background(),
				&pb.ControlCommand{Cmd: pb.ControlCommand_START_STAT}); err != nil {
				logger.Fatal().Err(err).Msg("Could not send control command to server.")
			}

			wg := &sync.WaitGroup{}
			wg.Add(nClients)
			for i := 0; i < nClients; i++ {
				if ch0 && ch1 {
					go runClient(i, srvAddr, ch, wg)
					ch = (ch + 1) % 2 // If both channels should be used, alternate between them
				} else if ch0 {
					go runClient(i, srvAddr, 0, wg)
				} else if ch1 {
					go runClient(i, srvAddr, 1, wg)
				} else {
					panic(fmt.Errorf("Channel not specified. This must never happen."))
				}
			}
			wg.Wait()

			if _, err := controlClient.Control(
				context.Background(),
				&pb.ControlCommand{Cmd: pb.ControlCommand_STOP_STAT}); err != nil {
				logger.Fatal().Err(err).Msg("Could not send control command to server.")
			}

		} else {
			logger.Error().Msg("Must specify at least one channel to send messages.")
		}

	}

}

type contentionTest0Server struct {
	pb.UnimplementedContentionTest0Server
}

type contentionTest1Server struct {
	pb.UnimplementedContentionTest1Server
}

type contentionTestControlServer struct {
	pb.UnimplementedContentionTestControlServer
}

func (ts *contentionTestControlServer) Control(_ context.Context, cmd *pb.ControlCommand) (*pb.ControlResponse, error) {

	switch cmd.Cmd {
	case pb.ControlCommand_START_STAT:

		logger.Info().Msg("Resetting statistics.")

		statsLock.Lock()

		// Unfortunately the profiler cannot be reset each time stat_start is called.
		// Thus unlike the other statistics, that are effectively launched by the last connected client
		// (as they are being reset by each new connection), the profiling is started by the first connecting client.
		// This is OK though, as by its nature, the mutex profiler does not suffer from being satted too early.
		if !counting {
			profiling.StartProfiler("mutex", "contention-test.mutex", 1)
		}
		totalReq0 = 0
		totalReq1 = 0
		cumTime0 = 0
		cumTime1 = 0
		connections0 = 0
		connections1 = 0
		bandwidth = 0
		counting = true

		statsLock.Unlock()
	case pb.ControlCommand_STOP_STAT:

		statsLock.Lock()
		// Only stop stats once. This time, the last connecting client resets the counting
		if counting {

			// Stop others and wait for them
			logger.Info().Int32("running", atomic.LoadInt32(&running)).Msg("Stopping.")
			counting = false
			atomic.StoreInt32(&running, 0)
			for currentConnections > 0 {
				statsLock.Unlock()
				time.Sleep(100 * time.Millisecond)
				statsLock.Lock()
			}
			statsLock.Unlock()

			profiling.StopProfiler()
			statsLock.Lock()
			rate0 := int64(0)
			rate1 := int64(0)
			if cumTime0 > 0 {
				rate0 = connections0 * totalReq0 * 1000 / cumTime0
			}
			if cumTime1 > 0 {
				rate1 = connections1 * totalReq1 * 1000 / cumTime1
			}
			logger.Info().
				Int64("totalReq0", totalReq0).
				Int64("totalReq1", totalReq1).
				Int64("rate0", rate0).
				Int64("rate1", rate1).
				Int64("totalRate", rate0+rate1).
				Float64("Mbps", bandwidth).
				Msg("Finished experiment.")
			statsLock.Unlock()
			srvStopChan <- struct{}{}

			if outfile != "" {
				f, err := os.OpenFile(outfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					logger.Error().Err(err).Str("outfile", outfile).Msg("Failed to open file.")
				}
				defer func() {
					if err := f.Close(); err != nil {
						logger.Error().Err(err).Str("outfile", outfile).Msg("Failed to close file.")
					}
				}()

				f.WriteString(fmt.Sprintf("% 10d, % 10d, % 10.3f, % 10d, % 10d, % 10d, % 10d,", // newline left out on purpose
					rate0+rate1,
					connections0+connections1,
					bandwidth,
					connections0,
					rate0,
					connections1,
					rate1,
				))
			}
		} else {
			statsLock.Unlock()
		}
	}

	return &pb.ControlResponse{}, nil
}

func (ts *contentionTest0Server) TestContention0(srv pb.ContentionTest0_TestContention0Server) error {

	statsLock.Lock()
	currentConnections++
	statsLock.Unlock()

	logger.Info().Msg("Incoming test connection (0).")

	startTime := time.Now().UnixNano()
	received := 0
	receivedData := 0
	for req, err := srv.Recv(); atomic.LoadInt32(&running) == 1 && err == nil; _, err = srv.Recv() {

		// Increment (local) stats variables
		received++
		receivedData += len(req.Payload)

		// Do dummy work
		key := rand.Intn(mapKeySpace)

		if numMaps > 0 {
			idx := rand.Intn(numMaps)
			plainMapLocks[idx].Lock()
			plainMaps[idx][key] = true
			plainMapLocks[idx].Unlock()
		}
		val, _ := syncMap.Load(key)
		syncMap.Store(key/2, val == nil)
	}
	endTime := time.Now().UnixNano()
	duration := (endTime - startTime) / 1000000 // In milliseconds
	var mbps float64
	var rate int
	if duration != 0 {
		mbps = float64(1024*receivedData*8) / float64(endTime-startTime) // In Mbps
		rate = 1000 * received / int(duration)
	} else {
		mbps = 0
		rate = 0
	}

	logger.Info().
		Int32("running", atomic.LoadInt32(&running)).
		Int("numRequests", received).
		Int("channel", 0).
		Int64("duration", duration).
		Int("rate", rate).
		Float64("Mbps", mbps).
		Msg("Done Receiving.")

	statsLock.Lock()
	totalReq0 += int64(received)
	cumTime0 += duration
	bandwidth += mbps
	connections0++
	currentConnections--
	statsLock.Unlock()

	return nil
}

func (ts *contentionTest1Server) TestContention1(srv pb.ContentionTest1_TestContention1Server) error {

	statsLock.Lock()
	currentConnections++
	statsLock.Unlock()

	logger.Info().Msg("Incoming test connection (1).")

	startTime := time.Now().UnixNano()
	received := 0
	receivedData := 0
	for req, err := srv.Recv(); atomic.LoadInt32(&running) == 1 && err == nil; _, err = srv.Recv() {

		// Increment (local) stats variables
		receivedData += len(req.Payload)
		received++

		// Do dummy work
		key := rand.Intn(mapKeySpace)

		if numMaps > 0 {
			idx := rand.Intn(numMaps)
			plainMapLocks[idx].Lock()
			plainMaps[idx][key] = true
			plainMapLocks[idx].Unlock()
		}
		val, _ := syncMap.Load(key)
		syncMap.Store(key/2, val == nil)
	}
	endTime := time.Now().UnixNano()
	duration := (endTime - startTime) / 1000000 // In milliseconds
	var mbps float64
	var rate int
	if duration != 0 {
		mbps = float64(1024*receivedData*8) / float64(endTime-startTime) // In mbps
		rate = 1000 * received / int(duration)
	} else {
		mbps = 0
		rate = 0
	}

	logger.Info().
		Int32("running", atomic.LoadInt32(&running)).
		Int("numRequests", received).
		Int("channel", 1).
		Int64("duration", duration).
		Int("rate", rate).
		Float64("mbps", mbps).
		Msg("Done Receiving.")

	statsLock.Lock()
	totalReq1 += int64(received)
	cumTime1 += duration
	bandwidth += mbps
	connections1++
	currentConnections--
	statsLock.Unlock()

	return nil
}

func runServer(wg *sync.WaitGroup) (*grpc.Server, *grpc.Server, *grpc.Server) {
	// Depending on configuration, create a plain or TLS-enabled gRPC server
	var grpcServer0 *grpc.Server
	var grpcServer1 *grpc.Server
	var grpcControlServer *grpc.Server
	if useTLS {
		tlsConfig := messenger.ConfigureTLS(config.Config.CertFile, config.Config.KeyFile)
		grpcServer0 = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
		grpcServer1 = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
		grpcControlServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		grpcServer0 = grpc.NewServer()
		grpcServer1 = grpc.NewServer()
		grpcControlServer = grpc.NewServer()
	}

	// Assign logic to the gRPC server.
	pb.RegisterContentionTest0Server(grpcServer0, &contentionTest0Server{})
	pb.RegisterContentionTest1Server(grpcServer1, &contentionTest1Server{})
	pb.RegisterContentionTestControlServer(grpcControlServer, &contentionTestControlServer{})

	// Start listening on the network
	conn0, err := net.Listen("tcp", ":"+strconv.Itoa(port0))
	if err != nil {
		logger.Fatal().Int("port", port0).Err(err).Msg("Failed to listen for connections.")
		os.Exit(1)
	}
	conn1, err := net.Listen("tcp", ":"+strconv.Itoa(port1))
	if err != nil {
		logger.Fatal().Int("port", port1).Err(err).Msg("Failed to listen for connections.")
		os.Exit(1)
	}
	controlConn, err := net.Listen("tcp", ":"+strconv.Itoa(controlPort))
	if err != nil {
		logger.Fatal().Int("port", controlPort).Err(err).Msg("Failed to listen for connections.")
		os.Exit(1)
	}

	// Start the gRPC servers
	go func() {
		if err := grpcServer0.Serve(conn0); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start gRPC server.")
		}
		wg.Done()
	}()
	go func() {
		if err := grpcServer1.Serve(conn1); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start gRPC server.")
		}
		wg.Done()
	}()
	go func() {
		if err := grpcControlServer.Serve(controlConn); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start gRPC server.")
		}
		wg.Done()
	}()

	return grpcServer0, grpcServer1, grpcControlServer
}

func runClient(clID int, serverAddr string, channel int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create logger specific to this client.
	clientLog := logger.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "15:04:05.000"}).With().Int("clId", clID).Logger()

	clientLog.Info().Bool("TLS", useTLS).Msg("Starting client.")

	var msgSink0 pb.ContentionTest0_TestContention0Client
	var msgSink1 pb.ContentionTest1_TestContention1Client
	if channel == 0 {
		if msgSink0 = invokeTestRPC0(pb.NewContentionTest0Client(newClientConnection(fmt.Sprintf("%s:%d", serverAddr, port0)))); msgSink0 == nil {
			clientLog.Fatal().Msg("Could not connect to test server (channel 0).")
		}
	} else {
		if msgSink1 = invokeTestRPC1(pb.NewContentionTest1Client(newClientConnection(fmt.Sprintf("%s:%d", serverAddr, port1)))); msgSink1 == nil {
			clientLog.Fatal().Msg("Could not connect to test server (channel 1).")
		}
	}

	clientLog.Info().Int("payloadSize", payloadSize).Int("channel", channel).Int("t", timeout).Msg("Sending requests.")
	startTime := time.Now().UnixNano()
	requestsSent := 0
	for time.Duration(time.Now().UnixNano()-startTime)*time.Nanosecond <= time.Duration(timeout)*time.Second {
		if channel == 0 {
			if !sendRequest0(requestsSent, msgSink0) {
				break
			}
			requestsSent++
		} else {
			if !sendRequest1(requestsSent, msgSink1) {
				break
			}
			requestsSent++
		}
	}
	endTime := time.Now().UnixNano()
	duration := (endTime - startTime) / 1000000                              // In milliseconds
	mbps := float64(payloadSize*requestsSent*8) / float64(endTime-startTime) // In mbps
	clientLog.Info().
		Int("payloadSize", payloadSize).
		Int("numRequests", requestsSent).
		Int64("duration", duration).
		Float64("mbps", mbps).
		Msg("Done sending requests.")

	if channel == 0 {
		if err := msgSink0.CloseSend(); err != nil {
			clientLog.Error().Err(err).Str("serverAddr", serverAddr).Msg("Failed closing connection (0) to server.")
		}
	}
	if channel == 1 {
		if err := msgSink1.CloseSend(); err != nil {
			clientLog.Error().Err(err).Str("serverAddr", serverAddr).Msg("Failed closing connection (1) to server.")
		}
	}
}

func newClientConnection(addr string) *grpc.ClientConn {
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

	// Set up a gRPC connection.
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		logger.Error().Err(err).Str("addrStr", addr).Msg("Could not connect to gRPC server.")
		return nil
	}

	return conn
}

func invokeTestRPC0(client pb.ContentionTest0Client) pb.ContentionTest0_TestContention0Client {
	// Remotely invoke the Test() function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.TestContention0(context.Background())
	if err != nil {
		logger.Error().Err(err).Msg("Could not invoke contention test 0.")
		return nil
	}

	// Return the message sink connected to the peer.
	return msgSink
}

func invokeTestRPC1(client pb.ContentionTest1Client) pb.ContentionTest1_TestContention1Client {
	// Remotely invoke the Test() function on the other node's gRPC server.
	// As this is "stream of requests"-type RPC, it returns a message sink.
	msgSink, err := client.TestContention1(context.Background())
	if err != nil {
		logger.Error().Err(err).Msg("Could not invoke contention test 1.")
		return nil
	}

	// Return the message sink connected to the peer.
	return msgSink
}

func sendRequest0(sn int, msgSink pb.ContentionTest0_TestContention0Client) bool {
	req := pb.ClientRequest{
		RequestId: &pb.RequestID{
			ClientId: -1,
			ClientSn: int32(sn),
		},
		Payload:   randomBytes,
		Pubkey:    nil,
		Signature: nil,
	}

	err := msgSink.Send(&req)
	if err == io.EOF {
		logger.Info().Msg("Request stream closed by server.")
		return false
	} else if err != nil {
		logger.Fatal().Err(err).Msg("Could not send request on network.")
		return false
	}
	return true
}

func sendRequest1(sn int, msgSink pb.ContentionTest0_TestContention0Client) bool {
	req := pb.ClientRequest{
		RequestId: &pb.RequestID{
			ClientId: -1,
			ClientSn: int32(sn),
		},
		Payload:   randomBytes,
		Pubkey:    nil,
		Signature: nil,
	}

	err := msgSink.Send(&req)
	if err == io.EOF {
		logger.Info().Msg("Request stream closed by server.")
		return false
	} else if err != nil {
		logger.Fatal().Err(err).Msg("Could not send request on network.")
		return false
	}
	return true
}
