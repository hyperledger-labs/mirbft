package main

import (
	cr "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/config"
	"github.com/hyperledger-labs/mirbft/crypto"
	"github.com/hyperledger-labs/mirbft/profiling"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"github.com/hyperledger-labs/mirbft/request"
	"github.com/hyperledger-labs/mirbft/tracing"
)

func main() {

	// Configure logger
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// TLS connection test
	//go server()
	//
	//time.Sleep(time.Second)
	//
	//go client()
	//
	//time.Sleep(time.Second)

	// Signature verification test
	//pubKey, privKey := keysFromFiles("tls-data/client-ecdsa-224.pem", "tls-data/client-ecdsa-224.key")
	//testVerifySignature(pubKey, privKey)

	// Test exiting process from a goroutine.
	//wg := sync.WaitGroup{}
	//wg.Add(1)
	//go func() {
	//	logger.Info().Msg("Goroutine started.")
	//	os.Exit(0)
	//
	//	// Infinite loop
	//	val := 0
	//	for {
	//		val += 1
	//	}
	//
	//	logger.Info().Msg("Goroutine Finished.")
	//	wg.Done()
	//}()
	//wg.Wait()

	//fmt.Printf("% 14s % 14s % 14s % 14s % 14s % 14s % 14s % 14s\n",
	//	"nClients",
	//	"nReq",
	//	"initFlat",
	//	"initStruct",
	//	"initNested",
	//	"lookupFlat",
	//	"lookupStruct",
	//	"lookupNested")
	//testMapLookpus(128, 1000)
	//testMapLookpus(128, 10000)
	//testMapLookpus(128, 100000)
	//testMapLookpus(512, 1000)
	//testMapLookpus(512, 10000)
	//testMapLookpus(512, 100000)

	//testGoprocinfo()

	//testArrayFillingVals(5000000)
	//testArrayFillingStruct(5000000)
	//testArrayFillingPtr(5000000)

	//testBatchCutting()

	testBatchVerification(1024, 32, []int{2, 4, 24, 30, 32})
}

func testBatchVerification(batchSize int, nBatches int, numVerifiers []int) {

	logger.Info().
		Int("batchSize", batchSize).
		Int("nBatches", nBatches).
		Interface("numVerifiers", numVerifiers).
		Msg("Initializing.")

	// Init
	privKey := loadPrivKey("tls-data/client-ecdsa-256.key")
	pubKey := loadPubKey("tls-data/client-ecdsa-256.pem")
	batchesWarmup := make([]*request.Batch, nBatches, nBatches)
	batchesParallel := make([]*request.Batch, nBatches, nBatches)
	batchesSequential := make([]*request.Batch, nBatches, nBatches)
	batchesExternal := make(map[int][]*request.Batch, len(numVerifiers))
	for _, n := range numVerifiers {
		batchesExternal[n] = make([]*request.Batch, nBatches, nBatches)
	}

	logger.Info().Msg("Creating and signing batches.")

	// Create batches
	var wg sync.WaitGroup
	wg.Add(3 + len(numVerifiers))
	go func() {
		for i := 0; i < nBatches; i++ {
			batchesWarmup[i] = createBatch(batchSize, privKey)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < nBatches; i++ {
			batchesSequential[i] = createBatch(batchSize, privKey)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < nBatches; i++ {
			batchesParallel[i] = createBatch(batchSize, privKey)
		}
		wg.Done()
	}()
	for _, n := range numVerifiers {
		go func(nv int) {
			for i := 0; i < nBatches; i++ {
				batchesExternal[nv][i] = createBatch(batchSize, privKey)
			}
			wg.Done()
		}(n)
	}
	wg.Wait()

	logger.Info().Msg("Warming up by one check of each type.")
	verifySequential(batchesWarmup, pubKey)
	verifyParallel(batchesWarmup, pubKey)
	verifierChan, vwg := startVerifiers(32, pubKey, nBatches*batchSize)
	verifyExternal(batchesWarmup, verifierChan, 32)
	stopVerifiers(verifierChan, vwg)

	logger.Info().Msg("Checking signatures.")

	// Perform test
	verifySequential(batchesSequential, pubKey)
	verifyParallel(batchesParallel, pubKey)
	for _, n := range numVerifiers {
		verifierChan, wg := startVerifiers(n, pubKey, nBatches*batchSize)
		verifyExternal(batchesExternal[n], verifierChan, n)
		stopVerifiers(verifierChan, wg)
	}
}

func startVerifiers(n int, pubKey interface{}, bufSize int) (chan *request.Request, *sync.WaitGroup) {

	inChan := make(chan *request.Request, bufSize)
	wg := &sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go verifyRequests(inChan, pubKey, wg)
	}

	return inChan, wg
}

func stopVerifiers(verifierChan chan *request.Request, wg *sync.WaitGroup) {
	close(verifierChan)
	wg.Wait()
}

func verifyRequests(inChan chan *request.Request, pubKey interface{}, wg *sync.WaitGroup) {
	for req := range inChan {
		if err := crypto.CheckSig(req.Digest, pubKey, req.Msg.Signature); err == nil {
			req.Verified = true
		}
		req.VerifiedChan <- req
	}

	wg.Done()
}

func verifySequential(batches []*request.Batch, pubKey interface{}) {
	var wg sync.WaitGroup
	wg.Add(len(batches))

	start := time.Now()

	for _, b := range batches {
		go func(batch *request.Batch) {
			verifyBatchSequential(batch, pubKey)
			wg.Done()
		}(b)
	}
	wg.Wait()

	duration := time.Since(start)
	logger.Info().
		Dur("dur", duration).
		Msg("Sequential.")
}

func verifyParallel(batches []*request.Batch, pubKey interface{}) {
	var wg sync.WaitGroup
	wg.Add(len(batches))

	start := time.Now()

	for _, b := range batches {
		go func(batch *request.Batch) {
			verifyBatchParallel(batch, pubKey)
			wg.Done()
		}(b)
	}

	wg.Wait()

	duration := time.Since(start)
	logger.Info().
		Dur("dur", duration).
		Msg("Parallel.")
}

func verifyExternal(batches []*request.Batch, verifierChan chan *request.Request, n int) {
	var wg sync.WaitGroup
	wg.Add(len(batches))

	start := time.Now()

	for _, b := range batches {
		go func(batch *request.Batch) {
			verifyBatchExternal(batch, verifierChan)
			wg.Done()
		}(b)
	}

	wg.Wait()

	duration := time.Since(start)
	logger.Info().
		Dur("dur", duration).
		Int("numVerifiers", n).
		Msg("External.")
}

func verifyBatchSequential(b *request.Batch, pubKey interface{}) {
	for _, req := range b.Requests {
		req.Verified = false
		if err := crypto.CheckSig(req.Digest,
			pubKey,
			req.Msg.Signature); err != nil {
			logger.Warn().
				Err(err).
				Int32("clSn", req.Msg.RequestId.ClientSn).
				Int32("clId", req.Msg.RequestId.ClientId).
				Msg("Invalid request signature.")
		} else {
			req.Verified = true
		}
	}
}

func verifyBatchParallel(b *request.Batch, pubKey interface{}) {
	var wg sync.WaitGroup
	wg.Add(len(b.Requests))

	for _, r := range b.Requests {
		r.Verified = false
		go func(req *request.Request) {
			if err := crypto.CheckSig(req.Digest,
				pubKey,
				req.Msg.Signature); err != nil {
				logger.Warn().
					Err(err).
					Int32("clSn", req.Msg.RequestId.ClientSn).
					Int32("clId", req.Msg.RequestId.ClientId).
					Msg("Invalid request signature.")
			} else {
				req.Verified = true
			}
			wg.Done()
		}(r)
	}

	wg.Wait()
}

func verifyBatchExternal(b *request.Batch, verifierChan chan *request.Request) {
	verifiedChan := make(chan *request.Request, len(b.Requests))

	verifying := 0
	for _, r := range b.Requests {
		verifying++
		r.VerifiedChan = verifiedChan
		r.Verified = false
		verifierChan <- r
	}

	for verifying > 0 {
		verifying--
		req := <-verifiedChan
		req.VerifiedChan = nil
		if !req.Verified {
			logger.Warn().
				Int32("clSn", req.Msg.RequestId.ClientSn).
				Int32("clId", req.Msg.RequestId.ClientId).
				Msg("Request signature verification failed.")
		}
	}
}

func createBatch(nReq int, privKey interface{}) *request.Batch {

	newBatch := &request.Batch{Requests: make([]*request.Request, nReq, nReq)}
	for i := 0; i < nReq; i++ {

		reqMsg := createRequest(int32(i), privKey)
		req := &request.Request{
			Msg:      reqMsg,
			Digest:   request.Digest(reqMsg),
			Buffer:   nil,   // Dummy value
			Bucket:   nil,   // Dummy value
			Verified: false, // signature has not yet been verified
			InFlight: false, // request has not yet been proposed (an identical one might have been, though, in which case we discard this request object)
			Next:     nil,   // This request object is not part of a bucket list.
			Prev:     nil,
		}
		newBatch.Requests[i] = req
	}

	return newBatch
}

func createRequest(seqNr int32, privKey interface{}) *pb.ClientRequest {

	// Generate random request payload
	randomRequestPayload := make([]byte, config.Config.RequestPayloadSize)
	rand.Read(randomRequestPayload)

	// Create request message.
	req := &pb.ClientRequest{
		RequestId: &pb.RequestID{
			ClientId: 0,
			ClientSn: seqNr,
		},
		Payload:   randomRequestPayload,
		Signature: nil,
	}

	// Sign request message.
	var err error = nil
	req.Signature, err = crypto.Sign(request.Digest(req), privKey)
	if err != nil {
		logger.Error().Err(err).Int32("clSn", seqNr).Msg("Failed signing request.")
	}
	// TODO: Add public key to request or remove the Pubkey request field.

	return req
}

// Loads private key for signing requests
func loadPrivKey(privKeyFile string) interface{} {
	privKey, err := crypto.PrivateKeyFromFile(privKeyFile)
	if err != nil {
		logger.Error().
			Err(err).
			Str("fileName", config.Config.ClientPrivKeyFile).
			Msg("Could not load client private key from file.")
	}
	return privKey
}

func loadPubKey(pubKeyFile string) interface{} {
	if pk, err := crypto.PublicKeyFromFile(pubKeyFile); err == nil {
		return pk
	} else {
		logger.Error().
			Err(err).
			Str("keyFile", config.Config.ClientPubKeyFile).
			Msg("Could not load client public key.")
		return nil
	}
}

func testBatchCutting() {
	// Create buckets and a buffer
	b0 := request.NewBucket(0)
	b1 := request.NewBucket(1)
	buf := request.NewBuffer(0)

	request.Buckets = make([]*request.Bucket, 2)
	request.Buckets[0] = b0
	request.Buckets[1] = b1

	// Fill buckets with requests
	for i := 0; i < 2048; i++ {
		reqMsg := &pb.ClientRequest{
			RequestId: &pb.RequestID{
				ClientId: 0,
				ClientSn: int32(i),
			},
			Payload:   nil,
			Pubkey:    nil,
			Signature: nil,
		}
		b0.AddRequest(&request.Request{
			Msg:      reqMsg,
			Digest:   request.Digest(reqMsg),
			Buffer:   buf,
			Bucket:   b0,
			Verified: true,
			InFlight: false,
			Next:     nil,
			Prev:     nil,
		})
	}

	bg := request.NewBucketGroup([]int{0, 1})

	logger.Info().Int("len", bg.CountRequests()).Msg("Bucket group created.")

	batch := bg.CutBatch(1024, 0)

	logger.Info().Int("len", len(batch.Requests)).Msg("Batch cut")
}

func testMapLookpus(numClients int32, numRequests int32) {

	type reqID struct {
		clID int32
		clSN int32
	}

	// Create dummy request to put in maps.
	dummyRequest := &request.Request{
		Digest: make([]byte, 1, 1),
	}

	// Initialize maps
	flatMap := make(map[int64]*request.Request)
	nestedMap := make(map[int32]map[int32]*request.Request)
	structMap := make(map[reqID]*request.Request)

	// Fill flat map
	start := time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			reqID := int64(clID)<<32 + int64(clSN)
			flatMap[reqID] = dummyRequest
		}
	}
	initFlatTime := time.Since(start)

	// Fill nested map
	start = time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			clMap, ok := nestedMap[clID]
			if !ok {
				clMap = make(map[int32]*request.Request)
				nestedMap[clID] = clMap
			}
			clMap[clSN] = dummyRequest
		}
	}
	initNestedTime := time.Since(start)

	// Fill struct map
	start = time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			reqID := reqID{
				clID: clID,
				clSN: clSN,
			}
			structMap[reqID] = dummyRequest
		}
	}
	initStructTime := time.Since(start)

	// Look up in flat map
	start = time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			reqID := int64(clID)<<32 + int64(clSN)
			req := flatMap[reqID]
			req.Digest[0]++
		}
	}
	lookupFlatTime := time.Since(start)

	// Look up in nested map
	start = time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			clMap, ok := nestedMap[clID]
			if !ok {
				panic("Client entry in nested map not found.")
			}
			req := clMap[clSN]
			req.Digest[0]++
		}
	}
	lookupNestedTime := time.Since(start)

	// Look up in flat map
	start = time.Now()
	for clSN := int32(0); clSN < numRequests; clSN++ {
		for clID := int32(0); clID < numClients; clID++ {
			reqID := reqID{
				clID: clID,
				clSN: clSN,
			}
			req := structMap[reqID]
			req.Digest[0]++
		}
	}
	lookupStructTime := time.Since(start)

	fmt.Printf("% 14d % 14d % 14d % 14d % 14d % 14d %14d %14d\n",
		numClients,
		numRequests,
		initFlatTime.Nanoseconds()/1000000,
		initStructTime.Nanoseconds()/1000000,
		initNestedTime.Nanoseconds()/1000000,
		lookupFlatTime.Nanoseconds()/1000000,
		lookupStructTime.Nanoseconds()/1000000,
		lookupNestedTime.Nanoseconds()/1000000,
	)
}

type dummyType struct {
	EventType tracing.EventType
	Timestamp int64
	ClId      int32
	ClSn      int32
	PeerId    int32
	Val0      int64
	Val1      int64
	Val2      int64
	Val3      int64
	Val4      int64
	Val5      int64
	Val6      int64
	Val7      int64
	Val8      int64
	Val9      int64
	//DummyStr string
	//DummyStruct interface{}
}

func testArrayFillingPtr(length int) {

	// Start measuring time
	start := time.Now()

	// Pre-allocate array
	buffer := make([]*dummyType, length, length)
	allocated := time.Now()

	for i := 0; i < length; i++ {
		buffer[i] = &dummyType{
			EventType: tracing.REQ_SEND,
			Timestamp: int64(i),
			ClId:      int32(i),
			ClSn:      int32(i),
			PeerId:    int32(i),
			Val0:      int64(i),
			Val1:      int64(i),
			Val2:      int64(i),
			Val3:      int64(i),
			Val4:      int64(i),
			Val5:      int64(i),
			Val6:      int64(i),
			Val7:      int64(i),
			Val8:      int64(i),
			Val9:      int64(i),
			//DummyStr:  "",
		}
	}
	filled := time.Now()

	logger.Info().TimeDiff("alloc", allocated, start).Msg("Ptr")
	logger.Info().TimeDiff("fill", filled, allocated).Msg("Ptr")
}

func testArrayFillingStruct(length int) {

	// Start measuring time
	start := time.Now()

	// Pre-allocate array
	buffer := make([]dummyType, length, length)
	allocated := time.Now()

	for i := 0; i < length; i++ {
		buffer[i] = dummyType{
			EventType: tracing.REQ_SEND,
			Timestamp: int64(i),
			ClId:      int32(i),
			ClSn:      int32(i),
			PeerId:    int32(i),
			Val0:      int64(i),
			Val1:      int64(i),
			Val2:      int64(i),
			Val3:      int64(i),
			Val4:      int64(i),
			Val5:      int64(i),
			Val6:      int64(i),
			Val7:      int64(i),
			Val8:      int64(i),
			Val9:      int64(i),
			//DummyStr:  "",
		}
	}
	filled := time.Now()

	logger.Info().TimeDiff("alloc", allocated, start).Msg("Struct")
	logger.Info().TimeDiff("fill", filled, allocated).Msg("Struct")
}

func testArrayFillingVals(length int) {

	// Start measuring time
	start := time.Now()

	// Pre-allocate array
	buffer := make([]dummyType, length, length)
	allocated := time.Now()

	for i := 0; i < length; i++ {
		buffer[i].EventType = tracing.REQ_SEND
		buffer[i].Timestamp = int64(i)
		buffer[i].ClId = int32(i)
		buffer[i].ClSn = int32(i)
		buffer[i].PeerId = int32(i)
		buffer[i].Val0 = int64(i)
		buffer[i].Val1 = int64(i)
		buffer[i].Val2 = int64(i)
		buffer[i].Val3 = int64(i)
		buffer[i].Val4 = int64(i)
		buffer[i].Val5 = int64(i)
		buffer[i].Val6 = int64(i)
		buffer[i].Val7 = int64(i)
		buffer[i].Val8 = int64(i)
		buffer[i].Val9 = int64(i)
		//buffer[i].DummyStr =  ""
	}
	filled := time.Now()

	logger.Info().TimeDiff("alloc", allocated, start).Msg("Vals")
	logger.Info().TimeDiff("fill", filled, allocated).Msg("Vals")
}

func testGoprocinfo() {
	// Test goprocinfo
	numCPUBurners := 4
	go func() {
		for {
			usage := profiling.GetCPUUsage([]string{"Load", "System", "IOWait"}, 500*time.Millisecond)
			logger.Info().Float32("Load", usage[0]).Msg("Usage")
			logger.Info().Float32("System", usage[1]).Msg("Usage")
			logger.Info().Float32("IOWait", usage[2]).Msg("Usage")
		}
	}()

	for i := 0; i < numCPUBurners; i++ {
		go func() {
			logger.Info().Msg("Worker started.")
			for {
				time.Sleep(0 * time.Nanosecond) // Required for the go scheduler to be able to preempt this goroutine.
			}
		}()
		time.Sleep(2 * time.Second)
	}
	logger.Info().Msg("Done.")
}

func keysFromFiles(pubKeyFile string, privKeyFile string) (pubKey interface{}, privKey interface{}) {
	var pub, priv interface{} = nil, nil
	var err error = nil
	if pub, err = crypto.PublicKeyFromFile(pubKeyFile); err != nil {
		logger.Fatal().
			Err(err).
			Str("keyFile", pubKeyFile).
			Msg("Could not load public key from file.")
		return nil, nil
	}
	priv, err = crypto.PrivateKeyFromFile(privKeyFile)
	if err != nil {
		logger.Fatal().
			Err(err).
			Str("fileName", privKeyFile).
			Msg("Could not load private key from file.")
		return nil, nil
	}
	return pub, priv
}

func testVerifySignature(pubKey interface{}, privKey interface{}) {
	data := []byte("Signed message.")
	digest := crypto.Hash(data)

	signature, err := crypto.Sign(digest, privKey)
	if err != nil {
		logger.Error().Err(err).Msg("Failed signing message.")
	}
	logger.Info().Int("sigLength", len(signature)).Msg("Message signed.")

	if err := crypto.CheckSig(digest, pubKey, signature); err != nil {
		logger.Error().Err(err).Msg("Invalid request signature.")
	} else {
		logger.Info().Msg("Signature valid.")
	}

}

// TLS connection test client.
func client() {
	cert, err := tls.LoadX509KeyPair("tls-data/auth.pem", "tls-data/auth.key")
	if err != nil {
		log.Fatalf("server: loadkeys: %s", err)
	}

	b, _ := ioutil.ReadFile("tls-data/ca.pem")
	certpool := x509.NewCertPool()
	if !certpool.AppendCertsFromPEM(b) {
		log.Fatalf("credentials: failed to append certificates")
	}

	config := tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: false, // TODO: The way we use it here, the server never sends any messages. Try skipping verification.
		RootCAs:            certpool,
	}
	conn, err := tls.Dial("tcp", "127.0.0.1:8000", &config)
	if err != nil {
		log.Fatalf("client: dial: %s", err)
	}
	defer conn.Close()
	log.Println("client: connected to: ", conn.RemoteAddr())
	state := conn.ConnectionState()
	for _, v := range state.PeerCertificates {
		fmt.Println("Client: Server public key is:")
		fmt.Println(x509.MarshalPKIXPublicKey(v.PublicKey))
	}
	log.Println("client: handshake: ", state.HandshakeComplete)
	log.Println("client: mutual: ", state.NegotiatedProtocolIsMutual)
	message := "Hello\n"
	n, err := io.WriteString(conn, message)
	if err != nil {
		log.Fatalf("client: write: %s", err)
	}
	log.Printf("client: wrote %q (%d bytes)", message, n)
	reply := make([]byte, 256)
	n, err = conn.Read(reply)
	log.Printf("client: read %q (%d bytes)", string(reply[:n]), n)
	log.Print("client: exiting")
}

// TLS connection test server.
func server() {
	cert, err := tls.LoadX509KeyPair("tls-data/auth.pem", "tls-data/auth.key")
	if err != nil {
		log.Fatalf("server: loadkeys: %s", err)

	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		log.Fatal("Error parsing server certificate.")
	}

	certpool := x509.NewCertPool()
	pem, err := ioutil.ReadFile("tls-data/ca.pem")
	if err != nil {
		log.Fatalf("Failed to read client certificate authority: %v", err)
	}
	if !certpool.AppendCertsFromPEM(pem) {
		log.Fatalf("Can't parse client certificate authority")
	}

	tlsConfig := &tls.Config{
		Rand:         cr.Reader,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certpool,
	}

	service := "0.0.0.0:8000"
	listener, err := tls.Listen("tcp", service, tlsConfig)
	if err != nil {
		log.Fatalf("server: listen: %s", err)
	}
	log.Print("server: listening")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("server: accept: %s", err)
			break
		}
		log.Printf("server: accepted from %s", conn.RemoteAddr())
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	tlscon, ok := conn.(*tls.Conn)
	if ok {
		log.Print("server: conn: type assert to TLS succeedded")
		err := tlscon.Handshake()
		if err != nil {
			log.Fatalf("server: handshake failed: %s", err)
		} else {
			log.Print("server: conn: Handshake completed")
		}
		state := tlscon.ConnectionState()
		log.Println("Server: client public key is:")
		for _, v := range state.PeerCertificates {
			log.Print(x509.MarshalPKIXPublicKey(v.PublicKey))
		}
		buf := make([]byte, 512)
		for {
			log.Print("server: conn: waiting")
			n, err := conn.Read(buf)
			if err != nil {
				if err != nil {
					log.Printf("server: conn: read: %s", err)
				}
				break

			}
			log.Printf("server: conn: echo %q\n", string(buf[:n]))
			n, err = conn.Write(buf[:n])
			log.Printf("server: conn: wrote %d bytes", n)
			if err != nil {
				log.Printf("server: write: %s", err)
				break
			}
		}
	}
	log.Println("server: conn: closed")
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
