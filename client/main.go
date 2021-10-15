/*
Copyright IBM Corp. 2018 All Rights Reserved.

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

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"sync/atomic"

	"github.com/IBM/mirbft/config"
	pb "github.com/IBM/mirbft/protos"
	"github.com/IBM/mirbft/tracing"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var log = logging.MustGetLogger("client")
var format = logging.MustStringFormatter(
	`%{time:2006/01/02 15:04:05.000000} %{shortfunc} %{message}`,
)

func main() {
	// Client setup
	configFile := arg(1)
	config.LoadFile(configFile)

	// Get output file prefix from command line.
	// Log and trace files will all begin with this
	outFilePrefix := os.Args[2]

	// Open status file
	status_file, err := os.Create("STATUS.sh")
	fatal(err)
	defer status_file.Close()

	status_file.WriteString("status=UP\n")

	logBackend := logging.NewLogBackend(os.Stdout, "", 0)
	backendFormatter := logging.NewBackendFormatter(logBackend, format)
	logging.SetBackend(backendFormatter)

	if config.Config.Logging == "debug" {
		logging.SetLevel(logging.DEBUG, "client")
	}
	if config.Config.Logging == "info" {
		logging.SetLevel(logging.INFO, "client")
	}
	if config.Config.Logging == "error" {
		logging.SetLevel(logging.ERROR, "client")
	}
	if config.Config.Logging == "critical" {
		logging.SetLevel(logging.CRITICAL, "client")
	}

	osns := make([]pb.ConsensusClient, 0)
	grpcConns := make([]*grpc.ClientConn, 0)

	id := config.Config.Id
	N := config.Config.N
	F := config.Config.F

	payloadSize := config.Config.RequestSize
	signed := config.Config.SignatureVerification
	useTLS := config.Config.UseTLS

	total := config.Config.RequestsPerClient
	sendParallelism := config.Config.Parallelism
	timeout := int64(0)
	if config.Config.RequestRate > 0 {
		timeout = int64(1000000000 / config.Config.RequestRate)
	}
	clients := config.Config.Clients

	leaderRotationDist := config.Config.BucketRotationPeriod
	batchSize := config.Config.BatchSizeBytes
	buckets := config.Config.Buckets

	receivers := config.Config.Receivers
	broadcast := config.Config.Broadcast
	dst := config.Config.Destination

	caCertFile := config.Config.Servers.CACertFile
	requestsPerBatch := batchSize / payloadSize
	period := requestsPerBatch * leaderRotationDist
	payload := make([]byte, payloadSize, payloadSize)
	makeParallelism := runtime.NumCPU()
	txNo := total / makeParallelism

	c, err := New(id, N, F, receivers, buckets, period, dst, int64(total),
		osns, caCertFile, useTLS)

	c.trace.Start(fmt.Sprintf("%s-%03d.trc", outFilePrefix, c.id), int32(c.id))
	defer c.trace.Stop()

	log.Infof("Preparing requests...")

	for j := 0; j < txNo; j++ {
		var threads sync.WaitGroup
		threads.Add(makeParallelism)
		for p := 0; p < makeParallelism; p++ {
			go func(j int) {
				defer threads.Done()
				seq := uint64(j)
				select {
				case c.queue <- c.makeRequest(payload, seq, signed):
				default:
				}
			}(j*makeParallelism + p)
		}
		threads.Wait()
	}

	if config.Config.Blocking {
		log.Infof("Requests added to queue. Bring up the servers and press enter...")
		fmt.Scanln()
	}

	for i, addr := range config.Config.Servers.Addresses {
		conn, err := c.AddOSN(addr)
		grpcConns = append(grpcConns, conn)
		defer grpcConns[i].Close()
		fatal(err)
	}

	waitc := make([]chan struct{}, c.n)

	for inc := c.dst; inc < c.n+dst; inc++ {
		i := inc % c.n
		log.Infof("Opening stream to server %d", i)
		c.stream[i], err = c.servers[i].Request(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		waitc[i] = make(chan struct{})
		time.Sleep(500 * time.Millisecond)

		go func(i int) {
			for {
				resp, err := c.stream[i].Recv()
				if err == io.EOF {
					// read done.
					close(waitc[i])
					return
				}
				if err != nil {
					log.Fatalf("Failed to receive a note : %v", err)
				}
				if fmt.Sprintf("%s", resp.Response) == "DELIVERED" {
					// Request delivered
					log.Debugf("DELIVERED %d from %d", resp.Request.Seq, resp.Src)
					// Check if registered to all severs
					c.registrationsLock.Lock()
					if ok, _ := c.registrations[resp.Src]; !ok {
						c.registrations[resp.Src] = true
						if len(c.registrations) == N {
							c.registered <- true
						}
					}
					c.registrationsLock.Unlock()

					atomic.AddInt32(&c.delivered[resp.Request.Seq], 1)
					c.sentTimestampsLock.RLock()
					c.trace.Event(tracing.RESP_RECEIVE, int64(int32(id)), time.Now().UnixNano()/1000-c.sentTimestamps[uint64(id)])
					c.sentTimestampsLock.RUnlock()
					if atomic.LoadInt32(&c.delivered[resp.Request.Seq]) == int32(F+1) {
						c.sentTimestampsLock.RLock()
						c.trace.Event(tracing.ENOUGH_RESP, int64(int32(id)), time.Now().UnixNano()/1000-c.sentTimestamps[uint64(id)])
						c.sentTimestampsLock.RUnlock()
						c.submitTimestampsLock.RLock()
						c.trace.Event(tracing.REQ_FINISHED, int64(int32(id)), time.Now().UnixNano()/1000-c.submitTimestamps[uint64(id)])
						c.submitTimestampsLock.RUnlock()
						log.Debugf("Finished %d", resp.Request.Seq)
						c.checkInOrderDelivery(int64(resp.Request.Seq))
						if atomic.LoadInt64(&c.lastDelivered) == c.numRequests-1 {
							log.Infof("ALL DELIVERED")
							atomic.StoreInt32(&c.stop, 1)
						}
					}
				} else if fmt.Sprintf("%s", resp.Response) == "ACK" {
					// Request added to pending on server side
					log.Debugf("ACK")
				} else if fmt.Sprintf("%s", resp.Response) == "BUCKETS" {

				} else {
					// Request could not be processed, put request back in the queue
					log.Infof("client: received %s from %d", fmt.Sprintf("%s", resp.Response), resp.Src)
					c.queue <- c.makeRequest(payload, resp.Request.Seq, signed)
				}
			}
		}(i)
	}

	log.Infof("REGISTERING")
	c.register()

	<-c.registered

	log.Infof("START")
	done := make(chan bool)

	// Make client stop after a predefined time, if configured
	if config.Config.ClientRunTime != 0 {
		log.Infof("Setting up client timeout.")
		time.AfterFunc(time.Duration(config.Config.ClientRunTime)*time.Millisecond, func() {
			atomic.StoreInt32(&c.stop, 1) // A non-zero value of the stop variable halts the request submissions.
			log.Infof("Stopping client on timeout.")
			done <-true
		})
	}

	sent := int32(clients)

	go func() {
		for atomic.LoadInt32(&c.stop) == 0 {
			for i := 0; i < sendParallelism; i++ {
				request, more := <- c.queue
				if !more {
					if atomic.LoadInt32(&c.stop) == 1{
						done <- true
						return
					}
				}
				if broadcast {
					c.broadcastRequest(request)
				} else {
					c.sendRequest(request)
				}
				c.submitTimestampsLock.Lock()
				c.submitTimestamps[request.request.Seq] = time.Now().UnixNano() / 1000 // In us
				c.submitTimestampsLock.Unlock()
				atomic.AddInt32(&sent, int32(clients))
				if atomic.LoadInt32(&sent)%int32(c.period)== 0 {
					log.Infof("Rotating buckets")
					c.rotateBuckets()
				}
			}
			if timeout > 0 {
				time.Sleep(time.Duration(timeout) * time.Nanosecond)
			}
		}
	}()

	<-done
	log.Infof("FINISH %d", total )
	status_file.WriteString("status=FINISHED\n")

	close(c.queue)

	for i := 0; i < N; i++ {
		c.stream[i].CloseSend()
		<-waitc[i]
	}
}

func fatal(err error) {
	if err != nil {
		log.Error(err)
	}
}

func arg(i int) string {
	if len(os.Args) < i {
		panic(errors.New("Insufficient arguments"))
	}
	input := os.Args[i]
	return input
}
