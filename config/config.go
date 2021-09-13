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

package config

import (
	"io/ioutil"

	"github.com/op/go-logging"
	"gopkg.in/yaml.v2"
)

var log = logging.MustGetLogger("server-main")
var format = logging.MustStringFormatter(
	`%{time:15:04:05.000000} %{shortfunc} %{message}`,
)
var Config configuration

// TODO check if we can parse int to uint64
type configuration struct {
	Id int `yaml:"id"`
	N  int `yaml:"n"` // number of servers
	F  int `yaml:"f"` // number of byzantine servers tolerated
	D  int `yaml:"d"` // number of dispatchers per node

	Logging string `yaml:"logging"`
	ChainId string `yaml:"chainId"`
	Ledger  string `yaml:"ledger"` // persistent storage path

	MaxLeaders        int  `yaml:"maxLeaders"`        // max leaderset size, set to 1 to emulate PBFT
	BatchDurationNsec int  `yaml:"batchDurationNsec"` // timeout to cut a batch in nanoseconds
	BatchSizeBytes    int  `yaml:"batchSizeBytes"`    // batch size in bytes
	EpochTimeoutNsec  int  `yaml:"epochTimeoutNsec"`  // epoch change time out in nanoseconds
	BatchSignature    bool `yaml:"batchSignature"`    // batch signatures on commit messages

	WatermarkDist        int `yaml:"watermarkDist"`        // low to high watermaks distance in number of batches
	CheckpointDist       int `yaml:"checkpointDist"`       // checkpoint distance in number of batches
	BucketRotationPeriod int `yaml:"bucketRotationPeriod"` // max epoch size in recovery & bucket rotation period in stable mode
	ClientWatermarkDist  int `yaml:"clientWatermarkDist"`  // number of parallel requests per client
	Buckets              int `yaml:"buckets"`              // buckets per leader

	// Optimizations
	PayloadSharding bool `yaml:"payloadSharding"` // light total order broadcast
	SigSharding     bool `yaml:"sigSharding"`     // request signature verification sharding

	// Byzantine behavior
	ByzantineDuplication bool `yaml:"byzantineDuplication"` // if true nodes do not filter out preprepared or delivered requests
	Censoring            int  `yaml:"censoring"`            // the percentage of requests a malicious node drops [0..100]
	ByzantineDelay       int  `yaml:"byzantineDelay"`       // time added to batch timeout in nanoseconds
	ByzantineAfter       int  `yaml:"byzantineAfter"`       // Adding Byzantine delay for sequence numbers greater or equal to ByzantineAfter
	ByzantineUntil       int  `yaml:"byzantineUntil"`       // Adding Byzantine delay for sequence numbers lees than ByzantineUntil

	// Network Configuration
	UseTLS                 bool `yaml:"useTLS"` // Use TLS for both peer-to-peer and client-to-peer communication
	ServerConnectionBuffer int  `yaml:"serverConnectionBuffer"`	// seconds servers need to wait for each other to connect
	SignatureVerification  bool `yaml:"signatureVerification"` // request signature verification

	// Requests load
	RequestSize     int `yaml:"requestSize"`
	MaxRequestCount int `yaml:"maxRequestCount"` // Requests generated locally, at the server

	// Parameters that define request rate
	RequestsPerClient int `yaml:"requestsPerClient"` // Total number of requests the client submits is RequestsPerClient
	Parallelism       int `yaml:"parallelism"`       // Number of requests send in parallel per instance
	RequestRate       int `yaml:"requestRate"`       // Max request rate per client, in requests per second
	Clients           int `yaml:"clients"`           // Number of client instances
	ClientRunTime     int `yaml:"clientRunTime"`     // Timeout for client to submit all its requests, in milliseconds. Set to 0 for no timeout.

	// Client broadcast parameters
	Blocking    bool `yaml:"blocking"`    // Set to true for the client to wait command line input before starting
	Receivers   int  `yaml:"receivers"`   // Number of leaders client sends requests to
	Broadcast   bool `yaml:"broadcast"`   // If true send requests to Receivers servers estimating which server has an active bucket per request
	Destination int  `yaml:"destination"` // If communication is dedicated

	// TLS connection parameters
	Self struct {
		Listen     string `yaml:"listen"`
		CACertFile string `yaml:"caCertFile"`
		KeyFile    string `yaml:"keyFile"`
		CertFile   string `yaml:"certFile"`
	}
	Servers struct {
		CACertFile string   `yaml:"caCertFile"`
		CertFiles  []string `yaml:"certFiles"`
		Addresses  []string `yaml:"addresses"`
	}
}

func LoadFile(configFileName string) {
	f, err := ioutil.ReadFile(configFileName)

	if err != nil {
		log.Fatalf("Could not read config file %s", configFileName)
	}

	err = yaml.Unmarshal(f, &Config)
	if err != nil {
		log.Fatalf("Could not unmarshal config file %s: %s", configFileName, err.Error())
	}

	log.Debugf("Id: %d", Config.Id)
	log.Debugf("N: %d", Config.N)
	log.Debugf("F: %d", Config.F)
	log.Debugf("D: %d", Config.D)
	log.Debugf("Logging: %s", Config.Logging)
	log.Debugf("ChainId: %s", Config.ChainId)
	log.Debugf("Ledger: %s", Config.Ledger)
	log.Debugf("MaxLeaders: %d", Config.MaxLeaders)
	log.Debugf("BatchDurationNsec: %d", Config.BatchDurationNsec)
	log.Debugf("BatchSizeBytes: %d", Config.BatchSizeBytes)
	log.Debugf("EpochTimeoutNsec: %d", Config.EpochTimeoutNsec)
	log.Debugf("BatchSignature: %t", Config.BatchSignature)
	log.Debugf("WatermarkDist: %d", Config.WatermarkDist)
	log.Debugf("CheckpointDist: %d", Config.CheckpointDist)
	log.Debugf("BucketRotationPeriod: %d", Config.BucketRotationPeriod)
	log.Debugf("ClientWatermarkDist: %d", Config.ClientWatermarkDist)
	log.Debugf("Buckets: %d", Config.Buckets)
	log.Debugf("PayloadSharding: %t", Config.PayloadSharding)
	log.Debugf("SigSharding: %t", Config.SigSharding)
	log.Debugf("ByzantineDuplication: %t", Config.ByzantineDuplication)
	log.Debugf("Censoring: %d", Config.Censoring)
	log.Debugf("ByzantineDelay: %d", Config.Censoring)
	log.Debugf("ByzantineAfter: %d", Config.ByzantineAfter)
	log.Debugf("ByzantineUntil: %d", Config.ByzantineUntil)
	log.Debugf("UseTLS: %t", Config.UseTLS)
	log.Debugf("ServerConfigurationBuffer: %d", Config.ServerConnectionBuffer)
	log.Debugf("SignatureVerification: %t", Config.SignatureVerification)
	log.Debugf("RequestSize: %d", Config.RequestSize)
	log.Debugf("RequestsPerClient: %d", Config.RequestsPerClient)
	log.Debugf("Parallelism: %d", Config.Parallelism)
	log.Debugf("RequestRate: %d", Config.RequestRate)
	log.Debugf("ClientRunTime: %d", Config.ClientRunTime)
	log.Debugf("Clients: %d", Config.Clients)
	log.Debugf("Blocking: %t", Config.Blocking)
	log.Debugf("Receivers: %d", Config.Receivers)
	log.Debugf("Destination: %d", Config.Destination)
	log.Debugf("Self:")
	log.Debugf("    Listen: %s", Config.Self.Listen)
	log.Debugf("    CACertFile: %s", Config.Self.CACertFile)
	log.Debugf("    KeyFile: %s", Config.Self.KeyFile)
	log.Debugf("    CertFile: %s", Config.Self.CertFile)
	log.Debugf("Servers:")
	log.Debugf("    CACertFile: %s", Config.Servers.CACertFile)
	for _, cert := range Config.Servers.CertFiles {
		log.Debugf("    CertFile: %s", cert)
	}
	for _, addr := range Config.Servers.Addresses {
		log.Debugf("    CertFile: %s", addr)
	}
}
