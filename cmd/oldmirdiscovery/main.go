package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	logger "github.com/rs/zerolog/log"
	"github.com/hyperledger-labs/mirbft/discovery"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
)

const (
	clientPortOffset = 2
)

type peerData struct {
	OwnID      int32
	OwnAddress string
	NumPeers   int
	NumFaults  int
	ListenPort int32
	Addresses  []string
}

type clientData struct {
	NumPeers  int
	NumFaults int
	Addresses []string
}

func main() {
	// Configure logger
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	logger.Logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true})
	//zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	//logger.Logger = logger.Output(zerolog.ConsoleWriter{
	//	Out:        os.Stdout,
	//	NoColor:    true,
	//	TimeFormat: "15:04:05.000",
	//})

	// Get discovery server address and own addresses from command line
	role := os.Args[1]
	dServAddr := os.Args[2]

	var outData interface{}
	if role == "peer" {

		// Get additional command line parameters only relevant for peers
		ownPublicIP := os.Args[3]
		ownPrivateIP := os.Args[4]

		// Register with the discovery server
		ownID, peerIdentities, _, _, _ := discovery.RegisterPeer(dServAddr, ownPublicIP, ownPrivateIP)

		// Extract necessary data from server response
		peerInfo := peerData{
			OwnID:      ownID,
			OwnAddress: findOwnIdentity(peerIdentities, ownID).PrivateAddr,
			NumPeers:   len(peerIdentities),
			NumFaults:  (len(peerIdentities) - 1) / 3,
			ListenPort: findOwnIdentity(peerIdentities, ownID).Port,
			Addresses:  make([]string, 0),
		}
		for _, identity := range peerIdentities {
			// Peers use private addresses among themselves
			addrString := fmt.Sprintf("%s:%d", identity.PrivateAddr, identity.Port)
			peerInfo.Addresses = append(peerInfo.Addresses, addrString)
		}
		outData = peerInfo
	} else if role == "client" {
		_, peerIdentities := discovery.RegisterClient(dServAddr)
		clientInfo := clientData{
			NumPeers:  len(peerIdentities),
			NumFaults: (len(peerIdentities) - 1) / 3,
			Addresses: make([]string, 0),
		}
		for _, identity := range peerIdentities {
			// Clients use public addresses to connect to peers
			addrString := fmt.Sprintf("%s:%d", identity.PublicAddr, identity.Port+clientPortOffset)
			clientInfo.Addresses = append(clientInfo.Addresses, addrString)
		}
		outData = clientInfo
	} else {
		logger.Error().Str("role", role).Msg("Unknown role. Accepted: peer, client")
	}

	jsonData, err := json.MarshalIndent(outData, "", "    ")
	if err != nil {
		logger.Error().Err(err).Msg("Could not marshal output to JSON format.")
	}
	fmt.Println(string(jsonData))
}

func findOwnIdentity(identities []*pb.NodeIdentity, ownID int32) *pb.NodeIdentity {
	for _, identity := range identities {
		if identity.NodeId == ownID {
			return identity
		}
	}
	return nil
}
