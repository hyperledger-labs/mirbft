// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"context"

	logger "github.com/rs/zerolog/log"
	pb "github.com/hyperledger-labs/mirbft/protobufs"
	"google.golang.org/grpc"
)

func RegisterPeer(serverAddrPort string, ownPublicIP string, ownPrivateIP string) (int32, []*pb.NodeIdentity, []byte, []byte, []byte) {

	// Set up a GRPC connection.
	conn, err := grpc.Dial(serverAddrPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logger.Fatal().Str("srvAddr", serverAddrPort).Msg("Couldn't connect to discovery server.")
	}
	defer conn.Close()

	// Register client stub.
	client := pb.NewDiscoveryClient(conn)

	// Submit RegisterPeer request and obtain own ID as well as all peers' identities
	response, err := client.RegisterPeer(context.Background(), &pb.RegisterPeerRequest{
		PublicAddr:  ownPublicIP,
		PrivateAddr: ownPrivateIP,
	})
	if err != nil {
		logger.Fatal().Msg("RegisterPeer request failed.")
	}

	// Return discovered values.
	return response.NewPeerId, response.Peers, response.PrivKey, response.TblsPubKey, response.TblsPrivKeyShare
}

func SyncPeer(serverAddrPort string, ownPeerID int32) {

	// Set up a GRPC connection.
	conn, err := grpc.Dial(serverAddrPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logger.Fatal().Str("srvAddr", serverAddrPort).Msg("Couldn't connect to discovery server.")
	}
	defer conn.Close()

	// Register client stub.
	client := pb.NewDiscoveryClient(conn)

	// Submit SyncPeer request
	if _, err := client.SyncPeer(context.Background(), &pb.SyncRequest{PeerId: ownPeerID}); err != nil {
		logger.Fatal().Msg("SyncPeer request request failed.")
	}
}

func RegisterClient(serverAddrPort string) (int32, []*pb.NodeIdentity) {

	// Set up a GRPC connection.
	conn, err := grpc.Dial(serverAddrPort, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logger.Fatal().Str("srvAddr", serverAddrPort).Msg("Couldn't connect to discovery server.")
	}
	defer conn.Close()

	// Register client stub.
	client := pb.NewDiscoveryClient(conn)

	// Submit Orderers request and obtain all orderers' identities
	response, err := client.RegisterClient(context.Background(), &pb.RegisterClientRequest{})
	if err != nil {
		logger.Fatal().Msg("RegisterClient request failed.")
	}

	// Return discovered values.
	return response.NewClientId, response.Peers
}
