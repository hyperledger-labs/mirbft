/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	pb "github.com/IBM/mirbft/mirbftpb"
)

var _ = Describe("NodeMsg", func() {
	var (
		nodeID        NodeID
		networkConfig *pb.NetworkConfig
		myConfig      *Config
		clientWindows *clientWindows
		oddities      *oddities
		nodeMsgs      *nodeMsgs
	)

	BeforeEach(func() {
		nodeID = NodeID(0)
		networkConfig = &pb.NetworkConfig{
			CheckpointInterval: 2,
			F:                  0,
			Nodes:              []uint64{0},
			NumberOfBuckets:    1,
			MaxEpochLength:     10,
		}
		myConfig = &Config{
			ID:     uint64(0),
			Logger: zap.NewExample(),
			BatchParameters: BatchParameters{
				CutSizeBytes: 1,
			},
			SuspectTicks:         4,
			NewEpochTimeoutTicks: 8,
			BufferSize:           100,
		}
		clientWindows = nil
		oddities = nil
	})

	JustBeforeEach(func() {
		nodeMsgs = newNodeMsgs(nodeID, networkConfig, myConfig, clientWindows, oddities)
		Expect(nodeMsgs).NotTo(BeNil())
	})

	Context("ingest", func() {
		It("buffers messages to be consumed by next", func() {
			Expect(nodeMsgs.buffer.Len()).To(BeZero())
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 5}}})
			Expect(nodeMsgs.buffer.Len()).To(Equal(1))
			// set epoch, so the message above is outdated
			nodeMsgs.setActiveEpoch(&epoch{config: &epochConfig{number: 6}})
			Expect(nodeMsgs.next()).To(BeNil())
		})

		When("buffer is overflown", func() {
			BeforeEach(func() {
				myConfig.BufferSize = 1
			})

			It("drops oldest msg", func() {
				nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Preprepare{}})
				nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 5}}})
				Expect(nodeMsgs.buffer.Len()).To(Equal(1))
				nodeMsgs.setActiveEpoch(&epoch{config: &epochConfig{number: 6}})
				Expect(nodeMsgs.next()).To(BeNil())
			})

		})
	})
})
