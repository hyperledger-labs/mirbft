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
		o             *oddities
		nodeMsgs      *nodeMsgs

		defaultEpochConfig *epochConfig
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
		o = &oddities{logger: zap.NewNop()}
		defaultEpochConfig = &epochConfig{
			number:          5,
			initialSequence: 0,
			networkConfig:   networkConfig,
			leaders:         []uint64{0},
			buckets:         map[BucketID]NodeID{0: 0},
		}
	})

	JustBeforeEach(func() {
		nodeMsgs = newNodeMsgs(nodeID, networkConfig, myConfig, clientWindows, o)
		Expect(nodeMsgs).NotTo(BeNil())
	})

	Context("process", func() {
		JustBeforeEach(func() {
			nodeMsgs.setActiveEpoch(&epoch{config: defaultEpochConfig})
		})

		It("skips stale messages", func() {
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 4}}})
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 4}}})
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 4}}})
			Expect(nodeMsgs.next()).To(BeNil())
			Expect(nodeMsgs.buffer.Len()).To(BeZero())
		})

		It("returns at first current message", func() {
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Prepare{Prepare: &pb.Prepare{Epoch: 4}}})
			nodeMsgs.ingest(&pb.Msg{Type: &pb.Msg_Suspect{Suspect: &pb.Suspect{Epoch: 5}}})
			Expect(nodeMsgs.next()).NotTo(BeNil())
		})
	})

	Context("buffer", func() {
		It("stores incoming messages", func() {
			// buffer is nil
			Expect(nodeMsgs.buffer.Len()).To(BeZero())
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
				Expect(nodeMsgs.buffer.Len()).To(Equal(0))
			})
		})
	})

	Context("process", func() {

	})
})

var _ = Describe("EpochMsgs", func() {

})
