/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

// epochConfig is the information required by the various
// state machines whose state is scoped to an epoch
type epochConfig struct {
	// myConfig is the configuration specific to this node
	myConfig *Config

	// oddities stores counts of suspicious acitivities and logs them.
	oddities *oddities

	// number is the epoch number this config applies to
	number uint64

	// highWatermark is the current maximum seqno that may be processed
	highWatermark SeqNo

	// lowWatermark is the current minimum seqno for which messages are valid
	lowWatermark SeqNo

	// F is the total number of faults tolerated by the network
	f int

	// CheckpointInterval is the number of sequence numbers to commit before broadcasting a checkpoint
	checkpointInterval SeqNo

	// nodes is all the node ids in the network
	nodes []NodeID

	// buckets is a map from bucket ID to leader ID
	buckets map[BucketID]NodeID
}
