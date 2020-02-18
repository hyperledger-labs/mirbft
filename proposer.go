/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type proposer struct {
	myConfig *Config

	proposalBuckets []*proposalBucket // indexed by bucket
}

type proposalBucket struct {
	queue     []Proposal
	sizeBytes int
	pending   [][]Proposal
}

func newProposer(myConfig *Config, buckets int) *proposer {
	proposalBuckets := make([]*proposalBucket, buckets)
	for i := range proposalBuckets {
		proposalBuckets[i] = &proposalBucket{}
	}
	return &proposer{
		myConfig:        myConfig,
		proposalBuckets: proposalBuckets,
	}
}

func (p *proposer) applyPreprocessResult(result PreprocessResult) {
	bucket := int(result.Cup % uint64(len(p.proposalBuckets)))
	proposalBucket := p.proposalBuckets[bucket]
	proposalBucket.queue = append(proposalBucket.queue, result.Proposal)
	proposalBucket.sizeBytes += len(result.Proposal.Data)
	if proposalBucket.sizeBytes >= p.myConfig.BatchParameters.CutSizeBytes {
		proposalBucket.pending = append(proposalBucket.pending, proposalBucket.queue)
		proposalBucket.queue = nil
		proposalBucket.sizeBytes = 0
	}
}

func (p *proposer) hasOutstanding(bucket BucketID) bool {
	index := int(bucket)
	return len(p.proposalBuckets[index].queue) > 0 || len(p.proposalBuckets[index].pending) > 0
}

func (p *proposer) hasPending(bucket BucketID) bool {
	index := int(bucket)
	return len(p.proposalBuckets[index].pending) > 0
}

func (p *proposer) next(bucket BucketID) []Proposal {
	index := int(bucket)
	proposalBucket := p.proposalBuckets[index]

	if len(proposalBucket.pending) > 0 {
		n := proposalBucket.pending[0]
		proposalBucket.pending = proposalBucket.pending[1:]
		return n
	}

	if len(proposalBucket.queue) > 0 {
		n := proposalBucket.queue
		proposalBucket.queue = nil
		proposalBucket.sizeBytes = 0
		return n
	}

	panic("called next when nothing outstanding")
}
