/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

type proposer struct {
	myConfig *Config

	queue     [][]byte
	sizeBytes int
	pending   [][][]byte
}

func newProposer(myConfig *Config) *proposer {
	return &proposer{
		myConfig: myConfig,
	}
}

func (p *proposer) propose(data []byte) {
	p.queue = append(p.queue, data)
	p.sizeBytes += len(data)
	if p.sizeBytes >= p.myConfig.BatchParameters.CutSizeBytes {
		p.pending = append(p.pending, p.queue)
		p.queue = nil
	}
}

func (p *proposer) hasOutstanding() bool {
	return len(p.queue) > 0 || p.hasPending()
}

func (p *proposer) hasPending() bool {
	return len(p.pending) > 0
}

func (p *proposer) next() [][]byte {
	if len(p.pending) > 0 {
		n := p.pending[0]
		p.pending = p.pending[1:]
		return n
	}

	if len(p.queue) > 0 {
		n := p.queue
		p.queue = nil
		return n
	}

	panic("called next when nothing outstanding")
}
